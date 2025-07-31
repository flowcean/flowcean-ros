#!/usr/bin/env python
# /// script
# dependencies = [
#     "flowcean",
#     "matplotlib",
#     "opencv-python",
#     "rclpy",
#     "numpy",
#     "polars",
#     "pyyaml",
#     "builtin_interfaces",
#     "std_msgs"
# ]
#
# [tool.uv.sources]
# flowcean = { path = "../../", editable = true }
# ///


import collections.abc
import importlib
import time
from dataclasses import fields, is_dataclass
from typing import Any

import cv2
import numpy as np
import polars as pl
import rclpy
import rclpy.time
import yaml
from builtin_interfaces.msg import Time
from custom_transforms.collapse import Collapse
from custom_transforms.zero_order_hold_matching import ZeroOrderHold
from flowcean.core.model import Model
from flowcean.core.transform import Lambda
from flowcean.polars import DataFrame
from flowcean.polars.transforms.drop import Drop
from nav_msgs.srv import GetMap
from rclpy.node import Node
from rclpy.qos import QoSPresetProfiles
from rosidl_runtime_py.utilities import (
    get_message,
)

_PRINT_FREQUENCY = 5  # [seconds]
_TO_NANO = 10e9  # [meter] --> [nanometer]


class MsgData:
    topic_name: str
    fields: dict[str, list]
    is_single: bool
    timestamp: float

    def __init__(
        self,
        topic_name: str = "",
        is_single: bool = False,
    ) -> None:
        self.topic_name = topic_name
        self.is_single = is_single

        self.fields = {}
        self.timestamp = -1

    def stamp(self, stamp: float) -> None:
        self.timestamp = stamp

    def clear(self) -> None:
        for field in self.fields:
            self.add_field(field)  # reinit field
        self.timestamp = -1

    def add_field(self, *field: str) -> None:
        for f in list(field):
            self.fields[f] = None

    def set_field(self, field: str, val: Any) -> None:
        self.fields[field] = val

    def read_field(self, field: str) -> Any:
        return self.fields[field]

    def data_complete(self) -> bool:
        return all(self.fields[field] is not None for field in self.fields)

    def items(self) -> tuple[list[str], list[Any]]:
        return self.fields.keys(), self.fields.values()

    def get_stamp(self) -> int:
        return int(self.timestamp)


class DataBuffer:
    topic_data: dict[str, MsgData]
    length: int

    def __init__(self, length: int = 1) -> None:
        self.topic_data = {}
        self.length = length

    def add(self, msg_info: MsgData) -> None:
        self.topic_data[msg_info.topic_name] = msg_info

    def store_msg(
        self,
        ros_msg: Any,
        topic_name: str,
        timestamp: Time,
    ) -> None:
        msg_info: MsgData = self.topic_data[topic_name]

        for field in msg_info.fields:
            value = ros_msg
            for sub_field in field.rsplit("."):
                value = getattr(value, sub_field)
            msg_info.set_field(field, value)
        msg_info.stamp(timestamp.sec * _TO_NANO + timestamp.nanosec)

    def is_full(self) -> bool:
        return all(
            self.topic_data[topic].data_complete() for topic in self.topic_data
        )

    def clear(self, keep_single: bool = True) -> None:
        for topic in self.topic_data:
            if not self.topic_data[topic].is_single or not keep_single:
                self.topic_data[topic].clear()


class FlowceanPredictor(Node):
    def __init__(self) -> None:
        super().__init__("flowcean_predictor")

        self.input_topic_config: dict[str, Any] = {}
        self.input_threshold: float = (
            self.declare_parameter(
                "input_threshold",
                1.0,
            )
            .get_parameter_value()
            .double_value
        )

        config_path: str = (
            self.declare_parameter(
                "topics_info",
                "config/topics_config.yaml",
            )
            .get_parameter_value()
            .string_value
        )
        model_path: str = (
            self.declare_parameter(
                "model_path",
                "models/model.pt",
            )
            .get_parameter_value()
            .string_value
        )
        self.image_size = 64
        self.width_meters = 15
        self.model = Model.load(model_path)

        # Read topics configuration from YAML file
        try:
            with open(config_path) as f:
                config: dict = yaml.safe_load(f)
                input_dict: dict = config["input_topics"]
                self.output_info: dict = config["output_topics"]
                assert len(input_dict.keys()) > 0, (
                    "Input topics configuration is empty."
                )
                assert len(self.output_info.keys()) > 0, (
                    "Output topics configuration is empty."
                )

        except ValueError as e:
            self.get_logger().error(
                f"Invalid configuration file format: {e}",
            )
            raise

        except KeyError as e:
            self.get_logger().error(
                f"Missing key in config file: {e}",
            )
            raise

        except FileNotFoundError:
            self.get_logger().error(
                f"File not found: {config_path}",
            )
            raise

        except yaml.YAMLError as e:
            self.get_logger().error(f"YAML parsing error in config file: {e}")
            raise

        # Subscribe to topics to get type, check fields and add to data buffer

        self.data_buffer: DataBuffer = DataBuffer(
            length=self.declare_parameter(
                "buffer_length",
                1,
            )
            .get_parameter_value()
            .integer_value,
        )
        num_topics = len(input_dict.keys())
        for val in input_dict.values():
            val["subscribed"] = False
        time_start = time.time()

        self.quality_of_service_mapping = {
            "UNKNOWN": QoSPresetProfiles.UNKNOWN.value,
            "SYSTEM_DEFAULT": QoSPresetProfiles.SYSTEM_DEFAULT.value,
            "SENSOR_DATA": QoSPresetProfiles.SENSOR_DATA.value,
            "SERVICES_DEFAULT": QoSPresetProfiles.SERVICES_DEFAULT.value,
            "PARAMETERS": QoSPresetProfiles.PARAMETERS.value,
            "PARAMETER_EVENTS": QoSPresetProfiles.PARAMETER_EVENTS.value,
            "ACTION_STATUS_DEFAULT": QoSPresetProfiles.ACTION_STATUS_DEFAULT.value,
        }

        while num_topics > 0:
            topics_and_types = dict(self.get_topic_names_and_types())

            for t_name, t_info in input_dict.items():
                if not t_info["subscribed"] and t_name in topics_and_types:
                    t_type = topics_and_types[t_name]
                    if len(t_type) > 1:
                        self.get_logger().error(
                            f"Found more than one type for topic {t_name}",
                        )
                        raise RuntimeError

                    try:
                        module, _msg, cls = t_type[0].rsplit("/", 2)
                        msg_type = getattr(
                            importlib.import_module(module + "." + _msg),
                            cls,
                        )
                    except ModuleNotFoundError:
                        self.get_logger().error(
                            f"Couldn't import module for {t_type[0]}",
                        )
                        raise

                    # Create new entry for data buffer and check fields
                    msg_info = MsgData()
                    msg_info.topic_name = t_name
                    msg_info.is_single = bool(t_info.get("one_time", False))

                    try:
                        if "include" in t_info:
                            for field in t_info["include"]:
                                assert self._check_field_in_msg(
                                    msg_class=msg_type,
                                    field=field,
                                )
                                msg_info.add_field(field)
                        else:
                            for field in t_info.get("exclude", []):
                                assert self._check_field_in_msg(
                                    msg_class=msg_type,
                                    field=field,
                                )
                            msg_info.add_field(
                                *self._get_fields_to_include(
                                    msg_type,
                                    t_info.get("exclude", []),
                                ),
                            )
                    except AssertionError:
                        self.get_logger().error(
                            f"Topic {t_name} of type {t_type} has no field {field}",
                        )
                        raise

                    # only subscribe to topics that continously publish

                    if not msg_info.is_single:
                        if self._subscribe_to_topic(
                            topic_name=t_name,
                            msg_type=msg_type,
                            qos=t_info.get("qos_profile", "SYSTEM_DEFAULT"),
                        ):
                            t_info["subscribed"] = True
                            num_topics -= 1
                            self.data_buffer.add(msg_info)
                    else:
                        # TODO: FOR ANY DATA NOT ONLY MAP
                        t_info["subscribed"] = True
                        num_topics -= 1
                        self.data_buffer.add(msg_info)
                        self.load_map_data()

            time_curr = time.time()
            if time_curr - time_start > _PRINT_FREQUENCY:
                time_start = time_curr
                unsubscribed_topics = " ".join(
                    t_name
                    for t_name, t_info in input_dict.items()
                    if not t_info["subscribed"]
                )

                self.get_logger().warning(
                    f"Unavailable topics: {unsubscribed_topics}",
                )

    def load_map_data(self) -> None:
        # read map file (.pgm)
        use_rosbag = (
            self.declare_parameter(
                "use_rosbag",
                False,
            )
            .get_parameter_value()
            .bool_value
        )
        if use_rosbag:
            pgm_array = np.asarray(
                cv2.imread(
                    self.declare_parameter(
                        "map_path",
                        "maps/map.pgm",
                    )
                    .get_parameter_value()
                    .string_value,
                ),
            )
            map_info = (
                self.declare_parameter(
                    "map_info_path",
                    "maps/map.yaml",
                )
                .get_parameter_value()
                .string_value
            )
            import yaml

            with open(map_info) as stream:
                try:
                    map_info = yaml.safe_load(stream)
                except yaml.YAMLError as exc:
                    print(exc)

            grid_data = np.zeros_like(pgm_array, dtype=np.int8)

            grid_data[pgm_array == 0] = 100  # Occupied
            grid_data[pgm_array == 255] = 0  # Free
            mask_unknown = (pgm_array != 0) & (pgm_array != 255)
            grid_data[mask_unknown] = -1  # Unknown

            # Flip vertically because ROS uses bottom-left origin
            grid_data = np.flipud(grid_data)

            # Create the OccupancyGrid message
            from geometry_msgs.msg import Point, Pose, Quaternion
            from nav_msgs.msg import OccupancyGrid
            from std_msgs.msg import Header

            map_msg = OccupancyGrid()

            map_msg.header = Header()
            map_msg.header.stamp = rclpy.time.Time().to_msg()
            map_msg.header.frame_id = "map"

            map_msg.info.resolution = map_info["resolution"]
            map_msg.info.width = pgm_array.shape[1]
            map_msg.info.height = pgm_array.shape[0]

            x, y, yaw = map_info["origin"]
            map_msg.info.origin = Pose()
            map_msg.info.origin.position = Point(x=x, y=y, z=0.0)
            map_msg.info.origin.orientation = Quaternion(
                x=0.0,
                y=0.0,
                z=np.sin(yaw / 2.0),
                w=np.cos(yaw / 2.0),
            )

            # Flatten to 1D list
            map_msg.data = grid_data.flatten().tolist()
        else:

            def _map_callback():
                future = self.cli.call_async(self.req)
                rclpy.spin_until_future_complete(self, future)
                if future.result() is not None:
                    self.get_logger().info("Map received.")
                    return future.result().map
                self.get_logger().error(
                    "Failed to call /map service. Is the map server running?",
                )
                return None

            self.cli = self.create_client(GetMap, "map")
            while not self.cli.wait_for_service(timeout_sec=1.0):
                self.get_logger().info("Waiting for /map service...")
            self.req = GetMap.Request()
            map_msg = _map_callback()
            if map_msg is None:
                raise RuntimeError(
                    "Failed to retrieve map from /map service.",
                )

        self._callback(map_msg, "/map")
        self._logger.info("Map data loaded successfully.")

    def _subscribe_to_topic(
        self,
        topic_name: str,
        msg_type: Any,
        qos: str,
    ) -> bool:
        try:
            self.create_subscription(
                msg_type,
                topic_name,
                lambda msg_data, topic_name=topic_name: self._callback(
                    msg_data,
                    topic_name,
                ),
                self.quality_of_service_mapping[qos],
            )
            self.get_logger().info(
                f"Subscribed to topic: {topic_name}",
            )
        except:
            self.get_logger().warning(
                f"Couldn't subscribe to topic {topic_name} and QoS {qos}",
            )
            return False
        return True

    def _check_field_in_msg(self, msg_class: Any, field: str) -> bool:
        """Recursively checks if a field is part of a msg.

        @params:
            - msg_class: ROS msg template
            - field: field to check
        returns:
            - True if all nested fields in msg
        """
        if "." in field:
            field, nested_fields = field.split(".", 1)
        else:
            nested_fields = None

        field_types = msg_class.get_fields_and_field_types()
        if field not in field_types:
            return False

        if nested_fields is None:
            return True

        nested_msg_class = get_message(
            field_types[field],
        )
        if nested_msg_class is None:
            return False
        return self._check_field_in_msg(nested_msg_class, nested_fields)

    def _get_fields_to_include(
        self,
        msg: Any,
        fields_to_exclude: list | None = None,
        input_fields: list | None = None,
        parent_name: str | None = None,
    ) -> dict:
        """Return fields of msg to be included in the input data."""
        if not fields_to_exclude:
            fields_to_exclude = []

        if not input_fields:
            input_fields = []
        # print("test")

        if not parent_name:
            parent_name = ""
        for field, field_type in msg.get_fields_and_field_types().items():
            if "/" not in field_type:
                full_name = parent_name[1:] + "." + field
                if full_name not in fields_to_exclude:
                    input_fields.append(full_name)
            else:
                nested_msg_class = get_message(field_type)

                input_fields = self._get_fields_to_include(
                    nested_msg_class,
                    fields_to_exclude,
                    input_fields,
                    parent_name + "." + field,
                )
                field.join(parent_name.split(".")[:-1])  # remove last field
        return input_fields

    def _callback(self, msg: Any, topic: str) -> None:
        msg_info: MsgData = self.data_buffer.topic_data[topic]
        if msg_info.is_single and msg_info.data_complete():
            return

        self.data_buffer.store_msg(
            ros_msg=msg,
            topic_name=topic,
            timestamp=self.get_clock().now().to_msg(),
        )
        self.get_logger().debug(
            f"Stored message for topic {topic}",
        )
        if self.data_buffer.is_full():
            self.predict()

    def predict(self) -> None:
        self.get_logger().info("Predicting...")

        def generate_observation() -> DataFrame:
            """Generate observation from the data buffer."""
            frames = []
            for topic_name, msg_info in self.data_buffer.topic_data.items():
                fields, values = msg_info.items()
                values = [_unpack_to_dict(v) for v in values]
                df = pl.LazyFrame([values], schema=[*fields], orient="row")
                time = pl.Series("time", [msg_info.get_stamp()]).cast(pl.Int64)
                nest_into_timeseries = pl.struct(
                    [
                        pl.col("time"),
                        pl.struct(pl.exclude("time")).alias("value"),
                    ],
                )
                frames.append(
                    df.with_columns(time).select(
                        nest_into_timeseries.implode().alias(topic_name),
                    ),
                )

            return (
                DataFrame(pl.concat(frames, how="horizontal"))
                if frames
                else DataFrame()
            )

        observation: DataFrame = generate_observation()
        transform = (
            Collapse("/map", element=0)
            | Lambda(
                lambda data: data.with_columns(
                    pl.col("/map").struct.with_fields(
                        pl.field("data").list.eval(pl.element() != 0),
                    ),
                ),
            )
            | ZeroOrderHold(
                features=[
                    "/scan",
                    "/particle_cloud",
                    # "/momo/pose",
                    "/amcl_pose",
                ],
                name="measurements",
            )
            | Drop("/scan", "/particle_cloud", "/amcl_pose")
        )
        data = transform(observation.data)

        data = (
            data.explode("measurements")
            .unnest("measurements")
            .unnest("value")
            .select(
                pl.col("/map"),
                pl.struct(
                    [
                        pl.col("/scan/ranges").alias("ranges"),
                        pl.col("/scan/angle_min").alias("angle_min"),
                        pl.col("/scan/angle_max").alias("angle_max"),
                        pl.col("/scan/angle_increment").alias(
                            "angle_increment",
                        ),
                        pl.col("/scan/range_min").alias("range_min"),
                        pl.col("/scan/range_max").alias("range_max"),
                    ],
                ).alias("/scan"),
                pl.col("/particle_cloud/particles").alias(
                    "/particle_cloud",
                ),
                pl.struct(
                    [
                        pl.struct(
                            [
                                pl.col(
                                    "/amcl_pose/pose.pose.position.x",
                                ).alias("position.x"),
                                pl.col(
                                    "/amcl_pose/pose.pose.position.y",
                                ).alias("position.y"),
                                pl.col(
                                    "/amcl_pose/pose.pose.orientation.x",
                                ).alias("orientation.x"),
                                pl.col(
                                    "/amcl_pose/pose.pose.orientation.y",
                                ).alias("orientation.y"),
                                pl.col(
                                    "/amcl_pose/pose.pose.orientation.z",
                                ).alias("orientation.z"),
                                pl.col(
                                    "/amcl_pose/pose.pose.orientation.w",
                                ).alias("orientation.w"),
                            ],
                        ).alias("pose"),
                    ],
                ).alias("/amcl_pose"),
            )
        )

        output_data: pl.LazyFrame = self.model.predict(
            data,
        )
        print(output_data.collect())
        self.data_buffer.clear()


def _unpack_to_dict(obj: object) -> object:
    if isinstance(obj, (tuple, list)):
        return type(obj)(_unpack_to_dict(item) for item in obj)
    if hasattr(obj, "__slots__") and hasattr(
        obj,
        "get_fields_and_field_types",
    ):
        return {
            f: _unpack_to_dict(getattr(obj, f))
            for f in obj.get_fields_and_field_types()
        }
    if is_dataclass(obj) and not isinstance(obj, type):
        return {
            f.name: _unpack_to_dict(getattr(obj, f.name))
            for f in fields(obj)
            if f.name != "__msgtype__"
        }
    if isinstance(obj, np.ndarray):
        obj = obj.astype(np.float32, copy=False)
        return obj.tolist()
    if isinstance(obj, collections.abc.Sequence):
        return list(obj)
    return obj


def main(args: list[str] | None = None) -> None:
    rclpy.init(args=args)
    node = FlowceanPredictor()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().debug("Shutting down")
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
