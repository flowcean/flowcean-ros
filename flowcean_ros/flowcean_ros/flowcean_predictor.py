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


import importlib
import time
from typing import Any

import polars as pl
import rclpy
import yaml
from builtin_interfaces.msg import Time
from custom_transforms.detect_delocalizations import (
    DetectDelocalizations,
)
from custom_transforms.localization_status import LocalizationStatus
from custom_transforms.particle_cloud_statistics import ParticleCloudStatistics
from custom_transforms.slice_time_series import SliceTimeSeries
from custom_transforms.zero_order_hold_matching import ZeroOrderHold
from sensor_msgs.msg import LaserScan
from nav2_msgs.msg import ParticleCloud
from flowcean.core.transform import Lambda
from flowcean.polars.transforms.drop import Drop
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
    empty: bool
    timestamp: float

    def __init__(self, topic_name: str = "", is_single: bool = False) -> None:
        self.topic_name = topic_name
        self.is_single = is_single

        self.fields = {}
        self.empty = True
        self.timestamp = -1

    def stamp(self, stamp: float) -> None:
        self.timestamp = stamp

    def clear(self) -> None:
        for field in self.fields:
            self.add_field(field)  # reinit field
        self.empty = True
        self.timestamp = -1

    def add_field(self, *field: str) -> None:
        for f in list(field):
            self.fields[f] = []

    def set_field(self, field: str, val: Any) -> None:
        self.fields[field].append(val)


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
            msg_info.fields[field] = value
        msg_info.stamp(timestamp.sec * _TO_NANO + timestamp.nanosec)

    def is_full(self) -> bool:
        return any(self.topic_data[topic].empty for topic in self.topic_data)

    def clear(self, keep_single: bool = True) -> None:
        for topic in self.topic_data:
            if not self.topic_data[topic].is_single or not keep_single:
                self.topic_data[topic].clear()


class FlowceanPredictor(Node):
    def __init__(self) -> None:
        super().__init__("flowcean_predictor")
        self.transform = ParticleCloudStatistics()  # update transforms

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
                    msg_info.is_single = (
                        bool(input_dict["one_time"])
                        if "one_time" in input_dict
                        else False
                    )

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
                            print(msg_type)
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

                    try:
                        self.create_subscription(
                            msg_type,
                            t_name,
                            lambda msg_data, topic_name=t_name: self._callback(
                                msg_data,
                                topic_name,
                            ),
                            self.quality_of_service_mapping[
                                t_info.get("qos_profile", "SYSTEM_DEFAULT")
                            ],
                        )
                        self.get_logger().info(
                            f"Subscribed to topic: {t_name} with type: {t_type}",
                        )
                        t_info["subscribed"] = True
                        num_topics -= 1
                    except:
                        self.get_logger().warning(
                            f"Couldn't subscribe to topic {t_name} with type {t_type} and QoS {t_info['qos_profile']}",
                        )

                    self.data_buffer.add(msg_info)

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
            time.sleep(1)  # Avoid busy waiting
        # print([info.fields for info in self.data_buffer.topic_data.values()])
        # rclpy.spin(self)

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
        return input_fields

    def _callback(self, msg: Any, topic: str) -> None:
        msg_info: MsgData = self.data_buffer.topic_data[topic]

        # Data of single message is already stored
        if msg_info.is_single and not msg_info.empty:
            return

        msg_info.empty = False
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
        def generate_observation() -> pl.DataFrame:
            """Generate observation from the data buffer."""
            frames = []
            for topic_name, msg_info in self.data_buffer.topic_data.items():
                struct_list = [
                    {"time": msg_info.timestamp, "value": msg_info.fields},
                ]
                frames.append(
                    pl.DataFrame({topic_name: [struct_list]}, strict=False),
                )

            return (
                pl.concat(frames, how="horizontal")
                if frames
                else pl.DataFrame()
            )

        observation: pl.DataFrame = generate_observation()

        data = (
            observation
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
                    "/momo/pose",
                    "/amcl_pose",
                ],
                name="measurements",
            )
            | Drop("/scan", "/particle_cloud", "/momo/pose", "/amcl_pose")
            # detect experiment slice points based on delocalization events
            | DetectDelocalizations("/delocalizations", name="slice_points")
            | Drop("/delocalizations")
            | SliceTimeSeries(
                time_series="measurements",
                slice_points="slice_points",
            )
            | Drop("slice_points")
            # detect localization status based on position and heading errors
            | LocalizationStatus(
                time_series="measurements",
                ground_truth="/momo/pose",
                estimation="/amcl_pose",
                position_threshold=0.4,
                heading_threshold=0.4,
            )
        )

        self.data_buffer.clear()


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
