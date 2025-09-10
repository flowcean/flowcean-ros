import numpy as np
# from polars import DataFrame, LazyFrame
from flowcean.polars import DataFrame
import rclpy
import time
from yaml import safe_load as yaml_safe_load
from rclpy.node import Node
from rclpy.qos import QoSPresetProfiles
from core.data_buffer import DataBuffer
from core.msg_data import MsgData
from core.logic import _unpack_to_dict, get_all_fields_of_class, get_msg_class, msg_has_field
from flowcean_ros.publisher_logic import Publisher
from nav_msgs.msg import OccupancyGrid
from flowcean.core.model import Model
from typing import Any
import polars as pl
from flowcean_ros.transforms import get_transform



_PRINT_FREQUENCY = 5.0  # seconds
_QOSPROFILE_MAP = {
    "UNKNOWN": QoSPresetProfiles.UNKNOWN.value,
    "SYSTEM_DEFAULT": QoSPresetProfiles.SYSTEM_DEFAULT.value,
    "SENSOR_DATA": QoSPresetProfiles.SENSOR_DATA.value,
    "SERVICES_DEFAULT": QoSPresetProfiles.SERVICES_DEFAULT.value,
    "PARAMETERS": QoSPresetProfiles.PARAMETERS.value,
    "PARAMETER_EVENTS": QoSPresetProfiles.PARAMETER_EVENTS.value,
    "ACTION_STATUS_DEFAULT": QoSPresetProfiles.ACTION_STATUS_DEFAULT.value,
}


class Predictor(Node):
    """ Predictor node for deployment of Flowcean models."""

    def __init__(self) -> None:
        super().__init__("predictor")
        
        self.declare_parameter("model_path", "models/model.pt")
        self.declare_parameter("topics_info", "config/topics_config.yaml")
        self.declare_parameter("input_threshold", 0.1)
        self.declare_parameter("buffer_length", 1)
        self.declare_parameter("use_map_file", False)
        
        use_map_file = self.get_parameter("use_map_file").get_parameter_value().bool_value
        buf_len = self.get_parameter("buffer_length").get_parameter_value().integer_value
        self.buffer = DataBuffer(length=buf_len)
        input_conf, output_conf = self._load_topics_config()
        self.model = self._load_model()


        time_start = time.time()

        # handle input configuration
        to_subscribe = set(input_conf.keys())
        while len(to_subscribe) > 0:
            topics_and_types = dict(self.get_topic_names_and_types())
            
            for topic_name, t_info in input_conf.items():
                
                # Only create new subscription if topic is not already subscribed
                if topic_name not in to_subscribe or topic_name not in topics_and_types:
                    continue
                
                # Import module of message type
                type_name : str = topics_and_types[topic_name]
                msg_class = get_msg_class(topic_name, type_name)
                
                try:
                    if "include" in t_info:
                        fields_to_store = t_info["include"]
                        for f in fields_to_store:
                            assert msg_has_field(msg_class, f)
                    else:
                        fields_to_exclude = t_info.get("exclude", [])
                        for f in fields_to_exclude:
                            assert msg_has_field(msg_class, f)
                        fields_to_store = get_all_fields_of_class(msg_class) - fields_to_exclude

                except AssertionError:
                    self.get_logger().error(
                        f"Topic {topic_name} of type {type_name} has no field {f}",
                    )
                    raise
                
                # handle map separately
                if topic_name == "/map":
                    msg = self._load_map_file() if use_map_file else self._call_map_service()
                    success = msg != None
                else:
                    # only subscribe to topics that continously publish
                    success = self._subscribe_to_topic(
                        topic_name=topic_name,
                        msg_type=msg_class,
                        qos=t_info.get("qos_profile", "SYSTEM_DEFAULT"),
                    )

                # only add to buffer if subscribed or data loaded successfully
                if success:     
                    self.buffer[topic_name] = MsgData(
                        topic=topic_name,
                        fields=fields_to_store,
                        is_single=bool(t_info.get("one_time", False)),
                    )
                    to_subscribe.remove(topic_name)
                    
                    if topic_name == "/map":
                        self._callback_to_store_msg(msg, "/map")
                        self._logger.info("Map data loaded successfully.")
                        

            time_curr = time.time()
            if time_curr - time_start > _PRINT_FREQUENCY:
                time_start = time_curr
                self.get_logger().warning(
                    f"Unavailable topics: {to_subscribe}",
                )

        # handle output configuration
        self._publishers : list = []
        for topic_name, t_info in output_conf.items():

            msg_class = get_msg_class(
                topic_name,
                [t_info["type"]]   
            )
            assert (msg_has_field(msg_class, f) for f in t_info["map"]) 

            ros_publisher = self.create_publisher(
                msg_type=msg_class,
                topic=topic_name,
                qos_profile=_QOSPROFILE_MAP[
                    t_info.get("qos_profile", "SYSTEM_DEFAULT")
                ],
            )

            self._publishers.append(
                Publisher(
                    msg_class=msg_class,
                    topic_name=topic_name,
                    ros_publisher=ros_publisher,
                    map = t_info["map"]
                )
            )

    def _load_model(self) -> Model:
        """ Load the Flowcean model from the specified path."""
        
        path = self.get_parameter("model_path").get_parameter_value().string_value
        if not path:
            raise ValueError("Model path cannot be empty.")
        
        return Model.load(path)
 
    def _load_topics_config(self) -> tuple[dict, dict]:
        """ Read input and output configuration from YAML file."""

        path = self.get_parameter("topics_info").get_parameter_value().string_value
        with open(path) as f:
            config: dict = yaml_safe_load(f)
            input_conf: dict = config["input_topics"]
            output_conf: dict = config["output_topics"]

        return input_conf, output_conf

    def _load_map_file(self) -> OccupancyGrid:
        """
        Load map from pgm and yaml files.
        """
        self.declare_parameter("map_file", "maps/map.pgm")
        self.declare_parameter("map_info_file", "maps/map.yaml")
        
        try:
            from PIL import Image
            img = Image.open(
                    self.get_parameter("map_file").get_parameter_value().string_value
                ).convert("L")
            width, height = img.size
            img = np.array(img)
            
            path = self.get_parameter("map_info_file").get_parameter_value().string_value
            with open(path) as f:
                map_info : dict = yaml_safe_load(f)
        except Exception as e:
            self.get_logger().error(f"Failed to load map file: {e}")
            return None  
        
        negate = map_info.get("negate", 0)
        occ_th = map_info.get("occupied_thresh", 0.65)
        free_th = map_info.get("free_thresh", 0.196)    

        # Convert grayscale to occupancy values
        occupancy = []
        for y in range(height):
            for x in range(width):
                color = img[height - y - 1, x] / 255.0
                if negate:
                    color = 1.0 - color
                if color > occ_th:
                    val = 100
                elif color < free_th:
                    val = 0
                else:
                    val = -1
                occupancy.append(val)

        # Build OccupancyGrid
        grid = OccupancyGrid()
        grid.header.frame_id = "map"
        grid.info.width = width
        grid.info.height = height
        grid.info.resolution = map_info["resolution"]
        grid.info.origin.position.x = map_info["origin"][0]
        grid.info.origin.position.y = map_info["origin"][1]
        grid.info.origin.position.z = 0.0
        grid.info.origin.orientation.w = 1.0
        grid.data = occupancy

        return grid

    def _call_map_service(self) -> OccupancyGrid:
        """
        Call the /map service to get the map data.
        """
        from nav_msgs.srv import GetMap
        client = self.create_client(GetMap, "map")
        while not client.wait_for_service(timeout_sec=1.0):
            self.get_logger().info("Waiting for /map service...")

        req = GetMap.Request()
        future = client.call_async(req)
        rclpy.spin_until_future_complete(self, future)
        res : GetMap.Response = future.result()

        if res is None:
            self.get_logger().error(
                "Failed to call /map service. Is the map server running?",
            )
            return None
        
        self.get_logger().info("Map received.")
        return res.map

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
                lambda msg_data, topic_name=topic_name: self._callback_to_store_msg(
                    msg_data,
                    topic_name,
                ),
                _QOSPROFILE_MAP[qos],
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
    
    def _callback_to_store_msg(self, msg: Any, topic: str) -> None:
        msg_data: MsgData = self.buffer.get(topic, None)
        if msg_data.is_single and msg_data.data_complete():
            return

        self.buffer.store(
            ros_msg=msg,
            topic_name=topic,
            timestamp=self.get_clock().now().to_msg(),
        )
        self.get_logger().debug(
            f"Stored message for topic {topic}",
        )

        if self.buffer.is_full():
            self.predict()

    def _generate_observation(self) -> pl.DataFrame:
        """
        Transforms a Dataframe based on data in the buffer.
        """

        frames = []
        print("buffer content: ", self.buffer.items())
        for topic_name, msg_data in self.buffer.items():
            
            fields, values = msg_data.keys(), msg_data.values() # HERE msg_data.items() gives fields and values wrongly
            
            values = [_unpack_to_dict(v) for v in values]
            
            print("fields", fields)
            print("values", values)
            
            df = pl.DataFrame([values], schema=[*fields], orient="row")
            # time = pl.Series("time", [msg_data.get_stamp()]).cast(pl.Int64)
            # nest_into_timeseries = pl.struct(
            #     [
            #         pl.col("time"),
            #         pl.struct(pl.exclude("time")).alias("value"),
            #     ],
            # )
            # df = df.with_columns(time).select(
            #     nest_into_timeseries.implode().alias(topic_name),
            # )
            print(df)
            frames.append(df)
            

        return (
            pl.DataFrame(pl.concat(frames, how="horizontal"))
            if frames
            else pl.DataFrame()
        )

    def predict(self) -> None:
        self.get_logger().info("  >>  Prediction...")

        observation: DataFrame = self._generate_observation()
        print(observation.collect_schema())

        transform = get_transform()
        data = transform(observation)

        output: pl.LazyFrame = self.model.predict(
            data,
        )
        output = output.collect()   

        # TODO: CONVERT DATAFRAME TO A DICT
        for publisher in self._publishers:            
            publisher(output)
        
        self.buffer.clear()






def main(args: list[str] | None = None) -> None:
    rclpy.init(args=args)
    node = Predictor()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().debug("Shutting down")
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()