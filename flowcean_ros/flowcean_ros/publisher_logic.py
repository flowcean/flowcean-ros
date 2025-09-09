
import flowcean_ros.helper_functions
from rclpy.publisher import Publisher as ROSPublisher
from typing import Any
from core.logic import set_field_value_of_ros_msg

class Publisher():

    def __init__(self, msg_class : Any, topic_name : str, ros_publisher : ROSPublisher, map : dict):
        self._topic_name = topic_name
        self._ros_publisher = ros_publisher
        self._map = map
        self._msg_type = msg_class
        
        self._helper_conf = None
        if self._map.get("helper", None) is not None:
            # if a helper function is defined, extract and import it
            self._helper_conf = self._map.pop("helper")

            function = getattr(helper_functions, self._helper_conf[0])
            self._helper_conf : tuple[callable, list[str]]= (
                function,
                self._helper_conf[1]
            )

    
    def __call__(self, data : dict[str, Any]):
        """
        publish a ROS message based on input data
        """
        msg = self._msg_type()

        if self._helper_conf:
            fun, columns = self._helper_conf
            input_values = [data[c] for c in columns]

            output_values = list(fun(input_values))

        for field, column in self._map.items():
            if value != "helper":
                value = data[column]
            else:
                value = output_values.pop(0)
            set_field_value_of_ros_msg(msg, field, value)            

        self._ros_publisher.publish(msg)