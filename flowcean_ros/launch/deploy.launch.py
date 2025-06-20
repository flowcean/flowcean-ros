import os

# flowcean imports
from ament_index_python.packages import get_package_share_directory
from geometry_msgs.msg import *
from launch import LaunchDescription
from launch_ros.actions import Node
from std_msgs.msg import *


def generate_launch_description():
    input_info = os.path.join(
        get_package_share_directory("flowcean_ros"),
        "config",
        "input_topics.yaml",
    )
    output_info = os.path.join(
        get_package_share_directory("flowcean_ros"),
        "config",
        "output_topics.yaml",
    )

    model_transforms = []

    return LaunchDescription(
        [
            Node(
                package="flowcean_ros",
                namespace="flowcean",
                executable="flowcean_predictor",
                name="model_deployment",
                parameters=[
                    {
                        "input_threshold": 1.0,
                    },  # number of seconds for how long topic data is considered up-to-date
                    {
                        "input_info": input_info,
                    },  # yaml file with input topic information
                    {
                        "output_info": output_info,
                    },  # yaml file with output topic information
                ],
            ),
        ],
    )
