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
                    {"model_path": "/path/to/your/model.pkl"},
                    {"model_name": "flowcean_model"},
                    {"input_threshold": 1.0},
                    {"input_info": input_info},
                    {"output_info": output_info},
                ],
            ),
        ],
    )
