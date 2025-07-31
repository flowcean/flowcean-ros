import os

import launch_ros

# flowcean imports
from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    threshold = 0.1  # threshold in seconds for input to be considered valid
    buffer_length = 10  # number of messages to buffer in the subscriber queue
    topics_info = os.path.join(
        get_package_share_directory("flowcean_ros"),
        "config",
        "topics_config.yaml",
    )  # yaml file with input topic information
    model_path = os.path.join(
        get_package_share_directory("flowcean_ros"),
        "models",
        "rec_20250704_173434_sliced.fml",
    )
    map_path = os.path.join(
        get_package_share_directory("flowcean_ros"),
        "maps",
        "warehouse_slamtoolbox.pgm",
    )
    map_info_path = os.path.join(
        get_package_share_directory("flowcean_ros"),
        "maps",
        "warehouse_slamtoolbox.yaml",
    )

    return LaunchDescription(
        [
            launch_ros.actions.SetParameter(name="use_sim_time", value=True),
            Node(
                package="flowcean_ros",
                namespace="flowcean",
                executable="flowcean_predictor",
                name="model_deployment",
                output="screen",
                parameters=[
                    {
                        "input_threshold": threshold,
                    },
                    {
                        "buffer_length": buffer_length,
                    },
                    {
                        "topics_info": topics_info,
                    },
                    {
                        "model_path": model_path,
                    },
                    {
                        "map_path": map_path,
                    },
                    {
                        "map_info_path": map_info_path,
                    },
                    {
                        "use_rosbag": True,
                    },
                ],
            ),
        ],
    )
