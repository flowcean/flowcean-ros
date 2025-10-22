import os

import launch_ros

# flowcean imports
from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    package_path = get_package_share_directory("flowcean_ros")

    # threshold in seconds for input to be considered valid
    threshold = 0.1  

    # number of messages to buffer in the subscriber queue
    buffer_length = 10

    # yaml file with input topic information
    topics_info = os.path.join(package_path, "config", "topics_config.yaml",)
    
    # fml file containing the trained model
    model_path = os.path.join(package_path, "models", "model.fml")
    
    # load a map from disk
    map_path = os.path.join(package_path, "maps", "warehouse_slamtoolbox.pgm")

    # load map info from disk
    map_info_path = os.path.join(package_path, "maps", "warehouse_slamtoolbox.yaml")

    return LaunchDescription(
        [
            launch_ros.actions.SetParameter(name="use_sim_time", value=True),
            Node(
                package="flowcean_ros",
                namespace="flowcean",
                executable="predictor_node",
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
                        "map_file": map_path,
                    },
                    {
                        "map_info_file": map_info_path,
                    },
                    {
                        "use_map_file": True,
                    },
                ],
            ),
        ],
    )
