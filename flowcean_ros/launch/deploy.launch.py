import os

import launch_ros

# flowcean imports
from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import SetEnvironmentVariable
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    package_path = get_package_share_directory("flowcean_ros")

    # Add robot_localization_failure to Python path for custom_transforms and ml_pipeline
    robot_loc_failure_path = "/home/workstation/ros2_ws/src/flowcean/examples/robot_localization_failure"
    current_pythonpath = os.environ.get("PYTHONPATH", "")
    if robot_loc_failure_path not in current_pythonpath:
        new_pythonpath = (
            f"{robot_loc_failure_path}:{current_pythonpath}"
            if current_pythonpath
            else robot_loc_failure_path
        )
    else:
        new_pythonpath = current_pythonpath

    # threshold in seconds for input to be considered valid
    threshold = 0.1

    # number of messages to buffer in the subscriber queue
    buffer_length = 10

    # yaml file with input topic information
    topics_info = os.path.join(
        package_path,
        "config",
        "topics_config_localization_monitor.yaml",
    )

    # Model path - supports both .fml files and ml_pipeline directories
    # Using test_1 model which doesn't require temporal features
    model_path = "/home/workstation/ros2_ws/src/flowcean/examples/robot_localization_failure/models/test_1_catboost_2026-01-03_17-16-51"

    # load a map from disk
    map_path = os.path.join(package_path, "maps", "unsymmetric_exp_1.pgm")

    # load map info from disk
    map_info_path = os.path.join(
        package_path,
        "maps",
        "unsymmetric_exp_1.yaml",
    )

    return LaunchDescription(
        [
            SetEnvironmentVariable("PYTHONPATH", new_pythonpath),
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
