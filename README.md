# flowcean_ros package

This package provides an interface for using Flowcean in ROS 2 Humble.

## Pre-requisites

To use this package, you need to have ROS 2 installed on your system. You can follow the instructions on the [ROS 2 installation page](https://docs.ros.org/en/humble/Installation.html) to install ROS 2 on your system. You will also need to [create a workspace](https://docs.ros.org/en/humble/Tutorials/Beginner-Client-Libraries/Creating-A-Workspace/Creating-A-Workspace.html) to build the package.

## Installation

To be able to use the latest features of Flowcean, you need to clone the Flowcean repository and install it from a local project path. Then build the flowcean_ros package, as some features may not be included in the latest PyPi release.

Here are the exact steps to install the package:

```bash
# clone flowcean_ros into your ROS 2 workspace
mkdir -p ~/ros2_ws/src
cd ~/ros2_ws/src
git clone https://github.com/flowcean/flowcean-ros

# clone flowcean to your desired location
cd /your/path/to
git clone https://github.com/flowcean/flowcean

# install flowcean system-wide
python3.10 -m pip install /your/absolute/path/to/flowcean

# build the package
cd ~/ros2_ws
colcon build --packages-select flowcean_ros
source install/setup.bash # or setup.zsh
```
