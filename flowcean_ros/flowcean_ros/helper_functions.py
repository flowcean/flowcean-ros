import math
from geometry_msgs.msg import Quaternion
from core.logic import helper_function

@helper_function
def quaternion_from_euler(*args):
    """
    Converts euler roll, pitch, yaw to quaternion (w in last place)
    """
    (yaw, ) = args

    roll, pitch = 0
    cy = math.cos(yaw * 0.5)
    sy = math.sin(yaw * 0.5)
    cp = math.cos(pitch * 0.5)
    sp = math.sin(pitch * 0.5)
    cr = math.cos(roll * 0.5)
    sr = math.sin(roll * 0.5)

    x = cy * cp * cr + sy * sp * sr
    y = cy * cp * sr - sy * sp * cr
    z = sy * cp * sr + cy * sp * cr
    w = sy * cp * cr - cy * sp * sr

    return x, y, z, w