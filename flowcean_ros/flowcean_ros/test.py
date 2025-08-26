Schema(
    [
        (
            "/amcl_pose",
            List(
                Struct(
                    {
                        "time": Int64,
                        "value": Struct(
                            {
                                "pose.pose.position.x": Float64,
                                "pose.pose.position.y": Float64,
                                "pose.pose.orientation.x": Float64,
                                "pose.pose.orientation.y": Float64,
                                "pose.pose.orientation.z": Float64,
                                "pose.pose.orientation.w": Float64,
                            }
                        ),
                    }
                )
            ),
        ),
        (
            "/scan",
            List(
                Struct(
                    {
                        "time": Int64,
                        "value": Struct(
                            {
                                "ranges": List(Float64),
                                "angle_min": Float64,
                                "angle_max": Float64,
                                "angle_increment": Float64,
                                "range_min": Float64,
                                "range_max": Float64,
                            }
                        ),
                    }
                )
            ),
        ),
        (
            "/map",
            List(
                Struct(
                    {
                        "time": Int64,
                        "value": Struct(
                            {
                                "data": List(Int64),
                                "info.resolution": Float64,
                                "info.width": Int64,
                                "info.height": Int64,
                                "info.origin.position.x": Float64,
                                "info.origin.position.y": Float64,
                                "info.origin.position.z": Float64,
                                "info.origin.orientation.x": Float64,
                                "info.origin.orientation.y": Float64,
                                "info.origin.orientation.z": Float64,
                                "info.origin.orientation.w": Float64,
                            }
                        ),
                    }
                )
            ),
        ),
        (
            "/particle_cloud",
            List(
                Struct(
                    {
                        "time": Int64,
                        "value": Struct(
                            {
                                "particles": List(
                                    Struct(
                                        {
                                            "pose": Struct(
                                                {
                                                    "position": Struct(
                                                        {
                                                            "x": Float64,
                                                            "y": Float64,
                                                            "z": Float64,
                                                        }
                                                    ),
                                                    "orientation": Struct(
                                                        {
                                                            "x": Float64,
                                                            "y": Float64,
                                                            "z": Float64,
                                                            "w": Float64,
                                                        }
                                                    ),
                                                }
                                            ),
                                            "weight": Float64,
                                        }
                                    )
                                )
                            }
                        ),
                    }
                )
            ),
        ),
    ]
)
