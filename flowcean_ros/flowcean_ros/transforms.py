import polars as pl
from custom_transforms.collapse import Collapse
from custom_transforms.scan_map_statistics import ScanMapStatistics
from custom_transforms.particle_cloud_statistics import ParticleCloudStatistics

from flowcean.core import Transform
from flowcean.core.transform import Lambda
from flowcean.polars.transforms.zero_order_hold_matching import ZeroOrderHold


# Global variable to store the map data
_OCCUPANCY_MAP = None


def set_occupancy_map(occupancy_map: dict) -> None:
    """Set the global occupancy map for feature extraction."""
    global _OCCUPANCY_MAP
    _OCCUPANCY_MAP = occupancy_map


def get_transform() -> Transform:
    return localization_monitor_transforms()


def localization_monitor_transforms() -> Transform:
    def convert_map_to_bool(df: pl.LazyFrame) -> pl.LazyFrame:
        return df.with_columns(
            pl.col("/map").struct.with_fields(
                pl.field("data").list.eval(pl.element() != 0),
            ),
        )

    def extract_features_from_timeseries(df: pl.LazyFrame) -> pl.LazyFrame:
        """Extract the latest value from each feature timeseries."""
        df_collected = df.collect()

        # List of all feature columns produced by ScanMapStatistics and ParticleCloudStatistics
        scanmap_cols = [
            "point_distance", "point_fitting", "point_inlier", "point_quality",
            "ray_inlier", "ray_inlier_percent", "ray_matching_percent",
            "ray_outlier_percent", "ray_quality",
            "angle_inlier", "angle_quality",
            "line_angle", "line_distance", "line_fitting", "line_length",
        ]
        pcs_cols = [
            "cog_max_distance", "cog_mean_dist", "cog_mean_absolute_deviation",
            "cog_median", "cog_median_absolute_deviation",
            "cog_min_distance", "cog_standard_deviation",
            "circle_radius", "circle_mean", "circle_mean_absolute_deviation",
            "circle_median", "circle_median_absolute_deviation",
            "circle_min_distance", "circle_standard_deviation",
            "num_clusters",
            "main_cluster_variance_x", "main_cluster_variance_y",
        ]

        all_feature_cols = scanmap_cols + pcs_cols

        # Extract the latest value from each feature timeseries
        feature_dict = {}
        for col in all_feature_cols:
            if col in df_collected.columns:
                timeseries = df_collected[col][0]
                # Check if timeseries is not None and has elements
                if timeseries is not None and len(timeseries) > 0:
                    # Get the most recent value
                    feature_dict[col] = timeseries[-1]["value"]
                else:
                    feature_dict[col] = None

        # Create a single-row DataFrame with all features
        result_df = pl.DataFrame([feature_dict])

        # Note: Temporal features (diff1, mean5, std5) are not added here
        # Use a model trained without temporal_features=false
        # To support temporal features, maintain a buffer of past samples

        return result_df.lazy()

    # Add feature extraction if map is available
    # Feature extraction happens on RAW data (before ZeroOrderHold restructuring)
    # because the transforms expect the original rosbag-like structure
    if _OCCUPANCY_MAP is not None:
        return (
            Collapse("/map", element=0)
            | Lambda(convert_map_to_bool)
            | ScanMapStatistics(
                occupancy_map=_OCCUPANCY_MAP,
                scan_topic="/scan",
                sensor_pose_topic="/amcl_pose",
            )
            | ParticleCloudStatistics(
                particle_cloud_feature_name="/particle_cloud",
            )
            | Lambda(extract_features_from_timeseries)
        )
    else:
        # Base transforms for data restructuring (when no map/features available)
        return (
            Collapse("/map", element=0)
            | Lambda(convert_map_to_bool)
            | ZeroOrderHold(
                features=[
                    "/scan",
                    "/particle_cloud",
                    "/amcl_pose",
                ],
                name="measurements",
            )
        )


def explode_and_collect_samples(data: pl.LazyFrame) -> pl.LazyFrame:
    return (
        data.explode("measurements")
        .unnest("measurements")
        .unnest("value")
        .select(
            pl.col("/map"),
            pl.struct(
                [
                    pl.col("/scan/ranges").alias("ranges"),
                    pl.col("/scan/angle_min").alias("angle_min"),
                    pl.col("/scan/angle_max").alias("angle_max"),
                    pl.col("/scan/angle_increment").alias(
                        "angle_increment",
                    ),
                    pl.col("/scan/range_min").alias("range_min"),
                    pl.col("/scan/range_max").alias("range_max"),
                ],
            ).alias("/scan"),
            pl.col("/particle_cloud/particles").alias(
                "/particle_cloud",
            ),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.col(
                                "/amcl_pose/pose.pose.position.x",
                            ).alias("position.x"),
                            pl.col(
                                "/amcl_pose/pose.pose.position.y",
                            ).alias("position.y"),
                            pl.col(
                                "/amcl_pose/pose.pose.orientation.x",
                            ).alias("orientation.x"),
                            pl.col(
                                "/amcl_pose/pose.pose.orientation.y",
                            ).alias("orientation.y"),
                            pl.col(
                                "/amcl_pose/pose.pose.orientation.z",
                            ).alias("orientation.z"),
                            pl.col(
                                "/amcl_pose/pose.pose.orientation.w",
                            ).alias("orientation.w"),
                        ],
                    ).alias("pose"),
                ],
            ).alias("/amcl_pose"),
        )
    )
