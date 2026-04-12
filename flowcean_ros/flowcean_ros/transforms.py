from collections import deque

import polars as pl
from custom_transforms.collapse import Collapse
from custom_transforms.scan_map_statistics import ScanMapStatistics
from custom_transforms.particle_cloud_statistics import ParticleCloudStatistics

from flowcean.core import Transform
from flowcean.core.transform import Lambda
from flowcean.polars.transforms.zero_order_hold_matching import ZeroOrderHold


# Global variable to store the map data
_OCCUPANCY_MAP = None

# Feature history for temporal feature computation (diff1, mean5, std5).
# Stores the last 5 base feature dicts (before temporal features are added).
_FEATURE_HISTORY: deque = deque(maxlen=5)
_USE_TEMPORAL_FEATURES: bool = False


def set_occupancy_map(occupancy_map: dict) -> None:
    """Set the global occupancy map for feature extraction."""
    global _OCCUPANCY_MAP
    _OCCUPANCY_MAP = occupancy_map


def set_use_temporal_features(use: bool) -> None:
    """Enable or disable temporal feature computation at inference time."""
    global _USE_TEMPORAL_FEATURES
    _USE_TEMPORAL_FEATURES = use


def reset_feature_history() -> None:
    """Clear the feature history buffer (call on AMCL reset or node startup)."""
    _FEATURE_HISTORY.clear()


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

        # AMCL covariance: σ²_x, σ²_y, σ²_θ from pose.covariance[0,7,35]
        if "/amcl_pose" in df_collected.columns:
            amcl_ts = df_collected["/amcl_pose"][0]
            if amcl_ts is not None and len(amcl_ts) > 0:
                amcl_val = amcl_ts[-1]["value"]
                if isinstance(amcl_val, dict):
                    cov = amcl_val.get("pose.covariance")
                    if cov is not None and len(cov) >= 36:
                        feature_dict["amcl_cov_x"] = float(cov[0])
                        feature_dict["amcl_cov_y"] = float(cov[7])
                        feature_dict["amcl_cov_yaw"] = float(cov[35])

        # Odometry velocity: forward speed and rotation rate
        if "/imperfect_odom" in df_collected.columns:
            odom_ts = df_collected["/imperfect_odom"][0]
            if odom_ts is not None and len(odom_ts) > 0:
                odom_val = odom_ts[-1]["value"]
                if isinstance(odom_val, dict):
                    feature_dict["odom_linear_x"] = float(odom_val.get("twist.twist.linear.x", 0.0))
                    feature_dict["odom_angular_z"] = float(odom_val.get("twist.twist.angular.z", 0.0))

        # Commanded velocity from Nav2
        if "/cmd_vel" in df_collected.columns:
            cmd_ts = df_collected["/cmd_vel"][0]
            if cmd_ts is not None and len(cmd_ts) > 0:
                cmd_val = cmd_ts[-1]["value"]
                if isinstance(cmd_val, dict):
                    feature_dict["cmd_linear_x"] = float(cmd_val.get("linear.x", 0.0))
                    feature_dict["cmd_angular_z"] = float(cmd_val.get("angular.z", 0.0))

        # Always push base features to history (cheap; keeps deque current
        # even when temporal features are off, so enabling them mid-session
        # works immediately without a warm-up gap).
        _FEATURE_HISTORY.append(dict(feature_dict))

        # Temporal features: diff1, mean5, std5 over the rolling window.
        # Mirrors add_temporal_features() in ml_pipeline/utils/common.py.
        # Note: training drops the first row (diff1=NaN); here we use 0.0
        # instead so inference never stalls waiting for a full window.
        if _USE_TEMPORAL_FEATURES:
            for col in list(feature_dict.keys()):
                vals = [
                    h[col] for h in _FEATURE_HISTORY
                    if col in h and h[col] is not None
                ]
                n = len(vals)
                feature_dict[f"{col}_diff1"] = (
                    float(vals[-1] - vals[-2]) if n >= 2 else 0.0
                )
                mean = sum(vals) / n if n else 0.0
                feature_dict[f"{col}_mean5"] = float(mean)
                variance = (
                    sum((v - mean) ** 2 for v in vals) / n if n >= 2 else 0.0
                )
                feature_dict[f"{col}_std5"] = float(variance ** 0.5)

        # Create a single-row DataFrame with all features
        result_df = pl.DataFrame([feature_dict])

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
