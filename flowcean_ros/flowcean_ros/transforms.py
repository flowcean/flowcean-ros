
from flowcean.core import Transform

from flowcean.polars.transforms.explode_time_series import ExplodeTimeSeries
from flowcean.polars.transforms.zero_order_hold_matching import ZeroOrderHold


def get_transform() -> Transform:
    transform = turtle_transforms()
    return transform

def turtle_transforms() -> Transform:
    return (
        ZeroOrderHold(
            features=[
                "/turtle1/cmd_vel",
                "/turtle1/pose",
            ],
            name="measurements",
        )
        | ExplodeTimeSeries("measurements")
        # ZeroOrderHold(
            # features=[
                # "/turtle1/cmd_vel",
                # "/turtle1/pose",
            # ],
            # name="measurements",
        # )
        # | ExplodeTimeSeries("measurements")
    )