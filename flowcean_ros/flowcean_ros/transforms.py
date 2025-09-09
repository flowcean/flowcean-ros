
from flowcean.core import Transform

# TODO: FIX IMPORTS LIKE MARKUS HAS
from flowcean.polars.transforms.explode import ExplodeTimeSeries
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
    )