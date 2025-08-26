from mission_control.api.state_change_data import StateChangeData
from mission_control.features.generated.features import *
from mission_control.features.generated.triggers import Triggers
from mission_control.smd.smd_utils import *
from mission_control.state_machine.state_machine import StateMachine

DELAY_AFTER_POSE_RESET = 3
LOCALIZATION_FAILURE_LIMIT = 30
ROUTE_NAME = "test"


def not_localized():
    return sequence("not_localized")(
        # trigger_after_delay(delay_in_seconds=1.0),
        # localization_lifecycle_shutdown,
        trigger_after_delay(delay_in_seconds=1.0),
        get_isaac_sim_ground_truth,        
        # localization_lifecycle_start,
        trigger_after_delay(delay_in_seconds=1.0),
        set_initial_pose,
        trigger_after_delay(delay_in_seconds=DELAY_AFTER_POSE_RESET)
    )

def get() -> StateMachine:
    root = parallel("root")(
        status_monitoring,
        localization_monitoring(
            localization_failure_limit=LOCALIZATION_FAILURE_LIMIT
        ),
        sequence("_")(
            trigger_after_delay(delay_in_seconds=1),
            localization_lifecycle_start,
            trigger_after_delay(delay_in_seconds=1),
            get_isaac_sim_ground_truth,
            set_initial_pose,
            trigger_after_delay(delay_in_seconds=DELAY_AFTER_POSE_RESET),
            load_route(route_name=ROUTE_NAME),
            loop("initialized")(
                step_route,
                parallel("navigation")(
                    autonomous_navigation,
                    command_movement,
                ).until(
                    Triggers.goal_reached,
                    Triggers.goal_not_reached,
                ),
            )
            .when(
                Triggers.localization_difference_above_threshold
                > not_localized(),
                Triggers.route_end_reached
                > load_route(route_name=ROUTE_NAME),
            )
            .until(
                Triggers.localization_failure_limit_reached,
            ),
            parallel("experiment finished")()
        ),
    ).build(None)

    return StateMachine(root)
