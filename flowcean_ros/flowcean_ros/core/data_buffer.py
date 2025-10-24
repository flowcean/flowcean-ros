from typing import Any

from builtin_interfaces.msg import Time
from core.msg_data import MsgData


class DataBuffer(dict[str, MsgData]):
    """A buffer for storing message data indexed by topic name."""

    length: int

    def __init__(self, length: int = 1) -> None:
        super().__init__()
        self.length = length

    def store(
        self,
        topic_name: str,
        ros_msg: Any,
        timestamp: Time,
    ) -> None:
        """Store a message and its timestamp in the buffer.

        Args:
            topic_name: The name of the topic the message belongs to.
            ros_msg: The ROS message to store.
            timestamp: The timestamp of the message.
        """
        msg_data: MsgData = self.get(topic_name, None)

        if msg_data is None:
            err = f"Topic '{topic_name}' not found in buffer."
            raise KeyError(err)

        msg_data.stamp(timestamp)

        for field in msg_data.keys():
            value = ros_msg
            for sub in field.rsplit("."):
                value = getattr(value, sub)
            msg_data[field] = value

    def is_full(self) -> bool:
        """Check if msgs for all topics have been received."""
        return all(v.data_complete() for v in self.values())

    def empty(self) -> None:
        """Deletes old msgs from the buffer but keeps single msgs."""
        for v in self.values():
            v.empty()
