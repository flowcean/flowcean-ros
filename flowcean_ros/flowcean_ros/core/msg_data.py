from typing import Any

from builtin_interfaces.msg import Time

_TO_NANO = 1e9


class MsgData(dict[str, Any]):
    """A class to hold message data with a topic name and timestamp."""

    _topic_name: str
    _is_single: bool
    _stamp: float

    def __init__(
        self,
        topic: str,
        fields: list[str],
        is_single: bool,
    ) -> None:
        """Create a MsgData instance to store fields of a message.

        Args:
            topic_name: The name of the topic this message belongs to.
            fields: Fields of ROS message to store
            is_single: If True, this ROS messages is published once only
        """
        super().__init__()
        self._topic_name = topic
        for field in fields:
            self.add_field(field)
        self._is_single: bool = is_single
        self._stamp: float = None

    def __setitem__(self, key: str, value: Any) -> None:
        if key not in self:
            self.add_field(key)
        super().__setitem__(key, value)

    def empty(self) -> None:
        if not self._is_single:
            self._stamp = None
            self.add_field(*self.keys())

    def stamp(self, timestamp: Time) -> None:
        self._stamp = timestamp.sec * _TO_NANO + timestamp.nanosec

    def add_field(self, *field: str) -> None:
        for f in list(field):
            super().__setitem__(f, None)

    def data_complete(self) -> bool:
        """Check if msg data is available."""
        return not any(v is None for v in super().values())

    def get_stamp(self) -> int:
        return self._stamp

    def get_topic_name(self) -> str:
        return self._topic_name
