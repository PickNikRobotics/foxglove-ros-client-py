from foxglove_ros_client import Header as ROS1Header
from foxglove_ros_client import Time

__all__ = ["Header"]


class Header(ROS1Header):
    def __init__(self, stamp=None, frame_id=None):
        super().__init__(stamp=stamp, frame_id=frame_id)
        self.data["stamp"] = Time(stamp["secs"], stamp["nsecs"]) if stamp else None
        self.data["frame_id"] = frame_id
        del self.data["seq"]
