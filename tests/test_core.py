from foxglove_ros_client import Header, Message, ServiceRequest, Time
from foxglove_ros_client.ros2 import Header as ROS2Header


def test_message_types_are_dict_like():
    msg = Message({"data": "hello"})
    assert msg["data"] == "hello"
    msg["data"] = "updated"
    assert dict(msg) == {"data": "updated"}


def test_time_helpers_match_roslibpy_shape():
    stamp = Time.from_sec(1.25)
    assert stamp.secs == 1
    assert stamp.nsecs == 250000000
    assert stamp.to_nsec() == 1250000000
    assert stamp.to_sec() == 1.25


def test_headers_accept_roslibpy_stamp_dicts():
    header = Header(seq=1, stamp={"secs": 2, "nsecs": 3}, frame_id="map")
    assert header["seq"] == 1
    assert header["stamp"].secs == 2
    assert header["frame_id"] == "map"

    ros2_header = ROS2Header(stamp={"secs": 2, "nsecs": 3}, frame_id="base")
    assert "seq" not in ros2_header
    assert ros2_header["stamp"].nsecs == 3


def test_service_request_is_dict_like():
    request = ServiceRequest({"name": "thing"})
    assert dict(request) == {"name": "thing"}
