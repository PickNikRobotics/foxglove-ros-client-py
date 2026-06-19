from foxglove_ros_client._cdr import CdrCodec, normalize_ros_type, to_rosbags_typename


def test_string_message_round_trips_from_plain_dict():
    codec = CdrCodec()
    data = codec.serialize("std_msgs/String", "string data", {"data": "hello"})
    assert codec.deserialize("std_msgs/msg/String", "string data", data) == {"data": "hello"}


def test_numeric_arrays_accept_plain_lists():
    codec = CdrCodec()
    schema = "float64[] position\nuint8[16] uuid"
    value = {"position": [1, 2.5], "uuid": [7] * 16}
    data = codec.serialize("example_msgs/msg/State", schema, value)
    assert codec.deserialize("example_msgs/msg/State", schema, data) == {
        "position": [1.0, 2.5],
        "uuid": [7] * 16,
    }


def test_service_request_response_typenames_are_rosbags_compatible():
    assert to_rosbags_typename("std_srvs/srv/Trigger_Request") == "std_srvs/msg/Trigger_Request"
    assert to_rosbags_typename("std_srvs/srv/Trigger_Response") == "std_srvs/msg/Trigger_Response"


def test_type_normalization_accepts_ros1_style_names():
    assert normalize_ros_type("std_msgs/String", "msg") == "std_msgs/msg/String"
    assert normalize_ros_type("/std_srvs/Trigger", "srv") == "std_srvs/srv/Trigger"


def test_reused_nested_type_names_are_isolated_by_root_schema():
    codec = CdrCodec()
    schema_a = """pkg/Nested nested
================================================================================
MSG: pkg/Nested
string value
"""
    schema_b = """pkg/Nested nested
================================================================================
MSG: pkg/Nested
int32 value
"""
    data_a = codec.serialize("pkg/msg/A", schema_a, {"nested": {"value": "hello"}})
    data_b = codec.serialize("pkg/msg/B", schema_b, {"nested": {"value": 42}})

    assert codec.deserialize("pkg/msg/A", schema_a, data_a) == {"nested": {"value": "hello"}}
    assert codec.deserialize("pkg/msg/B", schema_b, data_b) == {"nested": {"value": 42}}
