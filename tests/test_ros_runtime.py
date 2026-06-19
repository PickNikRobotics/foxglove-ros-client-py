import threading

from foxglove_ros_client._cdr import CdrCodec
from foxglove_ros_client._protocol import FoxgloveChannel, FoxgloveService
from foxglove_ros_client.event_emitter import EventEmitterMixin
from foxglove_ros_client.ros import Ros


class FakeProtocol:
    def __init__(self):
        self.subscription_id = 1
        self.call_id = 1
        self.subscribed = []
        self.calls = []

    def on(self, *_args):
        pass

    def subscribe(self, channel_id):
        self.subscribed.append(channel_id)
        result = self.subscription_id
        self.subscription_id += 1
        return result

    def unsubscribe(self, _subscription_id):
        pass

    def advertise_client_channel(self, topic, encoding, schema_name):
        self.advertised = (topic, encoding, schema_name)
        return 99

    def unadvertise_client_channel(self, _channel_id):
        pass

    def publish_message(self, channel_id, data):
        self.published = (channel_id, data)

    def get_parameters(self, names, request_id):
        self.get_parameter_request = (names, request_id)

    def set_parameters(self, parameters, request_id):
        self.set_parameter_request = (parameters, request_id)

    def call_service(self, service_id, encoding, request_data):
        self.calls.append((service_id, encoding, request_data))
        result = self.call_id
        self.call_id += 1
        return result


def make_ros():
    ros = Ros.__new__(Ros)
    EventEmitterMixin.__init__(ros)
    ros._id_counter = 0
    ros.protocol = FakeProtocol()
    ros.codec = CdrCodec()
    ros.channels = {}
    ros.channels_by_topic = {}
    ros.services = {}
    ros.services_by_name = {}
    ros._subscriptions = {}
    ros._subscriptions_by_topic = {}
    ros._pending_subscribers = []
    ros._client_channels = {}
    ros._pending_services = {}
    ros._pending_params = {}
    ros._known_params = {}
    ros._next_param_request_id = 1
    ros._lock = threading.RLock()
    ros._advertisements_ready = threading.Event()
    ros._channels_ready = threading.Event()
    ros._services_ready = threading.Event()
    return ros


def test_pending_topic_subscribes_when_channel_is_advertised():
    ros = make_ros()
    received = []

    ros.subscribe_topic("/chatter", "std_msgs/msg/String", received.append)
    assert ros._pending_subscribers

    channel = FoxgloveChannel(1, "/chatter", "cdr", "std_msgs/msg/String", "string data")
    ros._on_advertise([channel])
    assert ros.protocol.subscribed == [1]

    data = ros.codec.serialize("std_msgs/msg/String", "string data", {"data": "from server"})
    ros._on_message(1, 0, data)
    assert received[0]["data"] == "from server"


def test_service_response_decodes_to_service_response():
    ros = make_ros()
    service = FoxgloveService(
        id=5,
        name="/trigger",
        type="std_srvs/srv/Trigger",
        request_schema="",
        response_schema="bool success\nstring message",
    )
    ros.services[5] = service
    ros.services_by_name["/trigger"] = service

    responses = []
    ros.call_service_async("/trigger", "std_srvs/srv/Trigger", {}, responses.append)

    response_data = ros.codec.serialize(
        "std_srvs/srv/Trigger_Response",
        "bool success\nstring message",
        {"success": True, "message": "ok"},
    )
    ros._on_service_response(5, 1, "cdr", response_data)
    assert responses[0]["success"] is True
    assert responses[0]["message"] == "ok"


def test_publish_topic_uses_cdr_when_schema_is_advertised():
    ros = make_ros()
    ros._on_advertise([FoxgloveChannel(1, "/chatter", "cdr", "std_msgs/msg/String", "string data")])

    ros.publish_topic("/python/chatter", "std_msgs/String", {"data": "hello"})

    assert ros.protocol.advertised == ("/python/chatter", "cdr", "std_msgs/msg/String")
    channel_id, payload = ros.protocol.published
    assert channel_id == 99
    assert ros.codec.deserialize("std_msgs/msg/String", "string data", payload) == {"data": "hello"}


def test_publish_topic_falls_back_to_json_for_unknown_schema():
    ros = make_ros()
    ros.publish_topic("/python/unknown", "custom_msgs/msg/Unknown", {"data": "hello"})

    assert ros.protocol.advertised == ("/python/unknown", "json", "custom_msgs/msg/Unknown")
    assert ros.protocol.published == (99, b'{"data": "hello"}')


def test_rosapi_style_topic_and_service_discovery_uses_foxglove_registry():
    ros = make_ros()
    ros._on_advertise(
        [
            FoxgloveChannel(1, "/chatter", "cdr", "std_msgs/msg/String", "string data"),
            FoxgloveChannel(2, "/joint_states", "cdr", "sensor_msgs/msg/JointState", "string[] name"),
        ]
    )
    ros._on_advertise_services(
        [
            FoxgloveService(5, "/trigger", "std_srvs/srv/Trigger", "", "bool success\nstring message"),
            FoxgloveService(6, "/other", "example_interfaces/srv/AddTwoInts", "int64 a\nint64 b", "int64 sum"),
        ]
    )

    assert ros.get_topics() == ["/chatter", "/joint_states"]
    assert ros.get_topic_type("/chatter") == "std_msgs/msg/String"
    assert ros.get_topics_for_type("std_msgs/String") == ["/chatter"]
    assert ros.get_services() == ["/other", "/trigger"]
    assert ros.get_service_type("/trigger") == "std_srvs/srv/Trigger"
    assert ros.get_services_for_type("std_srvs/Trigger") == ["/trigger"]
    assert ros.wait_for_advertisements(0)
    assert ros._channels_ready.is_set()
    assert ros._services_ready.is_set()


def test_schema_details_match_rosapi_typedef_shape():
    ros = make_ros()
    ros._on_advertise(
        [
            FoxgloveChannel(
                1,
                "/joint_states",
                "cdr",
                "sensor_msgs/msg/JointState",
                "std_msgs/Header header\nstring[] name\nfloat64[] position\n"
                "================================================================================\n"
                "MSG: std_msgs/Header\n"
                "builtin_interfaces/Time stamp\nstring frame_id",
            )
        ]
    )

    details = ros.get_message_details("sensor_msgs/JointState")
    assert details["typedefs"][0]["type"] == "sensor_msgs/msg/JointState"
    assert details["typedefs"][0]["fieldnames"] == ["header", "name", "position"]
    assert details["typedefs"][0]["fieldarraylen"] == [-1, 0, 0]
    assert details["typedefs"][1]["type"] == "std_msgs/Header"


def test_param_names_are_tracked_from_set_and_parameter_values():
    ros = make_ros()
    ros.set_param_async("/node:param", 42)
    assert ros.get_params() == ["node.param"]

    received = []
    ros.get_param_async("/node:param", received.append)
    ros._on_parameter_values("param_get_2", [{"name": "node.param", "value": 43}])
    assert received == [43]
    assert ros.get_params() == ["node.param"]


def test_action_server_discovery_uses_hidden_send_goal_services():
    ros = make_ros()
    ros._on_advertise_services(
        [
            FoxgloveService(
                1,
                "/fibonacci/_action/send_goal",
                "example_interfaces/action/Fibonacci_SendGoal",
                "",
                "",
            ),
            FoxgloveService(2, "/ordinary", "std_srvs/srv/Trigger", "", ""),
        ]
    )
    received = []
    ros.get_action_servers(received.append)
    assert received == [["/fibonacci"]]


def test_action_goal_uses_send_goal_then_get_result_services():
    ros = make_ros()
    ros._on_advertise_services(
        [
            FoxgloveService(
                1,
                "/fibonacci/_action/send_goal",
                "example_interfaces/action/Fibonacci_SendGoal",
                SEND_GOAL_REQUEST_SCHEMA,
                SEND_GOAL_RESPONSE_SCHEMA,
            ),
            FoxgloveService(
                2,
                "/fibonacci/_action/get_result",
                "example_interfaces/action/Fibonacci_GetResult",
                GET_RESULT_REQUEST_SCHEMA,
                GET_RESULT_RESPONSE_SCHEMA,
            ),
        ]
    )

    results = []
    ros.send_action_goal(
        "/fibonacci",
        "example_interfaces/action/Fibonacci",
        "goal-1",
        {"order": 3},
        results.append,
        None,
        None,
    )
    assert len(ros.protocol.calls) == 1
    assert ros.protocol.calls[0][0] == 1

    accepted = ros.codec.serialize(
        "example_interfaces/action/Fibonacci_SendGoal_Response",
        SEND_GOAL_RESPONSE_SCHEMA,
        {"accepted": True, "stamp": {"sec": 0, "nanosec": 0}},
    )
    ros._on_service_response(1, 1, "cdr", accepted)
    assert len(ros.protocol.calls) == 2
    assert ros.protocol.calls[1][0] == 2

    action_result = ros.codec.serialize(
        "example_interfaces/action/Fibonacci_GetResult_Response",
        GET_RESULT_RESPONSE_SCHEMA,
        {"status": 4, "result": {"sequence": [0, 1, 1]}},
    )
    ros._on_service_response(2, 2, "cdr", action_result)
    assert results[0]["status"].name == "SUCCEEDED"
    assert results[0]["values"] == {"sequence": [0, 1, 1]}


def test_flattened_action_goal_schema_does_not_wrap_goal_field():
    ros = make_ros()
    ros._on_advertise_services(
        [
            FoxgloveService(
                1,
                "/flat_action/_action/send_goal",
                "example_interfaces/action/FlatAction_SendGoal",
                FLAT_ACTION_SEND_GOAL_REQUEST_SCHEMA,
                SEND_GOAL_RESPONSE_SCHEMA,
            ),
            FoxgloveService(
                2,
                "/flat_action/_action/get_result",
                "example_interfaces/action/FlatAction_GetResult",
                GET_RESULT_REQUEST_SCHEMA,
                "int8 status\nstring error_message",
            ),
        ]
    )

    ros.send_action_goal(
        "/flat_action",
        "example_interfaces/action/FlatAction",
        "goal-1",
        {"label": "sample", "priority": 3},
        lambda _result: None,
        None,
        None,
    )

    request_bytes = ros.protocol.calls[0][2]
    decoded = ros.codec.deserialize(
        "example_interfaces/action/FlatAction_SendGoal_Request",
        FLAT_ACTION_SEND_GOAL_REQUEST_SCHEMA,
        request_bytes,
    )
    assert "goal" not in decoded
    assert decoded["label"] == "sample"
    assert decoded["priority"] == 3


def test_action_goal_reports_include_hidden_hint_when_endpoints_are_missing():
    ros = make_ros()
    errors = []

    ros.send_action_goal(
        "/flat_action",
        "example_interfaces/action/FlatAction",
        "goal-1",
        {"label": "sample", "priority": 3},
        lambda _result: None,
        None,
        errors.append,
    )

    assert len(ros.protocol.calls) == 0
    assert "include_hidden:=true" in str(errors[0])


SEND_GOAL_REQUEST_SCHEMA = """unique_identifier_msgs/UUID goal_id
example_interfaces/action/Fibonacci_Goal goal
================================================================================
MSG: unique_identifier_msgs/UUID
uint8[16] uuid
================================================================================
MSG: example_interfaces/action/Fibonacci_Goal
int32 order
"""

SEND_GOAL_RESPONSE_SCHEMA = """bool accepted
builtin_interfaces/Time stamp
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"""

GET_RESULT_REQUEST_SCHEMA = """unique_identifier_msgs/UUID goal_id
================================================================================
MSG: unique_identifier_msgs/UUID
uint8[16] uuid
"""

GET_RESULT_RESPONSE_SCHEMA = """int8 status
example_interfaces/action/Fibonacci_Result result
================================================================================
MSG: example_interfaces/action/Fibonacci_Result
int32[] sequence
"""

FLAT_ACTION_SEND_GOAL_REQUEST_SCHEMA = """unique_identifier_msgs/UUID goal_id
string label
int32 priority
================================================================================
MSG: unique_identifier_msgs/UUID
uint8[16] uuid
"""
