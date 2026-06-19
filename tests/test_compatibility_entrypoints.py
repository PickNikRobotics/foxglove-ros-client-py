import pytest

from foxglove_ros_client.__main__ import build_parser
from foxglove_ros_client.comm import RosBridgeClientFactory
from foxglove_ros_client.comm.comm import RosBridgeProtocol
from foxglove_ros_client.comm.comm_autobahn import AutobahnRosBridgeClientFactory
from foxglove_ros_client.comm.comm_cli import CliRosBridgeClientFactory
from foxglove_ros_client.ros1.actionlib import ActionClient, Goal, GoalStatus, SimpleActionServer


class FakeRos:
    def __init__(self):
        self._id_counter = 0
        self.published = []
        self.subscribed = []
        self.unsubscribed = []
        self.unpublished = []

    @property
    def id_counter(self):
        self._id_counter += 1
        return self._id_counter

    def subscribe_topic(self, topic, message_type, callback, throttle_rate=0):
        self.subscribed.append((topic, message_type, callback, throttle_rate))

    def unsubscribe_topic(self, topic):
        self.unsubscribed.append(topic)

    def publish_topic(self, topic, message_type, message):
        self.published.append((topic, message_type, message))

    def unpublish_topic(self, topic):
        self.unpublished.append(topic)

    def call_later(self, *_args):
        return None

    def call_in_thread(self, callback):
        callback()


def test_comm_factory_url_helpers_remain_importable():
    assert RosBridgeClientFactory.create_url("localhost", 8765) == "ws://localhost:8765"
    assert AutobahnRosBridgeClientFactory.create_url("localhost", 8765) == "ws://localhost:8765"
    assert CliRosBridgeClientFactory.create_url("localhost", 8765) == "ws://localhost:8765"
    assert RosBridgeProtocol is not None
    with pytest.raises(ValueError):
        RosBridgeClientFactory.create_url("localhost")


def test_cli_parser_accepts_roslibpy_style_topic_command():
    args = build_parser().parse_args(["-r", "127.0.0.1", "-p", "8765", "topic", "type", "/chatter"])
    assert args.ros_host == "127.0.0.1"
    assert args.ros_port == 8765
    assert args.topic == "/chatter"


def test_cli_parser_accepts_action_list_command():
    args = build_parser().parse_args(["action", "list"])
    assert args.command == "action"
    assert args.subcommand == "list"


def test_ros1_action_client_goal_receives_result():
    ros = FakeRos()
    client = ActionClient(ros, "/test_action", "example/TestAction")
    goal = Goal(client, {"order": 3})

    goal.send()
    assert ros.published[-1][0] == "/test_action/goal"

    client._on_result_message(
        {
            "status": {"goal_id": {"id": goal.goal_id}, "status": GoalStatus.SUCCEEDED},
            "result": {"sequence": [0, 1, 1]},
        }
    )
    assert goal.wait(0.01) == {"sequence": [0, 1, 1]}
    assert goal.status["status"] == GoalStatus.SUCCEEDED


def test_simple_action_server_publishes_success_result():
    ros = FakeRos()
    server = SimpleActionServer(ros, "/test_action", "example/TestAction")
    goals = []
    server.start(goals.append)

    server._on_goal_message({"goal_id": {"stamp": {"secs": 0, "nsecs": 0}, "id": "g1"}, "goal": {"order": 3}})
    assert goals == [{"order": 3}]

    server.set_succeeded({"sequence": [0, 1, 1]})
    result_publish = ros.published[-1]
    assert result_publish[0] == "/test_action/result"
    assert result_publish[2]["status"]["status"] == GoalStatus.SUCCEEDED
    assert result_publish[2]["result"] == {"sequence": [0, 1, 1]}
