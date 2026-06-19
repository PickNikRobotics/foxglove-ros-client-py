from __future__ import annotations

import math
import random
import threading
import time
from typing import Any, Callable

from foxglove_ros_client import Message, Topic
from foxglove_ros_client.core import RosTimeoutError
from foxglove_ros_client.event_emitter import EventEmitterMixin

__all__ = ["Goal", "GoalStatus", "ActionClient", "SimpleActionServer"]


def _is_earlier(t1: dict[str, Any], t2: dict[str, Any]) -> bool:
    if t1["secs"] > t2["secs"]:
        return False
    if t1["secs"] < t2["secs"]:
        return True
    return t1["nsecs"] < t2["nsecs"]


class GoalStatus:
    PENDING = 0
    ACTIVE = 1
    PREEMPTED = 2
    SUCCEEDED = 3
    ABORTED = 4
    REJECTED = 5
    PREEMPTING = 6
    RECALLING = 7
    RECALLED = 8
    LOST = 9


class Goal(EventEmitterMixin):
    def __init__(self, action_client: "ActionClient", goal_message: Message | dict[str, Any]):
        super().__init__()
        self.action_client = action_client
        self.goal_id = "goal_%s_%d" % (random.random(), time.time() * 1000)
        self.wait_result = threading.Event()
        self.result = None
        self.status = None
        self.feedback = None
        self.goal_message = Message(
            {"goal_id": {"stamp": {"secs": 0, "nsecs": 0}, "id": self.goal_id}, "goal": dict(goal_message)}
        )
        self.action_client.add_goal(self)
        self.on("status", self._set_status)
        self.on("result", self._set_result)
        self.on("feedback", self._set_feedback)

    @property
    def is_active(self) -> bool:
        return self.status is not None and self.status["status"] in (GoalStatus.ACTIVE, GoalStatus.PENDING)

    @property
    def is_finished(self) -> bool:
        return self.result is not None and not self.is_active

    def send(self, result_callback: Callable[[Any], None] | None = None, timeout: float | None = None) -> None:
        if result_callback:
            self.on("result", result_callback)
        self.status = {"status": GoalStatus.PENDING}
        self.action_client.goal_topic.publish(self.goal_message)
        if timeout:
            self.action_client.ros.call_later(timeout, self._trigger_timeout)

    def cancel(self) -> None:
        self.action_client.cancel_topic.publish(Message({"id": self.goal_id}))

    def wait(self, timeout: float | None = None) -> Any:
        if not self.wait_result.wait(timeout):
            raise RosTimeoutError("Goal failed to receive result")
        return self.result

    def _trigger_timeout(self) -> None:
        if not self.is_finished:
            self.emit("timeout")

    def _set_status(self, status: dict[str, Any]) -> None:
        self.status = status
        if self.is_finished:
            self.wait_result.set()

    def _set_result(self, result: Any) -> None:
        self.result = result
        if self.is_finished:
            self.wait_result.set()

    def _set_feedback(self, feedback: Any) -> None:
        self.feedback = feedback


class ActionClient(EventEmitterMixin):
    def __init__(
        self,
        ros,
        server_name: str,
        action_name: str,
        timeout: float | None = None,
        omit_feedback: bool = False,
        omit_status: bool = False,
        omit_result: bool = False,
    ):
        super().__init__()
        self.ros = ros
        self.server_name = server_name
        self.action_name = action_name
        self.omit_feedback = omit_feedback
        self.omit_status = omit_status
        self.omit_result = omit_result
        self.goals: dict[str, Goal] = {}
        self.wait_status = threading.Event()

        self.feedback_listener = Topic(ros, server_name + "/feedback", action_name + "Feedback")
        self.status_listener = Topic(ros, server_name + "/status", "actionlib_msgs/GoalStatusArray")
        self.result_listener = Topic(ros, server_name + "/result", action_name + "Result")
        self.goal_topic = Topic(ros, server_name + "/goal", action_name + "Goal")
        self.cancel_topic = Topic(ros, server_name + "/cancel", "actionlib_msgs/GoalID")
        self.goal_topic.advertise()
        self.cancel_topic.advertise()

        if not omit_status:
            self.status_listener.subscribe(self._on_status_message)
        if not omit_feedback:
            self.feedback_listener.subscribe(self._on_feedback_message)
        if not omit_result:
            self.result_listener.subscribe(self._on_result_message)

    def add_goal(self, goal: Goal) -> None:
        self.goals[goal.goal_id] = goal

    def cancel(self) -> None:
        self.cancel_topic.publish(Message())

    def dispose(self) -> None:
        self.goal_topic.unadvertise()
        self.cancel_topic.unadvertise()
        if not self.omit_status:
            self.status_listener.unsubscribe()
        if not self.omit_feedback:
            self.feedback_listener.unsubscribe()
        if not self.omit_result:
            self.result_listener.unsubscribe()

    def _on_status_message(self, message: dict[str, Any]) -> None:
        self.wait_status.set()
        for status in message.get("status_list", []):
            goal_id = status.get("goal_id", {}).get("id")
            goal = self.goals.get(goal_id)
            if goal:
                goal.emit("status", status)

    def _on_feedback_message(self, message: dict[str, Any]) -> None:
        goal_id = message.get("status", {}).get("goal_id", {}).get("id")
        goal = self.goals.get(goal_id)
        if goal:
            goal.emit("feedback", message.get("feedback"))

    def _on_result_message(self, message: dict[str, Any]) -> None:
        goal_id = message.get("status", {}).get("goal_id", {}).get("id")
        goal = self.goals.get(goal_id)
        if goal:
            goal.emit("status", message.get("status"))
            goal.emit("result", message.get("result"))


class SimpleActionServer(EventEmitterMixin):
    STATUS_PUBLISH_INTERVAL = 0.5

    def __init__(self, ros, server_name: str, action_name: str):
        super().__init__()
        self.ros = ros
        self.server_name = server_name
        self.action_name = action_name
        self._lock = threading.Lock()
        self.feedback_publisher = Topic(ros, server_name + "/feedback", action_name + "Feedback")
        self.status_publisher = Topic(ros, server_name + "/status", "actionlib_msgs/GoalStatusArray")
        self.result_publisher = Topic(ros, server_name + "/result", action_name + "Result")
        self.goal_listener = Topic(ros, server_name + "/goal", action_name + "Goal")
        self.cancel_listener = Topic(ros, server_name + "/cancel", "actionlib_msgs/GoalID")
        self.feedback_publisher.advertise()
        self.status_publisher.advertise()
        self.result_publisher.advertise()
        self.status_message = Message(dict(header=dict(stamp=dict(secs=0, nsecs=100), frame_id=""), status_list=[]))
        self.current_goal = None
        self.next_goal = None
        self.preempt_request = False
        self._disposed = False
        self.goal_listener.subscribe(self._on_goal_message)
        self.cancel_listener.subscribe(self._on_cancel_message)
        self.ros.call_later(self.STATUS_PUBLISH_INTERVAL, self._periodic_publish_status)

    def start(self, action_callback: Callable[[Any], None]) -> None:
        self.on("goal", lambda goal: self.ros.call_in_thread(lambda: action_callback(goal)))
        self.on("cancel", self._set_preempt_requested)

    def dispose(self) -> None:
        self._disposed = True
        self.feedback_publisher.unadvertise()
        self.status_publisher.unadvertise()
        self.result_publisher.unadvertise()
        self.goal_listener.unsubscribe()
        self.cancel_listener.unsubscribe()

    def is_preempt_requested(self) -> bool:
        with self._lock:
            return self.preempt_request

    def set_succeeded(self, result: dict[str, Any]) -> None:
        next_goal = None
        with self._lock:
            if not self.current_goal:
                return
            status = dict(goal_id=self.current_goal["goal_id"], status=GoalStatus.SUCCEEDED)
            self.status_message["status_list"] = [status]
            self._publish_status()
            self.status_message["status_list"] = []
            self.result_publisher.publish(Message({"status": status, "result": result}))
            next_goal = self._advance_goal_locked()
        if next_goal:
            self.emit("goal", next_goal["goal"])

    def send_feedback(self, feedback: dict[str, Any]) -> None:
        with self._lock:
            if not self.current_goal:
                return
            status = {"goal_id": self.current_goal["goal_id"], "status": GoalStatus.ACTIVE}
        self.feedback_publisher.publish(Message({"status": status, "feedback": feedback}))

    def set_preempted(self) -> None:
        next_goal = None
        with self._lock:
            if not self.current_goal:
                return
            status = dict(goal_id=self.current_goal["goal_id"], status=GoalStatus.PREEMPTED)
            self.status_message["status_list"] = [status]
            self._publish_status()
            self.status_message["status_list"] = []
            self.result_publisher.publish(Message({"status": status, "result": {}}))
            next_goal = self._advance_goal_locked()
        if next_goal:
            self.emit("goal", next_goal["goal"])

    def _set_preempt_requested(self) -> None:
        with self._lock:
            self.preempt_request = True

    def _publish_status(self) -> None:
        current_time = time.time()
        secs = int(math.floor(current_time))
        nsecs = int(round(1e9 * (current_time - secs)))
        self.status_message["header"]["stamp"]["secs"] = secs
        self.status_message["header"]["stamp"]["nsecs"] = nsecs
        self.status_publisher.publish(self.status_message)

    def _periodic_publish_status(self) -> None:
        if self._disposed:
            return
        with self._lock:
            self._publish_status()
        self.ros.call_later(self.STATUS_PUBLISH_INTERVAL, self._periodic_publish_status)

    def _on_goal_message(self, message: dict[str, Any]) -> None:
        will_cancel = False
        will_emit_goal = None
        with self._lock:
            if self.current_goal:
                self.next_goal = message
                will_cancel = True
            else:
                self.status_message["status_list"] = [dict(goal_id=message["goal_id"], status=GoalStatus.ACTIVE)]
                self.current_goal = message
                will_emit_goal = message["goal"]
        if will_cancel:
            self.emit("cancel")
        if will_emit_goal is not None:
            self.emit("goal", will_emit_goal)

    def _on_cancel_message(self, message: dict[str, Any]) -> None:
        will_cancel = False
        with self._lock:
            message_id = message.get("id", "")
            message_stamp = message.get("stamp", {"secs": 0, "nsecs": 0})
            secs = message_stamp.get("secs", 0)
            if secs == 0 and message_stamp.get("nsecs", 0) == 0 and message_id == "":
                self.next_goal = None
                will_cancel = self.current_goal is not None
            else:
                if self.current_goal and message_id == self.current_goal["goal_id"]["id"]:
                    will_cancel = True
                elif self.next_goal and message_id == self.next_goal["goal_id"]["id"]:
                    self.next_goal = None
                if self.next_goal and _is_earlier(self.next_goal["goal_id"]["stamp"], message_stamp):
                    self.next_goal = None
                if self.current_goal and _is_earlier(self.current_goal["goal_id"]["stamp"], message_stamp):
                    will_cancel = True
        if will_cancel:
            self.emit("cancel")

    def _advance_goal_locked(self) -> dict[str, Any] | None:
        if self.next_goal:
            self.current_goal = self.next_goal
            self.next_goal = None
        else:
            self.current_goal = None
        self.preempt_request = False
        return self.current_goal
