from __future__ import annotations

import json
import threading
import time
from collections import UserDict
from enum import Enum
from typing import Any, Callable


class RosTimeoutError(TimeoutError):
    pass


class Message(UserDict):
    def __init__(self, values: dict[str, Any] | None = None):
        self.data: dict[str, Any] = {}
        if values is not None:
            self.update(values)


class Header(UserDict):
    def __init__(self, seq: int | None = None, stamp: dict[str, Any] | None = None, frame_id: str | None = None):
        self.data: dict[str, Any] = {}
        self.data["seq"] = seq
        self.data["stamp"] = Time(stamp["secs"], stamp["nsecs"]) if stamp else None
        self.data["frame_id"] = frame_id


class Time(UserDict):
    def __init__(self, secs: int, nsecs: int):
        self.data: dict[str, int] = {}
        self.data["secs"] = self._ensure_int(secs)
        self.data["nsecs"] = self._ensure_int(nsecs)

    def _ensure_int(self, value: int | float) -> int:
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        raise ValueError("argument must be an integer")

    @property
    def secs(self) -> int:
        return self.data["secs"]

    @property
    def nsecs(self) -> int:
        return self.data["nsecs"]

    def is_zero(self) -> bool:
        return self.secs == 0 and self.nsecs == 0

    def to_nsec(self) -> int:
        return self.secs * int(1e9) + self.nsecs

    def to_sec(self) -> float:
        return float(self.secs) + float(self.nsecs) / int(1e9)

    @staticmethod
    def from_sec(float_secs: float) -> "Time":
        secs = int(float_secs)
        nsecs = int((float_secs - secs) * int(1e9))
        return Time(secs, nsecs)

    @staticmethod
    def now() -> "Time":
        return Time.from_sec(time.time())


class ServiceRequest(UserDict):
    def __init__(self, values: dict[str, Any] | None = None):
        self.data: dict[str, Any] = {}
        if values is not None:
            self.update(values)


class ServiceResponse(UserDict):
    def __init__(self, values: dict[str, Any] | None = None):
        self.data: dict[str, Any] = {}
        if values is not None:
            self.update(values)


class Result(UserDict):
    def __init__(self, values: dict[str, Any] | None = None):
        self.data: dict[str, Any] = {}
        if values is not None:
            self.update(values)


class Feedback(UserDict):
    def __init__(self, values: dict[str, Any] | None = None):
        self.data: dict[str, Any] = {}
        if values is not None:
            self.update(values)


class GoalStatus(Enum):
    UNKNOWN = 0
    ACCEPTED = 1
    EXECUTING = 2
    CANCELING = 3
    SUCCEEDED = 4
    CANCELED = 5
    ABORTED = 6


class Goal(UserDict):
    def __init__(self, values: dict[str, Any] | None = None):
        self.data: dict[str, Any] = {}
        if values is not None:
            self.update(values)


class MessageEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, (Header, Time)):
            return dict(o)
        return super().default(o)


class Topic:
    SUPPORTED_COMPRESSION_TYPES = ("png", "none")

    def __init__(
        self,
        ros: Any,
        name: str,
        message_type: str,
        compression: str | None = None,
        latch: bool = False,
        throttle_rate: int = 0,
        queue_size: int = 100,
        queue_length: int = 0,
        reconnect_on_close: bool = True,
    ):
        self.ros = ros
        self.name = name
        self.message_type = message_type
        self.compression = compression or "none"
        self.latch = latch
        self.throttle_rate = throttle_rate
        self.queue_size = queue_size
        self.queue_length = queue_length
        self.reconnect_on_close = reconnect_on_close
        self._subscribe_id: str | None = None
        self._advertise_id: str | None = None

        if self.compression not in self.SUPPORTED_COMPRESSION_TYPES:
            raise ValueError("Unsupported compression type. Must be one of: " + str(self.SUPPORTED_COMPRESSION_TYPES))

    @property
    def is_advertised(self) -> bool:
        return self._advertise_id is not None

    @property
    def is_subscribed(self) -> bool:
        return self._subscribe_id is not None

    def subscribe(self, callback: Callable[[Message], None]) -> None:
        if self._subscribe_id:
            return
        self._subscribe_id = "subscribe:%s:%d" % (self.name, self.ros.id_counter)
        self.ros.subscribe_topic(self.name, self.message_type, callback, self.throttle_rate)

    def unsubscribe(self) -> None:
        if not self._subscribe_id:
            return
        self.ros.unsubscribe_topic(self.name)
        self._subscribe_id = None

    def advertise(self) -> None:
        if self.is_advertised:
            return
        self._advertise_id = "advertise:%s:%d" % (self.name, self.ros.id_counter)

    def unadvertise(self) -> None:
        if not self.is_advertised:
            return
        self.ros.unpublish_topic(self.name)
        self._advertise_id = None

    def publish(self, message: Message | dict[str, Any]) -> None:
        if not self.is_advertised:
            self.advertise()
        self.ros.publish_topic(self.name, self.message_type, dict(message))


class ServiceException(Exception):
    pass


class Service:
    def __init__(self, ros: Any, name: str, service_type: str, reconnect_on_close: bool = True):
        self.ros = ros
        self.name = name
        self.service_type = service_type
        self.reconnect_on_close = reconnect_on_close
        self._is_advertised = False
        self._service_callback: Callable[..., Any] | None = None

    @property
    def is_advertised(self) -> bool:
        return self._is_advertised

    def call(
        self,
        request: ServiceRequest | dict[str, Any],
        callback: Callable[[ServiceResponse], None] | None = None,
        errback: Callable[[Any], None] | None = None,
        timeout: float | None = None,
    ) -> ServiceResponse | None:
        if self.is_advertised:
            return None

        if callback:
            self.ros.call_service_async(self.name, self.service_type, dict(request), callback, errback)
            return None

        result = self.ros.call_service_sync(self.name, self.service_type, dict(request), timeout)
        if isinstance(result, dict) and "exception" in result:
            raise ServiceException(result["exception"])
        return ServiceResponse(result)

    def advertise(self, callback: Callable[..., Any]) -> None:
        raise NotImplementedError("foxglove_bridge does not support client-side service advertisement.")

    def unadvertise(self) -> None:
        raise NotImplementedError("foxglove_bridge does not support client-side service advertisement.")


class ActionClient:
    def __init__(self, ros: Any, name: str, action_type: str, reconnect_on_close: bool = True):
        self.ros = ros
        self.name = name
        self.action_type = action_type
        self.reconnect_on_close = reconnect_on_close
        self.wait_results: dict[str, threading.Event] = {}

    def send_goal(
        self,
        goal: Goal | dict[str, Any],
        resultback: Callable[[Result], None],
        feedback: Callable[[Feedback], None] | None,
        errback: Callable[[Any], None] | None,
    ) -> str:
        goal_id = "send_action_goal:%s:%d" % (self.name, self.ros.id_counter)
        self.wait_results[goal_id] = threading.Event()

        def _resultback(result: Result) -> None:
            wait_result_event = self.wait_results.get(goal_id)
            if wait_result_event is not None:
                wait_result_event.set()
            resultback(result)

        def _errback(error: Any) -> None:
            wait_result_event = self.wait_results.get(goal_id)
            if wait_result_event is not None:
                wait_result_event.set()
            if errback:
                errback(error)

        self.ros.send_action_goal(self.name, self.action_type, goal_id, dict(goal), _resultback, feedback, _errback)
        return goal_id

    def cancel_goal(self, goal_id: str) -> None:
        self.ros.cancel_action_goal(self.name, goal_id)

    def wait_goal(self, goal_id: str, timeout: float | None = None) -> None:
        wait_result_event = self.wait_results.get(goal_id)
        if wait_result_event is None:
            raise ValueError("Unknown goal ID")
        if not wait_result_event.wait(timeout):
            raise RosTimeoutError("Goal failed to receive result")
        self.wait_results.pop(goal_id, None)


class Param:
    def __init__(self, ros: Any, name: str):
        self.ros = ros
        self.name = name

    def get(
        self,
        callback: Callable[[Any], None] | None = None,
        errback: Callable[[Any], None] | None = None,
        timeout: float | None = None,
    ) -> Any:
        if callback:
            self.ros.get_param_async(self.name, callback, errback)
            return None
        return self.ros.get_param_sync(self.name, timeout)

    def set(
        self,
        value: Any,
        callback: Callable[[Any], None] | None = None,
        errback: Callable[[Any], None] | None = None,
        timeout: float | None = None,
    ) -> Any:
        if callback:
            self.ros.set_param_async(self.name, value, callback, errback)
            return None
        return self.ros.set_param_sync(self.name, value, timeout)

    def delete(
        self,
        callback: Callable[[Any], None] | None = None,
        errback: Callable[[Any], None] | None = None,
        timeout: float | None = None,
    ) -> Any:
        raise NotImplementedError("foxglove_bridge parameter operations do not expose deleteParameters.")


__all__ = [
    "ActionClient",
    "Feedback",
    "Goal",
    "GoalStatus",
    "Header",
    "Message",
    "MessageEncoder",
    "Param",
    "Result",
    "RosTimeoutError",
    "Service",
    "ServiceException",
    "ServiceRequest",
    "ServiceResponse",
    "Time",
    "Topic",
]
