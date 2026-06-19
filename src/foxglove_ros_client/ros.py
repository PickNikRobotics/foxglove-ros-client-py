from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Callable

from ._cdr import CdrCodec, normalize_ros_type
from ._protocol import FoxgloveChannel, FoxgloveProtocolClient, FoxgloveService
from .core import Feedback, Message, Result, RosTimeoutError, ServiceResponse
from .event_emitter import EventEmitterMixin

LOGGER = logging.getLogger("foxglove_ros_client")

CONNECTION_TIMEOUT = 10
ROSAPI_TIMEOUT = 3


def set_rosapi_timeout(timeout: int) -> None:
    global ROSAPI_TIMEOUT
    ROSAPI_TIMEOUT = timeout


class Ros(EventEmitterMixin):
    def __init__(self, host: str, port: int | None = None, is_secure: bool = False, headers: dict[str, str] | None = None):
        super().__init__()
        self._id_counter = 0
        self.url = _create_url(host, port, is_secure)
        self.protocol = FoxgloveProtocolClient(self.url, headers=headers)
        self.codec = CdrCodec()

        self.channels: dict[int, FoxgloveChannel] = {}
        self.channels_by_topic: dict[str, FoxgloveChannel] = {}
        self.services: dict[int, FoxgloveService] = {}
        self.services_by_name: dict[str, FoxgloveService] = {}

        self._subscriptions: dict[int, dict[str, Any]] = {}
        self._subscriptions_by_topic: dict[str, int] = {}
        self._pending_subscribers: list[dict[str, Any]] = []
        self._client_channels: dict[str, dict[str, Any]] = {}
        self._pending_services: dict[int, dict[str, Any]] = {}
        self._pending_params: dict[str, dict[str, Any]] = {}
        self._known_params: dict[str, Any] = {}
        self._next_param_request_id = 1
        self._lock = threading.RLock()
        self._advertisements_ready = threading.Event()
        self._channels_ready = threading.Event()
        self._services_ready = threading.Event()

        self._wire_protocol()
        self.connect()

    @property
    def id_counter(self) -> int:
        self._id_counter += 1
        return self._id_counter

    @property
    def is_connected(self) -> bool:
        return self.protocol.is_connected

    def connect(self) -> None:
        if not self.is_connected:
            self.protocol.connect()

    def run(self, timeout: float = CONNECTION_TIMEOUT) -> None:
        self.connect()
        if not self.protocol.connected.wait(timeout):
            raise RosTimeoutError("Failed to connect to ROS")
        # foxglove_bridge sends topic/service advertisements immediately after
        # the WebSocket opens. Waiting here keeps roslibpy-style discovery calls
        # useful directly after run() returns.
        deadline = time.monotonic() + timeout
        self._channels_ready.wait(timeout)
        remaining = max(0.0, deadline - time.monotonic())
        self._services_ready.wait(remaining)

    def wait_for_advertisements(self, timeout: float | None = CONNECTION_TIMEOUT) -> bool:
        return self._advertisements_ready.wait(timeout)

    def run_forever(self) -> None:
        if self.protocol.thread and self.protocol.thread.is_alive():
            self.protocol.thread.join()
            return
        self.protocol.connect(forever=True)

    def run_event_loop(self) -> None:
        LOGGER.warning("Deprecation warning: use run_forever instead of run_event_loop")
        self.run_forever()

    def close(self, timeout: float = CONNECTION_TIMEOUT) -> None:
        if not self.is_connected:
            return
        self.emit("closing")
        self.protocol.close()
        if not self.protocol.closed.wait(timeout):
            raise RosTimeoutError("Failed to disconnect from ROS")

    def terminate(self) -> None:
        self.close()

    def on_ready(self, callback: Callable[[], None], run_in_thread: bool = True) -> None:
        def _run() -> None:
            if run_in_thread:
                self.call_in_thread(callback)
            else:
                callback()

        if self.is_connected:
            _run()
            return
        self.protocol.on("open", _run)

    def send_on_ready(self, message: Message | dict[str, Any]) -> None:
        raise NotImplementedError("send_on_ready is rosbridge-specific; use Topic, Service, or Param APIs.")

    def set_status_level(self, level: str, identifier: str) -> None:
        # rosbridge status filtering has no Foxglove WebSocket equivalent.
        return None

    def get_time(self, callback: Callable[[Any], None] | None = None, errback: Callable[[Any], None] | None = None) -> Any:
        from .core import Time

        value = Time.now()
        return _callback_or_return(value, callback)

    def get_topics(self, callback: Callable[[list[str]], None] | None = None, errback: Callable[[Any], None] | None = None) -> Any:
        topics = sorted(channel.topic for channel in self.channels.values())
        return _callback_or_return(topics, callback)

    def get_topic_type(
        self,
        topic: str,
        callback: Callable[[str], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        channel = self.channels_by_topic.get(topic)
        value = channel.schema_name if channel else ""
        return _callback_or_return(value, callback)

    def call_in_thread(self, callback: Callable[[], None]) -> threading.Thread:
        thread = threading.Thread(target=callback, daemon=True)
        thread.start()
        return thread

    def call_later(self, delay: float, callback: Callable[[], None]) -> threading.Timer:
        timer = threading.Timer(delay, callback)
        timer.daemon = True
        timer.start()
        return timer

    def blocking_call_from_thread(self, callback: Callable[[dict[str, Any]], Any], timeout: float | None) -> Any:
        placeholder: dict[str, Any] = {}
        event = threading.Event()

        def _run() -> None:
            try:
                callback(placeholder)
            finally:
                event.set()

        self.call_in_thread(_run)
        if not event.wait(timeout):
            raise RosTimeoutError("Timed out waiting for callback")
        if "exception" in placeholder:
            raise placeholder["exception"]
        return placeholder.get("result")

    def subscribe_topic(
        self,
        topic: str,
        message_type: str,
        callback: Callable[[Message], None],
        throttle_rate: int = 0,
    ) -> None:
        with self._lock:
            existing_id = self._subscriptions_by_topic.get(topic)
            if existing_id is not None:
                self._subscriptions[existing_id]["callbacks"].append(callback)
                return

            channel = self.channels_by_topic.get(topic)
            if channel:
                self._create_subscription(channel, message_type, callback, throttle_rate)
            else:
                self._pending_subscribers.append(
                    {"topic": topic, "message_type": message_type, "callback": callback, "throttle_rate": throttle_rate}
                )

    def unsubscribe_topic(self, topic: str, callback: Callable[[Message], None] | None = None) -> None:
        with self._lock:
            self._pending_subscribers = [
                item for item in self._pending_subscribers if item["topic"] != topic or (callback and item["callback"] != callback)
            ]
            subscription_id = self._subscriptions_by_topic.get(topic)
            if subscription_id is None:
                return
            subscription = self._subscriptions[subscription_id]
            if callback:
                subscription["callbacks"] = [cb for cb in subscription["callbacks"] if cb != callback]
                if subscription["callbacks"]:
                    return
            self.protocol.unsubscribe(subscription_id)
            self._subscriptions.pop(subscription_id, None)
            self._subscriptions_by_topic.pop(topic, None)

    def publish_topic(self, topic: str, message_type: str, message: dict[str, Any]) -> None:
        with self._lock:
            client_channel = self._client_channels.get(topic)
            if not client_channel:
                schema_name = normalize_ros_type(message_type, "msg")
                schema = self._find_schema(schema_name)
                encoding = "cdr" if schema else "json"
                channel_id = self.protocol.advertise_client_channel(topic, encoding, schema_name)
                client_channel = {"id": channel_id, "schema_name": schema_name, "schema": schema, "encoding": encoding}
                self._client_channels[topic] = client_channel

        if client_channel["encoding"] == "cdr":
            data = self.codec.serialize(client_channel["schema_name"], client_channel["schema"], message)
        else:
            import json

            data = json.dumps(message).encode("utf8")
        self.protocol.publish_message(client_channel["id"], data)

    def unpublish_topic(self, topic: str) -> None:
        with self._lock:
            client_channel = self._client_channels.pop(topic, None)
        if client_channel:
            self.protocol.unadvertise_client_channel(client_channel["id"])

    def get_topics_for_type(
        self,
        message_type: str,
        callback: Callable[[list[str]], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        canonical = normalize_ros_type(message_type, "msg")
        topics = [channel.topic for channel in self.channels.values() if normalize_ros_type(channel.schema_name, "msg") == canonical]
        return _callback_or_return(sorted(topics), callback)

    def get_services(
        self,
        callback: Callable[[list[str]], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        services = sorted(service.name for service in self.services.values())
        return _callback_or_return(services, callback)

    def get_service_type(
        self,
        service_name: str,
        callback: Callable[[str], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        service = self.services_by_name.get(service_name)
        value = service.type if service else ""
        return _callback_or_return(value, callback)

    def get_services_for_type(
        self,
        service_type: str,
        callback: Callable[[list[str]], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        canonical = normalize_ros_type(service_type, "srv")
        services = [service.name for service in self.services.values() if normalize_ros_type(service.type, "srv") == canonical]
        return _callback_or_return(sorted(services), callback)

    def get_service_request_details(
        self,
        type: str,
        callback: Callable[[dict[str, Any]], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        canonical = normalize_ros_type(type, "srv")
        service = self._find_service_by_type(canonical)
        result = _schema_details(canonical + "_Request", service.request_schema if service else "")
        return _callback_or_return(result, callback)

    def get_service_response_details(
        self,
        type: str,
        callback: Callable[[dict[str, Any]], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        canonical = normalize_ros_type(type, "srv")
        service = self._find_service_by_type(canonical)
        result = _schema_details(canonical + "_Response", service.response_schema if service else "")
        return _callback_or_return(result, callback)

    def get_message_details(
        self,
        message_type: str,
        callback: Callable[[dict[str, Any]], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        canonical = normalize_ros_type(message_type, "msg")
        schema = self._find_schema(canonical)
        result = _schema_details(canonical, schema)
        return _callback_or_return(result, callback)

    def get_params(
        self,
        callback: Callable[[list[str]], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        params = sorted(self._known_params.keys())
        return _callback_or_return(params, callback)

    def get_param(
        self,
        name: str,
        callback: Callable[[Any], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        return self.get_param_async(name, callback, errback) if callback else self.get_param_sync(name, ROSAPI_TIMEOUT)

    def set_param(
        self,
        name: str,
        value: Any,
        callback: Callable[[Any], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        return self.set_param_async(name, value, callback, errback) if callback else self.set_param_sync(name, value, ROSAPI_TIMEOUT)

    def delete_param(
        self,
        name: str,
        callback: Callable[[Any], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        error = NotImplementedError("foxglove_bridge parameter operations do not expose deleteParameters.")
        if errback:
            errback(error)
            return None
        raise error

    def get_action_servers(
        self,
        callback: Callable[[list[str]], None],
        errback: Callable[[Any], None] | None = None,
    ) -> None:
        action_names = {
            service.name[: -len("/_action/send_goal")]
            for service in self.services.values()
            if service.name.endswith("/_action/send_goal")
        }
        callback(sorted(action_names))

    def get_nodes(
        self,
        callback: Callable[[list[str]], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        return _callback_or_return([], callback)

    def get_node_details(
        self,
        node: str,
        callback: Callable[[dict[str, list[str]]], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> Any:
        return _callback_or_return({"services": [], "subscribing": [], "publishing": []}, callback)

    def authenticate(self, mac: str, client: str, dest: str, rand: str, t: float, level: str, end: float) -> None:
        # Foxglove WebSocket authentication is handled during the WebSocket
        # handshake by the deployment, not through a rosbridge auth operation.
        return None

    def call_service_async(
        self,
        service_name: str,
        service_type: str,
        request: dict[str, Any],
        callback: Callable[[ServiceResponse], None] | None,
        errback: Callable[[Any], None] | None = None,
    ) -> None:
        try:
            call_id = self._send_service_request(service_name, service_type, request, callback, errback)
            if call_id is None and errback:
                errback("service unavailable")
        except Exception as exc:
            if errback:
                errback(exc)
            else:
                raise

    def call_service_sync(
        self,
        service_name: str,
        service_type: str,
        request: dict[str, Any],
        timeout: float | None = None,
    ) -> dict[str, Any]:
        event = threading.Event()
        result: dict[str, Any] = {}

        def _callback(response: ServiceResponse) -> None:
            result["response"] = dict(response)
            event.set()

        def _errback(error: Any) -> None:
            result["exception"] = error
            event.set()

        self.call_service_async(service_name, service_type, request, _callback, _errback)
        if not event.wait(timeout if timeout is not None else ROSAPI_TIMEOUT):
            raise RosTimeoutError("Timed out waiting for service response")
        if "exception" in result:
            raise RuntimeError(result["exception"])
        return result.get("response", {})

    def get_param_async(self, name: str, callback: Callable[[Any], None], errback: Callable[[Any], None] | None = None) -> None:
        request_id = "param_get_%d" % self._next_param_request_id
        self._next_param_request_id += 1
        self._pending_params[request_id] = {"callback": callback, "errback": errback, "name": _to_foxglove_param_name(name)}
        self.protocol.get_parameters([_to_foxglove_param_name(name)], request_id)

    def get_param_sync(self, name: str, timeout: float | None = None) -> Any:
        event = threading.Event()
        result: dict[str, Any] = {}
        self.get_param_async(name, lambda value: (result.setdefault("value", value), event.set()), lambda error: (result.setdefault("error", error), event.set()))
        if not event.wait(timeout if timeout is not None else ROSAPI_TIMEOUT):
            raise RosTimeoutError("Timed out waiting for parameter response")
        if "error" in result:
            raise RuntimeError(result["error"])
        return result.get("value")

    def set_param_async(
        self,
        name: str,
        value: Any,
        callback: Callable[[Any], None] | None = None,
        errback: Callable[[Any], None] | None = None,
    ) -> None:
        request_id = "param_set_%d" % self._next_param_request_id
        self._next_param_request_id += 1
        wire_name = _to_foxglove_param_name(name)
        self._known_params[wire_name] = value
        self._pending_params[request_id] = {"callback": callback, "errback": errback, "name": wire_name, "set": True}
        self.protocol.set_parameters([{"name": wire_name, "value": value}], request_id)

    def set_param_sync(self, name: str, value: Any, timeout: float | None = None) -> Any:
        event = threading.Event()
        result: dict[str, Any] = {}
        self.set_param_async(name, value, lambda _value=None: (result.setdefault("value", _value), event.set()), lambda error: (result.setdefault("error", error), event.set()))
        if not event.wait(timeout if timeout is not None else ROSAPI_TIMEOUT):
            raise RosTimeoutError("Timed out waiting for parameter response")
        if "error" in result:
            raise RuntimeError(result["error"])
        return result.get("value")

    def send_action_goal(
        self,
        action_name: str,
        action_type: str,
        goal_id: str,
        goal: dict[str, Any],
        resultback: Callable[[Result], None],
        feedback: Callable[[Feedback], None] | None,
        errback: Callable[[Any], None] | None,
    ) -> None:
        action_type = normalize_ros_type(action_type, "action")
        uuid_bytes = _goal_uuid(goal_id)
        missing_services = self._missing_action_services(action_name)
        if missing_services:
            error = RuntimeError(
                "Action %s is not fully advertised by foxglove_bridge; missing %s. "
                "ROS 2 action endpoints are hidden services/topics in Foxglove, so launch "
                "foxglove_bridge with include_hidden:=true when using ActionClient."
                % (action_name, ", ".join(missing_services))
            )
            if errback:
                errback(error)
                return
            raise error

        if feedback:
            feedback_topic = _action_endpoint(action_name, "feedback")
            self.subscribe_topic(
                feedback_topic,
                action_type + "_FeedbackMessage",
                lambda msg: feedback(Feedback(msg.get("feedback", msg))),
            )

        send_goal_service = _action_endpoint(action_name, "send_goal")
        send_goal_request = self._action_send_goal_request(send_goal_service, uuid_bytes, goal)

        def _accepted(response: ServiceResponse) -> None:
            if response.get("accepted") is False:
                if errback:
                    errback({"accepted": False, "values": dict(response)})
                return

            self.call_service_async(
                _action_endpoint(action_name, "get_result"),
                action_type + "_GetResult",
                {"goal_id": {"uuid": uuid_bytes}},
                lambda result_response: resultback(_action_result(result_response)),
                errback,
            )

        self.call_service_async(
            send_goal_service,
            action_type + "_SendGoal",
            send_goal_request,
            _accepted,
            errback,
        )

    def _action_send_goal_request(self, service_name: str, uuid_bytes: list[int], goal: dict[str, Any]) -> dict[str, Any]:
        service = self.services_by_name.get(service_name)
        if not service:
            return {"goal_id": {"uuid": uuid_bytes}, "goal": goal}
        field_names = _schema_field_names(service.request_schema)
        if "goal" in field_names:
            return {"goal_id": {"uuid": uuid_bytes}, "goal": goal}
        request = {"goal_id": {"uuid": uuid_bytes}}
        request.update(goal)
        return request

    def _missing_action_services(self, action_name: str) -> list[str]:
        return [
            service_name
            for service_name in (
                _action_endpoint(action_name, "send_goal"),
                _action_endpoint(action_name, "get_result"),
            )
            if service_name not in self.services_by_name
        ]

    def cancel_action_goal(self, action_name: str, goal_id: str) -> None:
        self.call_service_async(
            _action_endpoint(action_name, "cancel_goal"),
            "action_msgs/srv/CancelGoal",
            {"goal_info": {"goal_id": {"uuid": _goal_uuid(goal_id)}, "stamp": {"sec": 0, "nanosec": 0}}},
            None,
            None,
        )

    def _wire_protocol(self) -> None:
        self.protocol.on("open", lambda: self.emit("connection"))
        self.protocol.on("close", self._on_close)
        self.protocol.on("error", lambda error: self.emit("error", error))
        self.protocol.on("advertise", self._on_advertise)
        self.protocol.on("unadvertise", self._on_unadvertise)
        self.protocol.on("advertiseServices", self._on_advertise_services)
        self.protocol.on("unadvertiseServices", self._on_unadvertise_services)
        self.protocol.on("message", self._on_message)
        self.protocol.on("serviceResponse", self._on_service_response)
        self.protocol.on("serviceCallFailure", self._on_service_failure)
        self.protocol.on("parameterValues", self._on_parameter_values)

    def _create_subscription(
        self,
        channel: FoxgloveChannel,
        message_type: str,
        callback: Callable[[Message], None],
        throttle_rate: int = 0,
    ) -> None:
        schema_name = channel.schema_name or normalize_ros_type(message_type, "msg")
        subscription_id = self.protocol.subscribe(channel.id)
        self.codec.register(schema_name, channel.schema)
        self._subscriptions[subscription_id] = {
            "topic": channel.topic,
            "schema_name": schema_name,
            "schema": channel.schema,
            "callbacks": [callback],
            "last_fired_at": 0.0,
            "throttle_rate": throttle_rate,
        }
        self._subscriptions_by_topic[channel.topic] = subscription_id

    def _send_service_request(
        self,
        service_name: str,
        service_type: str,
        request: dict[str, Any],
        callback: Callable[[ServiceResponse], None] | None,
        errback: Callable[[Any], None] | None,
    ) -> int | None:
        svc = self.services_by_name.get(service_name)
        if not svc:
            raise RuntimeError("Service %s not available" % service_name)

        canonical_type = normalize_ros_type(svc.type or service_type, "srv")
        request_schema = svc.request_schema
        response_schema = svc.response_schema
        if request and not request_schema:
            raise RuntimeError("Service %s did not advertise a request schema" % service_name)

        request_data = self.codec.serialize(canonical_type + "_Request", request_schema, request)
        call_id = self.protocol.call_service(svc.id, "cdr", request_data)
        self._pending_services[call_id] = {
            "service": service_name,
            "response_type": canonical_type + "_Response",
            "response_schema": response_schema,
            "callback": callback,
            "errback": errback,
        }
        return call_id

    def _find_schema(self, schema_name: str) -> str:
        canonical = normalize_ros_type(schema_name, "msg")
        for channel in self.channels.values():
            if normalize_ros_type(channel.schema_name, "msg") == canonical:
                return channel.schema
        return ""

    def _find_service_by_type(self, service_type: str) -> FoxgloveService | None:
        canonical = normalize_ros_type(service_type, "srv")
        for service in self.services.values():
            if normalize_ros_type(service.type, "srv") == canonical:
                return service
        return None

    def _on_close(self) -> None:
        self.channels.clear()
        self.channels_by_topic.clear()
        self.services.clear()
        self.services_by_name.clear()
        self._advertisements_ready.clear()
        self._channels_ready.clear()
        self._services_ready.clear()
        self._subscriptions.clear()
        self._subscriptions_by_topic.clear()
        self._client_channels.clear()
        for pending in self._pending_services.values():
            errback = pending.get("errback")
            if errback:
                errback("WebSocket closed before response")
        self._pending_services.clear()
        self.emit("close")

    def _on_advertise(self, channels: list[FoxgloveChannel]) -> None:
        with self._lock:
            for channel in channels:
                self.channels[channel.id] = channel
                self.channels_by_topic[channel.topic] = channel
            pending = list(self._pending_subscribers)
            self._pending_subscribers.clear()
            for item in pending:
                channel = self.channels_by_topic.get(item["topic"])
                if channel:
                    self._create_subscription(channel, item["message_type"], item["callback"], item["throttle_rate"])
                else:
                    self._pending_subscribers.append(item)
            if self.channels or self.services:
                self._advertisements_ready.set()
            if self.channels:
                self._channels_ready.set()
        self.emit("channelsChanged")

    def _on_unadvertise(self, channel_ids: list[int]) -> None:
        with self._lock:
            for channel_id in channel_ids:
                channel = self.channels.pop(channel_id, None)
                if channel:
                    self.channels_by_topic.pop(channel.topic, None)
        self.emit("channelsChanged")

    def _on_advertise_services(self, services: list[FoxgloveService]) -> None:
        with self._lock:
            for service in services:
                self.services[service.id] = service
                self.services_by_name[service.name] = service
            if self.channels or self.services:
                self._advertisements_ready.set()
            if self.services:
                self._services_ready.set()

    def _on_unadvertise_services(self, service_ids: list[int]) -> None:
        with self._lock:
            for service_id in service_ids:
                service = self.services.pop(service_id, None)
                if service:
                    self.services_by_name.pop(service.name, None)

    def _on_message(self, subscription_id: int, _timestamp: int, data: bytes) -> None:
        subscription = self._subscriptions.get(subscription_id)
        if not subscription:
            return
        decoded = Message(self.codec.deserialize(subscription["schema_name"], subscription["schema"], data))
        self.emit(subscription["topic"], decoded)
        throttle_rate = subscription.get("throttle_rate", 0) or 0
        if throttle_rate > 0:
            now = time.monotonic() * 1000
            if now - subscription["last_fired_at"] < throttle_rate:
                return
            subscription["last_fired_at"] = now
        for callback in list(subscription["callbacks"]):
            callback(decoded)

    def _on_service_response(self, _service_id: int, call_id: int, _encoding: str, data: bytes) -> None:
        pending = self._pending_services.pop(call_id, None)
        if not pending:
            return
        try:
            if pending["response_schema"]:
                decoded = self.codec.deserialize(pending["response_type"], pending["response_schema"], data)
            else:
                decoded = {}
            callback = pending.get("callback")
            if callback:
                callback(ServiceResponse(decoded))
        except Exception as exc:
            errback = pending.get("errback")
            if errback:
                errback(exc)
            else:
                raise

    def _on_service_failure(self, failure: dict[str, Any]) -> None:
        pending = self._pending_services.pop(int(failure.get("callId", -1)), None)
        if pending and pending.get("errback"):
            pending["errback"](failure.get("message", "service call failed"))

    def _on_parameter_values(self, request_id: str, parameters: list[dict[str, Any]]) -> None:
        pending = self._pending_params.pop(request_id, None)
        if not pending:
            return
        callback = pending.get("callback")
        if not callback:
            return
        if pending.get("set"):
            callback(None)
            return
        name = pending["name"]
        match = next((param for param in parameters if _to_foxglove_param_name(param.get("name", "")) == name), None)
        value = None if match is None else match.get("value")
        if match is not None:
            self._known_params[name] = value
        callback(value)


def _create_url(host: str, port: int | None, is_secure: bool) -> str:
    if host.startswith(("ws://", "wss://", "http://", "https://")):
        return host
    scheme = "wss" if is_secure else "ws"
    if port is None:
        raise ValueError("Port must be set when host is not a websocket URL")
    return f"{scheme}://{host}:{port}"


def _to_foxglove_param_name(name: str) -> str:
    return name.replace(":", ".").lstrip("/")


def _action_endpoint(action_name: str, endpoint: str) -> str:
    return action_name.rstrip("/") + "/_action/" + endpoint


def _goal_uuid(goal_id: str) -> list[int]:
    return list(uuid.uuid5(uuid.NAMESPACE_URL, goal_id).bytes)


def _action_result(response: ServiceResponse) -> Result:
    status_value = response.get("status", 0)
    try:
        from .core import GoalStatus

        status = GoalStatus(status_value)
    except Exception:
        status = status_value
    return Result({"status": status, "values": response.get("result", dict(response))})


def _callback_or_return(value: Any, callback: Callable[[Any], None] | None) -> Any:
    if callback:
        callback(value)
        return None
    return value


def _schema_details(typename: str, schema: str) -> dict[str, Any]:
    if not schema:
        return {"typedefs": [{"type": typename, "fieldnames": [], "fieldtypes": [], "fieldarraylen": []}]}

    sections = _split_schema_sections(typename, schema)
    return {"typedefs": [_typedef_from_schema(type_name, text) for type_name, text in sections]}


def _schema_field_names(schema: str) -> list[str]:
    if not schema:
        return []
    root_lines: list[str] = []
    for line in schema.splitlines():
        stripped = line.strip()
        if stripped.startswith("MSG: "):
            break
        if stripped.startswith("="):
            continue
        root_lines.append(line)
    return _typedef_from_schema("", "\n".join(root_lines))["fieldnames"]


def _split_schema_sections(typename: str, schema: str) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    current_type = typename
    current_lines: list[str] = []
    for line in schema.splitlines():
        stripped = line.strip()
        if stripped.startswith("MSG: "):
            sections.append((current_type, "\n".join(current_lines)))
            current_type = stripped[5:].strip()
            current_lines = []
            continue
        if stripped.startswith("="):
            continue
        current_lines.append(line)
    sections.append((current_type, "\n".join(current_lines)))
    return [(type_name, text) for type_name, text in sections if type_name]


def _typedef_from_schema(typename: str, schema: str) -> dict[str, Any]:
    fieldnames: list[str] = []
    fieldtypes: list[str] = []
    fieldarraylen: list[int] = []
    for raw_line in schema.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or "=" in line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        field_type, field_name = parts[0], parts[1]
        base_type, array_len = _split_array_type(field_type)
        fieldnames.append(field_name)
        fieldtypes.append(base_type)
        fieldarraylen.append(array_len)
    return {"type": typename, "fieldnames": fieldnames, "fieldtypes": fieldtypes, "fieldarraylen": fieldarraylen}


def _split_array_type(field_type: str) -> tuple[str, int]:
    if not field_type.endswith("]"):
        return field_type, -1
    start = field_type.rfind("[")
    if start == -1:
        return field_type, -1
    base_type = field_type[:start]
    length_text = field_type[start + 1 : -1]
    if length_text == "":
        return base_type, 0
    try:
        return base_type, int(length_text)
    except ValueError:
        return base_type, -1


__all__ = ["Ros", "set_rosapi_timeout"]
