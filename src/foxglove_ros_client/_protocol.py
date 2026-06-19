from __future__ import annotations

import json
import logging
import struct
import threading
from dataclasses import dataclass
from typing import Any, Callable

import websocket

LOGGER = logging.getLogger("foxglove_ros_client")

OP_MESSAGE_DATA = 0x01
OP_SERVICE_CALL_RESPONSE = 0x03
OP_CLIENT_MESSAGE_DATA = 0x01
OP_CLIENT_SERVICE_CALL_REQUEST = 0x02


@dataclass
class FoxgloveChannel:
    id: int
    topic: str
    encoding: str
    schema_name: str
    schema: str


@dataclass
class FoxgloveService:
    id: int
    name: str
    type: str
    request_schema: str
    response_schema: str


class FoxgloveProtocolClient:
    def __init__(self, url: str, headers: dict[str, str] | None = None):
        self.url = _to_ws_url(url)
        self.headers = headers or {}
        self.ws: websocket.WebSocketApp | None = None
        self.thread: threading.Thread | None = None
        self.connected = threading.Event()
        self.closed = threading.Event()
        self._send_lock = threading.Lock()
        self._next_subscription_id = 1
        self._next_call_id = 1
        self._next_client_channel_id = 1
        self._handlers: dict[str, list[Callable[..., None]]] = {
            "open": [],
            "close": [],
            "error": [],
            "advertise": [],
            "unadvertise": [],
            "advertiseServices": [],
            "unadvertiseServices": [],
            "message": [],
            "serviceResponse": [],
            "serviceCallFailure": [],
            "parameterValues": [],
        }

    @property
    def is_connected(self) -> bool:
        return self.connected.is_set() and not self.closed.is_set()

    def on(self, event: str, callback: Callable[..., None]) -> None:
        self._handlers.setdefault(event, []).append(callback)

    def connect(self, forever: bool = False) -> None:
        if self.thread and self.thread.is_alive():
            return

        header = [f"{key}: {value}" for key, value in self.headers.items()]
        self.closed.clear()
        self.ws = websocket.WebSocketApp(
            self.url,
            header=header,
            subprotocols=["foxglove.sdk.v1", "foxglove.websocket.v1"],
            on_open=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
            on_message=self._on_message,
        )

        if forever:
            self.ws.run_forever()
            return

        self.thread = threading.Thread(target=self.ws.run_forever, name="foxglove-ros-client-py", daemon=True)
        self.thread.start()

    def close(self) -> None:
        if self.ws:
            self.ws.close()
        self.connected.clear()
        self.closed.set()

    def subscribe(self, channel_id: int) -> int:
        subscription_id = self._next_subscription_id
        self._next_subscription_id += 1
        self.send_json({"op": "subscribe", "subscriptions": [{"id": subscription_id, "channelId": channel_id}]})
        return subscription_id

    def unsubscribe(self, subscription_id: int) -> None:
        self.send_json({"op": "unsubscribe", "subscriptionIds": [subscription_id]})

    def advertise_client_channel(self, topic: str, encoding: str, schema_name: str) -> int:
        channel_id = self._next_client_channel_id
        self._next_client_channel_id += 1
        self.send_json({"op": "advertise", "channels": [{"id": channel_id, "topic": topic, "encoding": encoding, "schemaName": schema_name}]})
        return channel_id

    def unadvertise_client_channel(self, channel_id: int) -> None:
        self.send_json({"op": "unadvertise", "channelIds": [channel_id]})

    def publish_message(self, channel_id: int, data: bytes) -> None:
        self.send_binary(struct.pack("<BI", OP_CLIENT_MESSAGE_DATA, channel_id) + data)

    def call_service(self, service_id: int, encoding: str, request_data: bytes) -> int:
        call_id = self._next_call_id
        self._next_call_id += 1
        encoding_bytes = encoding.encode("utf8")
        frame = (
            struct.pack("<BIII", OP_CLIENT_SERVICE_CALL_REQUEST, service_id, call_id, len(encoding_bytes))
            + encoding_bytes
            + request_data
        )
        self.send_binary(frame)
        return call_id

    def get_parameters(self, names: list[str], request_id: str) -> None:
        self.send_json({"op": "getParameters", "parameterNames": names, "id": request_id})

    def set_parameters(self, parameters: list[dict[str, Any]], request_id: str) -> None:
        self.send_json({"op": "setParameters", "parameters": parameters, "id": request_id})

    def send_json(self, message: dict[str, Any]) -> None:
        self._send(json.dumps({k: v for k, v in message.items() if v is not None}))

    def send_binary(self, message: bytes) -> None:
        self._send(message, websocket.ABNF.OPCODE_BINARY)

    def _send(self, message: str | bytes, opcode: int = websocket.ABNF.OPCODE_TEXT) -> None:
        ws = self.ws
        if not ws or not self.is_connected:
            return
        with self._send_lock:
            ws.send(message, opcode=opcode)

    def _emit(self, event: str, *args: Any) -> None:
        for callback in list(self._handlers.get(event, ())):
            try:
                callback(*args)
            except Exception:
                LOGGER.exception("Error in protocol %s handler", event)

    def _on_open(self, _ws: websocket.WebSocketApp) -> None:
        self.connected.set()
        self.closed.clear()
        self._emit("open")

    def _on_close(self, _ws: websocket.WebSocketApp, _status: int | None, _message: str | None) -> None:
        self.connected.clear()
        self.closed.set()
        self._emit("close")

    def _on_error(self, _ws: websocket.WebSocketApp, error: Exception) -> None:
        self._emit("error", error)

    def _on_message(self, _ws: websocket.WebSocketApp, message: str | bytes) -> None:
        if isinstance(message, str):
            self._handle_text(message)
        else:
            self._handle_binary(bytes(message))

    def _handle_text(self, text: str) -> None:
        try:
            message = json.loads(text)
        except ValueError:
            LOGGER.warning("Ignoring invalid Foxglove JSON message: %s", text[:100])
            return

        op = message.get("op")
        if op == "advertise":
            self._emit("advertise", [_read_channel(ch) for ch in message.get("channels", [])])
        elif op == "unadvertise":
            self._emit("unadvertise", list(message.get("channelIds", [])))
        elif op == "advertiseServices":
            self._emit("advertiseServices", [_read_service(svc) for svc in message.get("services", [])])
        elif op == "unadvertiseServices":
            self._emit("unadvertiseServices", list(message.get("serviceIds", [])))
        elif op == "parameterValues":
            self._emit("parameterValues", message.get("id", ""), message.get("parameters", []))
        elif op == "serviceCallFailure":
            self._emit("serviceCallFailure", message)

    def _handle_binary(self, payload: bytes) -> None:
        if not payload:
            return
        opcode = payload[0]
        if opcode == OP_MESSAGE_DATA:
            if len(payload) < 13:
                return
            subscription_id, timestamp = struct.unpack_from("<IQ", payload, 1)
            self._emit("message", subscription_id, timestamp, payload[13:])
        elif opcode == OP_SERVICE_CALL_RESPONSE:
            if len(payload) < 13:
                return
            service_id, call_id, encoding_len = struct.unpack_from("<III", payload, 1)
            start = 13
            end = start + encoding_len
            if len(payload) < end:
                return
            encoding = payload[start:end].decode("utf8")
            self._emit("serviceResponse", service_id, call_id, encoding, payload[end:])


def _to_ws_url(url: str) -> str:
    if url.startswith("http://"):
        return "ws://" + url[7:]
    if url.startswith("https://"):
        return "wss://" + url[8:]
    return url


def _read_channel(value: dict[str, Any]) -> FoxgloveChannel:
    return FoxgloveChannel(
        id=int(value["id"]),
        topic=value.get("topic", ""),
        encoding=value.get("encoding", "cdr"),
        schema_name=value.get("schemaName", ""),
        schema=value.get("schema", ""),
    )


def _read_service(value: dict[str, Any]) -> FoxgloveService:
    request = value.get("request") or {}
    response = value.get("response") or {}
    return FoxgloveService(
        id=int(value["id"]),
        name=value.get("name", ""),
        type=value.get("type", ""),
        request_schema=request.get("schema") or value.get("requestSchema", ""),
        response_schema=response.get("schema") or value.get("responseSchema", ""),
    )
