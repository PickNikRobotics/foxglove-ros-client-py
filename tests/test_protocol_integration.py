import json
import socket
import struct
import threading

from websockets.sync.server import serve

from foxglove_ros_client._cdr import CdrCodec
from foxglove_ros_client.ros import Ros


def test_receives_advertised_cdr_topic_over_websocket():
    port = _free_port()
    ready = threading.Event()
    done = threading.Event()
    received_subscribe = {}

    def server_main():
        with serve(handler, "127.0.0.1", port, subprotocols=["foxglove.sdk.v1"]) as server:
            ready.set()
            server.serve_forever()

    def handler(ws):
        ws.send(
            json.dumps(
                {
                    "op": "advertise",
                    "channels": [
                        {
                            "id": 1,
                            "topic": "/chatter",
                            "encoding": "cdr",
                            "schemaName": "std_msgs/msg/String",
                            "schema": "string data",
                        }
                    ],
                }
            )
        )
        subscribe = json.loads(ws.recv())
        received_subscribe.update(subscribe)
        subscription_id = subscribe["subscriptions"][0]["id"]
        codec = CdrCodec()
        payload = codec.serialize("std_msgs/msg/String", "string data", {"data": "from server"})
        ws.send(struct.pack("<BIQ", 0x01, subscription_id, 0) + payload)
        done.wait(2)

    server_thread = threading.Thread(target=server_main, daemon=True)
    server_thread.start()
    assert ready.wait(2)

    ros = Ros("127.0.0.1", port)
    ros.run(timeout=2)
    received = []
    message_event = threading.Event()

    def on_message(message):
        received.append(message)
        message_event.set()

    ros.subscribe_topic("/chatter", "std_msgs/msg/String", on_message)
    assert message_event.wait(2)
    done.set()
    ros.close()

    assert received[0]["data"] == "from server"
    assert received_subscribe["op"] == "subscribe"
    assert received_subscribe["subscriptions"][0]["channelId"] == 1


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
