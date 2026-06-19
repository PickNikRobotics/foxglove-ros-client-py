# foxglove-ros-client-py

A Python client for `foxglove_bridge` with a
[`roslibpy`](https://github.com/RobotWebTools/roslibpy)-compatible API surface.
It speaks the Foxglove WebSocket protocol instead of the rosbridge JSON
protocol.

The canonical import package is `foxglove_ros_client`:

```python
from foxglove_ros_client import Ros, Topic, Message, Service, ServiceRequest, Param
```

## Status

This is an early runtime-focused implementation for ROS 2 systems exposed by
`foxglove_bridge`.

Supported:

- `Ros(host, port=None, is_secure=False, headers=None)` with `run()`,
  `run_forever()`, `close()`, `terminate()`, `on()`, `off()`, `on_ready()`,
  `call_later()`, and `is_connected`.
- `Topic(...).subscribe()`, `unsubscribe()`, `publish()`, `advertise()`,
  `unadvertise()`.
- `Service(...).call()` with blocking and callback forms.
- `Param(...).get()` and `set()` through Foxglove parameter operations.
- Rosapi-style discovery helpers backed by Foxglove advertisements:
  `get_topics()`, `get_topic_type()`, `get_topics_for_type()`,
  `get_services()`, `get_service_type()`, `get_services_for_type()`,
  `get_message_details()`, `get_service_request_details()`,
  `get_service_response_details()`, and action-server discovery.
- ROS 2 `ActionClient.send_goal()`, `cancel_goal()`, and `wait_goal()` when the
  bridge advertises hidden action endpoints.
- ROS 1-style `foxglove_ros_client.ros1.actionlib.ActionClient`, `Goal`, and
  `SimpleActionServer` over action topics.
- `TFClient` over `/tf` and `/tf_static`, including fixed-frame resolution
  across parent/child transform chains.
- Dict-like `Message`, `ServiceRequest`, `ServiceResponse`, `Goal`, `Result`,
  `Feedback`, `Time`, `Header`, and `foxglove_ros_client.ros2.Header`.
- A `foxglove-ros-client-py` command-line entry point with roslibpy-style
  `topic`, `msg`, `service`, `srv`, and `param` subcommands.

Not supported:

- Client-side service advertisement. `foxglove_bridge` does not expose a
  WebSocket protocol for Python clients to serve ROS services.
- Full ROS graph node introspection. `get_nodes()` and `get_node_details()`
  currently return empty local best-effort data because Foxglove advertisements
  do not include node ownership.
- Parameter deletion and global parameter listing. `get_params()` reports
  parameters seen through this client session; the Foxglove protocol does not
  expose a `deleteParameters` or `listParameters` operation.

## Install From Source

```bash
git clone https://github.com/noah-wardlow/foxglove-ros-client-py.git
python -m pip install ./foxglove-ros-client-py
```

## Connect

Start the ROS 2 Foxglove bridge:

```bash
ros2 launch foxglove_bridge foxglove_bridge_launch.xml port:=8765
```

For ROS 2 actions, start the bridge with hidden topics and services included:

```bash
ros2 launch foxglove_bridge foxglove_bridge_launch.xml port:=8765 include_hidden:=true
```

Foxglove exposes ROS 2 actions through the hidden `/_action/*` services and
topics. This differs from rosbridge/actionlib topic wiring; `ActionClient` uses
Foxglove service calls to `send_goal`, `get_result`, and `cancel_goal`.

Use the familiar API shape:

```python
import foxglove_ros_client as foxy_ros

ros = foxy_ros.Ros("localhost", 8765)
ros.run()

listener = foxy_ros.Topic(ros, "/chatter", "std_msgs/msg/String")
listener.subscribe(lambda msg: print(msg["data"]))

talker = foxy_ros.Topic(ros, "/python/chatter", "std_msgs/msg/String")
talker.publish(foxy_ros.Message({"data": "hello from Python"}))
```

`std_msgs/String` and `std_msgs/msg/String` are both accepted. Subscription and
service responses are decoded from CDR using schemas advertised by the bridge.
Publishing uses CDR when a matching schema has been advertised, otherwise it
falls back to Foxglove's JSON client-channel encoding.

## Development

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e ".[test]"
pytest
python -m build
```

The command-line helper can be run after installation:

```bash
foxglove-ros-client-py -r localhost -p 8765 topic list
```

## Live Check

The live smoke test exercises topic subscription, publishing, parameter reads,
service discovery, and action discovery without sending action goals:

```bash
python examples/live_bridge_check.py --url ws://localhost:8765
```

Service calls are optional and generic:

```bash
python examples/live_bridge_check.py \
  --url ws://localhost:8765 \
  --service-name /example_service \
  --service-type example_interfaces/srv/Trigger \
  --service-request-json '{}'
```

Action execution is intentionally double-gated:

```bash
python examples/live_bridge_check.py \
  --url ws://localhost:8765 \
  --action-mode send-goal \
  --action-name /example_action \
  --action-type example_interfaces/action/Fibonacci \
  --action-goal-json '{"order": 5}' \
  --allow-action-execution
```
