from __future__ import annotations

import argparse
import json
import threading

import foxglove_ros_client as foxy_ros


def main() -> None:
    parser = argparse.ArgumentParser(description="Exercise foxglove_ros_client against a live foxglove_bridge.")
    parser.add_argument("--url", default="ws://10.211.55.3:3201")
    parser.add_argument("--topic", default="/joint_states")
    parser.add_argument("--topic-type", default="sensor_msgs/msg/JointState")
    parser.add_argument("--service-name", default="")
    parser.add_argument("--service-type", default="")
    parser.add_argument("--service-request-json", default="{}")
    parser.add_argument("--action-name", default="")
    parser.add_argument("--action-type", default="")
    parser.add_argument("--action-goal-json", default="{}")
    parser.add_argument("--action-timeout", type=float, default=30.0)
    parser.add_argument(
        "--allow-action-execution",
        action="store_true",
        help="Actually send an action goal. Omit this for safe smoke tests that cannot move a robot or affect an action server.",
    )
    parser.add_argument(
        "--action-mode",
        choices=("none", "send-goal"),
        default="none",
        help="Action execution path to test. The default only discovers action endpoints.",
    )
    parser.add_argument("--skip-action", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.skip_action and args.action_mode == "send-goal":
        args.action_mode = "none"
    if args.action_mode != "none" and not args.allow_action_execution:
        raise SystemExit("Refusing to send an action goal without --allow-action-execution")
    if args.action_mode != "none" and (not args.action_name or not args.action_type):
        raise SystemExit("--action-name and --action-type are required when sending an action goal")

    ros = foxy_ros.Ros(args.url)
    ros.run(timeout=5)
    try:
        print(f"connected topics={len(ros.get_topics())} services={len(ros.get_services())}")

        _check_topic(ros, args.topic, args.topic_type)
        _check_publish(args.url)
        _check_native_params(ros)
        _check_services(ros, args.service_name, args.service_type, json.loads(args.service_request_json))
        _check_action_discovery(ros)
        if args.action_mode == "send-goal":
            goal = json.loads(args.action_goal_json)
            _check_action_goal(ros, args.action_name, args.action_type, goal, args.action_timeout)
    finally:
        ros.close()


def _check_topic(ros: foxy_ros.Ros, topic_name: str, topic_type: str) -> None:
    event = threading.Event()
    received = []
    topic = foxy_ros.Topic(ros, topic_name, topic_type)
    topic.subscribe(lambda message: (received.append(message), event.set()))
    if not event.wait(5):
        raise RuntimeError(f"Timed out waiting for {topic_name}")
    topic.unsubscribe()
    print(f"topic {topic_name} ok keys={sorted(received[-1].keys())[:8]}")


def _check_publish(url: str) -> None:
    topic_name = "/foxglove_ros_client/live_string"
    subscriber = foxy_ros.Ros(url)
    subscriber.run(timeout=5)
    publisher = foxy_ros.Ros(url)
    publisher.run(timeout=5)
    try:
        event = threading.Event()
        received = []
        listener = foxy_ros.Topic(subscriber, topic_name, "std_msgs/msg/String")
        listener.subscribe(lambda message: (received.append(message), event.set()))
        talker = foxy_ros.Topic(publisher, topic_name, "std_msgs/msg/String")
        for i in range(20):
            talker.publish(foxy_ros.Message({"data": f"hello {i}"}))
            if event.wait(0.1):
                break
        if not received:
            raise RuntimeError(f"Timed out waiting for published {topic_name}")
        listener.unsubscribe()
        talker.unadvertise()
        print(f"publish {topic_name} ok data={received[-1]['data']!r}")
    finally:
        subscriber.close()
        publisher.close()


def _check_native_params(ros: foxy_ros.Ros) -> None:
    port = foxy_ros.Param(ros, "/foxglove_bridge:port").get(timeout=5)
    debug = foxy_ros.Param(ros, "foxglove_bridge.debug").get(timeout=5)
    print(f"param protocol ok port={port} debug={debug}")


def _check_services(ros: foxy_ros.Ros, service_name: str, service_type: str, request: dict) -> None:
    services = ros.get_services()
    print(f"service discovery ok count={len(services)}")
    if not service_name:
        return
    if not service_type:
        service_type = ros.get_service_type(service_name)
    response = foxy_ros.Service(ros, service_name, service_type).call(foxy_ros.ServiceRequest(request), timeout=5)
    print(f"service call ok name={service_name} keys={sorted(response.keys())[:8]}")


def _check_action_discovery(ros: foxy_ros.Ros) -> None:
    actions = []
    ros.get_action_servers(actions.append)
    print(f"action discovery ok count={len(actions[0])}")


def _check_action_goal(
    ros: foxy_ros.Ros,
    action_name: str,
    action_type: str,
    goal: dict,
    timeout: float,
) -> None:
    event = threading.Event()
    results = []
    errors = []
    client = foxy_ros.ActionClient(ros, action_name, action_type)

    def on_result(result):
        results.append(result)
        event.set()

    def on_error(error):
        errors.append(error)
        event.set()

    goal_id = client.send_goal(foxy_ros.Goal(goal), on_result, None, on_error)
    if not event.wait(timeout):
        client.cancel_goal(goal_id)
        raise RuntimeError(f"Timed out waiting for {action_name} result")
    if errors:
        if "WebSocket closed before response" in str(errors[0]):
            raise RuntimeError(
                f"WebSocket closed before {action_name} returned. The bridge or action server likely exited; "
                "check the ROS logs before sending another goal."
            )
        raise RuntimeError(errors[0])
    result = results[0]
    if result["status"] != foxy_ros.GoalStatus.SUCCEEDED:
        raise RuntimeError(f"{action_name} returned {result}")
    values = result["values"]
    error_code = values.get("error_code", {})
    if error_code and error_code.get("val") != error_code.get("SUCCESS"):
        raise RuntimeError(f"{action_name} failed: {values}")
    print(f"action goal ok action={action_name}")


if __name__ == "__main__":
    main()
