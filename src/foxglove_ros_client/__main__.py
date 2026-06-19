from __future__ import annotations

import argparse
import json
from typing import Any

from . import Ros


def rostopic_list(ros: Ros, **_kwargs: Any) -> None:
    for topic in ros.get_topics():
        print(topic)


def rostopic_type(ros: Ros, topic: str, **_kwargs: Any) -> None:
    print(ros.get_topic_type(topic))


def rostopic_find(ros: Ros, type: str, **_kwargs: Any) -> None:
    for topic in ros.get_topics_for_type(type):
        print(topic)


def rosmsg_info(ros: Ros, type: str, **_kwargs: Any) -> None:
    _print_type(ros.get_message_details(type))


def rosservice_list(ros: Ros, **_kwargs: Any) -> None:
    for service in ros.get_services():
        print(service)


def rosservice_type(ros: Ros, service: str, **_kwargs: Any) -> None:
    print(ros.get_service_type(service))


def rosservice_find(ros: Ros, type: str, **_kwargs: Any) -> None:
    for service in ros.get_services_for_type(type):
        print(service)


def rossrv_info(ros: Ros, type: str, **_kwargs: Any) -> None:
    _print_type(ros.get_service_request_details(type))
    print("---")
    _print_type(ros.get_service_response_details(type))


def rosservice_info(ros: Ros, service: str, **kwargs: Any) -> None:
    type_name = ros.get_service_type(service)
    print("Type: %s\n" % type_name)
    print("Message definition")
    print("------------------")
    rossrv_info(ros, type_name, **kwargs)


def rosaction_list(ros: Ros, **_kwargs: Any) -> None:
    ros.get_action_servers(lambda actions: [print(action) for action in actions])


def rosparam_list(ros: Ros, **_kwargs: Any) -> None:
    for param in ros.get_params():
        print(param)


def rosparam_set(ros: Ros, param: str, value: str, **_kwargs: Any) -> None:
    ros.set_param(param, json.loads(value))


def rosparam_get(ros: Ros, param: str, **_kwargs: Any) -> None:
    print(ros.get_param(param))


def rosparam_delete(ros: Ros, param: str, **_kwargs: Any) -> None:
    ros.delete_param(param)


def _print_typedef(typedef: str, def_map: dict[str, dict[str, Any]], level: int) -> None:
    defs = def_map[typedef]
    for fname, ftype, flen in zip(defs["fieldnames"], defs["fieldtypes"], defs["fieldarraylen"]):
        if flen == -1:
            ftype_info = ftype
        elif flen == 0:
            ftype_info = ftype + "[]"
        else:
            ftype_info = "%s[%d]" % (ftype, flen)
        print("%s%s %s" % ("  " * level, ftype_info, fname))
        if ftype in def_map:
            _print_typedef(ftype, def_map, level + 1)


def _print_type(typedata: dict[str, Any]) -> None:
    if len(typedata["typedefs"]) == 0:
        return
    main_type = typedata["typedefs"][0]["type"]
    def_map = {typedef["type"]: typedef for typedef in typedata["typedefs"]}
    _print_typedef(main_type, def_map, 0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="foxglove_ros_client command-line utility")
    parser.add_argument("-r", "--ros-host", type=str, help="Foxglove bridge host name or websocket URL", default="localhost")
    parser.add_argument("-p", "--ros-port", type=int, help="Foxglove bridge port", default=8765)

    commands = parser.add_subparsers(help="commands")
    commands.dest = "command"
    commands.required = True

    topic_command = commands.add_parser("topic", help="ROS Topics")
    topic_subcommands = topic_command.add_subparsers(help="ROS topic commands")
    topic_subcommands.dest = "subcommand"
    topic_subcommands.required = True
    topic_subcommands.add_parser("list", help="List available ROS topics").set_defaults(func=rostopic_list)
    topic_type_parser = topic_subcommands.add_parser("type", help="ROS topic type")
    topic_type_parser.add_argument("topic")
    topic_type_parser.set_defaults(func=rostopic_type)
    topic_find_parser = topic_subcommands.add_parser("find", help="ROS topics by type")
    topic_find_parser.add_argument("type")
    topic_find_parser.set_defaults(func=rostopic_find)

    msg_command = commands.add_parser("msg", help="ROS Message type information")
    msg_subcommands = msg_command.add_subparsers(help="ROS Message type commands")
    msg_subcommands.dest = "subcommand"
    msg_subcommands.required = True
    msg_info_parser = msg_subcommands.add_parser("info", help="ROS message type information")
    msg_info_parser.add_argument("type")
    msg_info_parser.set_defaults(func=rosmsg_info)

    service_command = commands.add_parser("service", help="ROS Services")
    service_subcommands = service_command.add_subparsers(help="ROS service commands")
    service_subcommands.dest = "subcommand"
    service_subcommands.required = True
    service_subcommands.add_parser("list", help="List available ROS services").set_defaults(func=rosservice_list)
    service_type_parser = service_subcommands.add_parser("type", help="ROS service type")
    service_type_parser.add_argument("service")
    service_type_parser.set_defaults(func=rosservice_type)
    service_find_parser = service_subcommands.add_parser("find", help="ROS services by type")
    service_find_parser.add_argument("type")
    service_find_parser.set_defaults(func=rosservice_find)
    service_info_parser = service_subcommands.add_parser("info", help="ROS service information")
    service_info_parser.add_argument("service")
    service_info_parser.set_defaults(func=rosservice_info)

    srv_command = commands.add_parser("srv", help="ROS Service type information")
    srv_subcommands = srv_command.add_subparsers(help="ROS service type commands")
    srv_subcommands.dest = "subcommand"
    srv_subcommands.required = True
    srv_info_parser = srv_subcommands.add_parser("info", help="ROS service type information")
    srv_info_parser.add_argument("type")
    srv_info_parser.set_defaults(func=rossrv_info)

    action_command = commands.add_parser("action", help="ROS 2 Actions")
    action_subcommands = action_command.add_subparsers(help="ROS action commands")
    action_subcommands.dest = "subcommand"
    action_subcommands.required = True
    action_subcommands.add_parser("list", help="List available ROS actions").set_defaults(func=rosaction_list)

    param_command = commands.add_parser("param", help="ROS Params")
    param_subcommands = param_command.add_subparsers(help="ROS parameter commands")
    param_subcommands.dest = "subcommand"
    param_subcommands.required = True
    param_subcommands.add_parser("list", help="List available ROS parameters").set_defaults(func=rosparam_list)
    param_set_parser = param_subcommands.add_parser("set", help="Set ROS param value")
    param_set_parser.add_argument("param")
    param_set_parser.add_argument("value")
    param_set_parser.set_defaults(func=rosparam_set)
    param_get_parser = param_subcommands.add_parser("get", help="Get ROS param value")
    param_get_parser.add_argument("param")
    param_get_parser.set_defaults(func=rosparam_get)
    param_delete_parser = param_subcommands.add_parser("delete", help="Delete ROS param")
    param_delete_parser.add_argument("param")
    param_delete_parser.set_defaults(func=rosparam_delete)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    ros = Ros(args.ros_host, args.ros_port)
    try:
        ros.run()
        args.func(ros, **vars(args))
    finally:
        ros.terminate()


if __name__ == "__main__":
    main()
