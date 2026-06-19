from __future__ import annotations

import math
from collections import deque
from typing import Any, Callable

from .core import Topic

__all__ = ["TFClient"]


class TFClient:
    def __init__(
        self,
        ros,
        fixed_frame="/base_link",
        angular_threshold=2.0,
        translation_threshold=0.01,
        rate=10.0,
        update_delay=50,
        topic_timeout=2000.0,
        server_name="/tf2_web_republisher",
        repub_service_name="/republish_tfs",
    ):
        self.ros = ros
        self.fixed_frame = fixed_frame
        self.rate = rate
        self.frame_info: dict[str, dict[str, Any]] = {}
        self._edges: dict[tuple[str, str], dict[str, Any]] = {}
        self._tf = Topic(ros, "/tf", "tf2_msgs/msg/TFMessage", throttle_rate=int(1000 / rate) if rate else 0)
        self._tf_static = Topic(ros, "/tf_static", "tf2_msgs/msg/TFMessage")
        self._started = False

    def subscribe(self, frame_id: str, callback: Callable[[dict[str, Any]], None]) -> None:
        frame_id = self._normalize_frame_id(frame_id)
        frame = self.frame_info.setdefault(frame_id, {"cbs": []})
        frame["cbs"].append(callback)
        if not self._started:
            self._tf.subscribe(self._process_tf_message)
            self._tf_static.subscribe(self._process_tf_message)
            self._started = True
        resolved = self._resolve_transform(self._normalize_frame_id(self.fixed_frame), frame_id)
        if resolved is not None:
            frame["transform"] = resolved
            callback(resolved)

    def unsubscribe(self, frame_id: str, callback: Callable[[dict[str, Any]], None] | None = None) -> None:
        frame_id = self._normalize_frame_id(frame_id)
        frame = self.frame_info.get(frame_id)
        if not frame:
            return
        if callback is None:
            self.frame_info.pop(frame_id, None)
            return
        frame["cbs"] = [cb for cb in frame.get("cbs", []) if cb != callback]
        if not frame["cbs"]:
            self.frame_info.pop(frame_id, None)

    def dispose(self) -> None:
        if self._started:
            self._tf.unsubscribe()
            self._tf_static.unsubscribe()
        self._started = False

    def _process_tf_message(self, message: dict[str, Any]) -> None:
        for transform in message.get("transforms", []):
            parent = self._normalize_frame_id(transform.get("header", {}).get("frame_id", ""))
            child = self._normalize_frame_id(transform.get("child_frame_id", ""))
            if not parent or not child:
                continue
            value = _normalize_transform(transform.get("transform", {}))
            self._edges[(parent, child)] = value
        self._emit_resolved_frames()

    def _emit_resolved_frames(self) -> None:
        fixed_frame = self._normalize_frame_id(self.fixed_frame)
        for frame_id, frame in list(self.frame_info.items()):
            value = self._resolve_transform(fixed_frame, frame_id)
            if value is None:
                continue
            frame["transform"] = value
            for callback in list(frame.get("cbs", [])):
                callback(value)

    def _resolve_transform(self, source: str, target: str) -> dict[str, Any] | None:
        if source == target:
            return _identity_transform()

        adjacency: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for (parent, child), transform in self._edges.items():
            adjacency.setdefault(parent, []).append((child, transform))
            adjacency.setdefault(child, []).append((parent, _invert_transform(transform)))

        queue = deque([(source, _identity_transform())])
        visited = {source}
        while queue:
            frame, current = queue.popleft()
            for next_frame, edge in adjacency.get(frame, []):
                if next_frame in visited:
                    continue
                composed = _compose_transform(current, edge)
                if next_frame == target:
                    return composed
                visited.add(next_frame)
                queue.append((next_frame, composed))
        return None

    def _normalize_frame_id(self, frame_id: str) -> str:
        return frame_id[1:] if frame_id.startswith("/") else frame_id


def _identity_transform() -> dict[str, Any]:
    return {
        "translation": {"x": 0.0, "y": 0.0, "z": 0.0},
        "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
    }


def _normalize_transform(transform: dict[str, Any]) -> dict[str, Any]:
    translation = transform.get("translation", {})
    rotation = _normalize_quaternion(transform.get("rotation", {}))
    return {
        "translation": {
            "x": float(translation.get("x", 0.0)),
            "y": float(translation.get("y", 0.0)),
            "z": float(translation.get("z", 0.0)),
        },
        "rotation": {"x": rotation[0], "y": rotation[1], "z": rotation[2], "w": rotation[3]},
    }


def _compose_transform(first: dict[str, Any], second: dict[str, Any]) -> dict[str, Any]:
    first_t = _vector(first["translation"])
    first_q = _quat(first["rotation"])
    second_t = _vector(second["translation"])
    second_q = _quat(second["rotation"])

    rotated = _rotate_vector(first_q, second_t)
    translation = (first_t[0] + rotated[0], first_t[1] + rotated[1], first_t[2] + rotated[2])
    rotation = _normalize_quaternion_tuple(_quat_multiply(first_q, second_q))
    return _transform_from_tuple(translation, rotation)


def _invert_transform(transform: dict[str, Any]) -> dict[str, Any]:
    translation = _vector(transform["translation"])
    rotation = _quat(transform["rotation"])
    inv_rotation = _quat_inverse(rotation)
    inv_translation = _rotate_vector(inv_rotation, (-translation[0], -translation[1], -translation[2]))
    return _transform_from_tuple(inv_translation, inv_rotation)


def _transform_from_tuple(translation: tuple[float, float, float], rotation: tuple[float, float, float, float]) -> dict[str, Any]:
    return {
        "translation": {"x": translation[0], "y": translation[1], "z": translation[2]},
        "rotation": {"x": rotation[0], "y": rotation[1], "z": rotation[2], "w": rotation[3]},
    }


def _vector(value: dict[str, Any]) -> tuple[float, float, float]:
    return (float(value.get("x", 0.0)), float(value.get("y", 0.0)), float(value.get("z", 0.0)))


def _quat(value: dict[str, Any]) -> tuple[float, float, float, float]:
    return _normalize_quaternion(value)


def _normalize_quaternion(value: dict[str, Any]) -> tuple[float, float, float, float]:
    return _normalize_quaternion_tuple(
        (
            float(value.get("x", 0.0)),
            float(value.get("y", 0.0)),
            float(value.get("z", 0.0)),
            float(value.get("w", 1.0)),
        )
    )


def _normalize_quaternion_tuple(value: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    norm = math.sqrt(sum(part * part for part in value))
    if norm == 0:
        return (0.0, 0.0, 0.0, 1.0)
    return (value[0] / norm, value[1] / norm, value[2] / norm, value[3] / norm)


def _quat_inverse(value: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    return (-value[0], -value[1], -value[2], value[3])


def _quat_multiply(
    left: tuple[float, float, float, float],
    right: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    lx, ly, lz, lw = left
    rx, ry, rz, rw = right
    return (
        lw * rx + lx * rw + ly * rz - lz * ry,
        lw * ry - lx * rz + ly * rw + lz * rx,
        lw * rz + lx * ry - ly * rx + lz * rw,
        lw * rw - lx * rx - ly * ry - lz * rz,
    )


def _rotate_vector(
    rotation: tuple[float, float, float, float],
    vector: tuple[float, float, float],
) -> tuple[float, float, float]:
    q_vector = (vector[0], vector[1], vector[2], 0.0)
    rotated = _quat_multiply(_quat_multiply(rotation, q_vector), _quat_inverse(rotation))
    return (rotated[0], rotated[1], rotated[2])
