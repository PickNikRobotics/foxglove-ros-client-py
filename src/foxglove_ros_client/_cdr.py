from __future__ import annotations

from dataclasses import is_dataclass
from typing import Any

import numpy as np
from rosbags.typesys import Stores, get_types_from_msg, get_typestore

NUMPY_DTYPES = {
    "bool": np.bool_,
    "byte": np.int8,
    "char": np.uint8,
    "int8": np.int8,
    "uint8": np.uint8,
    "int16": np.int16,
    "uint16": np.uint16,
    "int32": np.int32,
    "uint32": np.uint32,
    "int64": np.int64,
    "uint64": np.uint64,
    "float32": np.float32,
    "float64": np.float64,
}


class CdrCodec:
    def __init__(self) -> None:
        self._stores: dict[tuple[str, str], Any] = {}

    def register(self, typename: str, schema: str) -> tuple[Any, str]:
        wire_typename = to_rosbags_typename(typename)
        key = (wire_typename, schema)
        store = self._stores.get(key)
        if store is not None:
            return store, wire_typename

        store = get_typestore(Stores.EMPTY)
        store.register(get_types_from_msg(schema, wire_typename))
        self._stores[key] = store
        return store, wire_typename

    def serialize(self, typename: str, schema: str, value: dict[str, Any]) -> bytes:
        store, wire_typename = self.register(typename, schema)
        message = self._to_ros_message(store, wire_typename, value)
        return bytes(store.serialize_cdr(message, wire_typename))

    def deserialize(self, typename: str, schema: str, data: bytes | memoryview) -> dict[str, Any]:
        store, wire_typename = self.register(typename, schema)
        message = store.deserialize_cdr(data, wire_typename)
        return self._from_ros_message(message)

    def _to_ros_message(self, store: Any, typename: str, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        cls = store.types[typename]
        fields = store.fielddefs[typename][1]
        kwargs = {}
        for field_name, field_type in fields:
            if field_name not in value:
                kwargs[field_name] = default_value(field_type)
                continue
            kwargs[field_name] = self._convert_value(store, field_type, value[field_name])
        return cls(**kwargs)

    def _convert_value(self, store: Any, field_type: Any, value: Any) -> Any:
        kind = field_type[0].name
        descriptor = field_type[1]

        if kind == "NAME":
            return self._to_ros_message(store, descriptor, value)

        if kind == "ARRAY" or kind == "SEQUENCE":
            item_type = descriptor[0]
            values = list(value or [])
            item_kind = item_type[0].name
            item_descriptor = item_type[1]
            if item_kind == "BASE":
                base_name = item_descriptor[0]
                dtype = NUMPY_DTYPES.get(base_name)
                if dtype is not None and base_name != "bool":
                    return np.asarray(values, dtype=dtype)
                return values
            if item_kind == "NAME":
                return [self._to_ros_message(store, item_descriptor, item) for item in values]
            return values

        if kind == "BASE":
            base_name = descriptor[0]
            if base_name in ("string", "wstring"):
                return "" if value is None else str(value)
            if isinstance(value, dict):
                if "sec" in value and "nanosec" in value:
                    return value
                if "secs" in value and "nsecs" in value:
                    return {"sec": value["secs"], "nanosec": value["nsecs"]}
            return value

        return value

    def _from_ros_message(self, value: Any) -> Any:
        if isinstance(value, np.ndarray):
            return value.tolist()
        if is_dataclass(value):
            result = {}
            for name in value.__dataclass_fields__:
                if name == "__msgtype__":
                    continue
                result[name] = self._from_ros_message(getattr(value, name))
            return result
        if isinstance(value, list):
            return [self._from_ros_message(item) for item in value]
        return value


def to_rosbags_typename(typename: str) -> str:
    normalized = normalize_ros_type(typename)
    if "/srv/" in normalized and normalized.endswith(("_Request", "_Response")):
        package, rest = normalized.split("/srv/", 1)
        return f"{package}/msg/{rest}"
    return normalized


def normalize_ros_type(typename: str, kind: str = "msg") -> str:
    trimmed = typename.lstrip("/")
    if "/msg/" in trimmed or "/srv/" in trimmed or "/action/" in trimmed:
        return trimmed
    parts = trimmed.split("/", 1)
    if len(parts) != 2:
        return trimmed
    return f"{parts[0]}/{kind}/{parts[1]}"


def default_value(field_type: Any) -> Any:
    kind = field_type[0].name
    descriptor = field_type[1]
    if kind in ("ARRAY", "SEQUENCE"):
        item_type = descriptor[0]
        item_kind = item_type[0].name
        if item_kind == "BASE" and item_type[1][0] in NUMPY_DTYPES:
            return np.asarray([], dtype=NUMPY_DTYPES[item_type[1][0]])
        return []
    if kind == "NAME":
        return None
    if kind == "BASE":
        base_name = descriptor[0]
        if base_name in ("string", "wstring"):
            return ""
        if base_name == "bool":
            return False
        return 0
    return None
