from __future__ import annotations

import logging
import threading
from collections import defaultdict
from typing import Any, Callable, DefaultDict

LOGGER = logging.getLogger("foxglove_ros_client")


class EventEmitterException(Exception):
    pass


class EventEmitterMixin:
    def __init__(self) -> None:
        self._listeners: DefaultDict[str, list[Callable[..., Any]]] = defaultdict(list)
        self._listeners_lock = threading.RLock()

    def on(self, event_name: str, callback: Callable[..., Any]) -> None:
        with self._listeners_lock:
            if callback not in self._listeners[event_name]:
                self._listeners[event_name].append(callback)

    def off(self, event_name: str, callback: Callable[..., Any] | None = None) -> None:
        with self._listeners_lock:
            if callback is None:
                self._listeners.pop(event_name, None)
                return
            callbacks = self._listeners.get(event_name)
            if not callbacks:
                return
            self._listeners[event_name] = [cb for cb in callbacks if cb != callback]
            if not self._listeners[event_name]:
                self._listeners.pop(event_name, None)

    def remove_all_listeners(self, event_name: str | None = None) -> None:
        with self._listeners_lock:
            if event_name is None:
                self._listeners.clear()
            else:
                self._listeners.pop(event_name, None)

    def emit(self, event_name: str, *args: Any) -> None:
        with self._listeners_lock:
            callbacks = list(self._listeners.get(event_name, ()))

        for callback in callbacks:
            try:
                callback(*args)
            except Exception:
                LOGGER.exception("Error in %s listener", event_name)
