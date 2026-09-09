"""The replicated state machine: an ordered chat transcript.

Raft guarantees every node applies the same commands in the same order, so
every node's ``messages`` list is identical once caught up. Applying a ``msg``
command appends it and notifies local subscribers (connected chat clients).
"""

from __future__ import annotations

from typing import Callable


class ChatState:
    def __init__(self) -> None:
        self.messages: list[dict] = []
        self._subscribers: list[Callable[[dict], None]] = []

    def apply(self, command: dict) -> None:
        if not isinstance(command, dict):
            return
        if command.get("type") == "msg":
            seq = len(self.messages) + 1
            entry = {
                "seq": seq,
                "sender": command.get("sender", "?"),
                "text": command.get("text", ""),
            }
            self.messages.append(entry)
            for cb in list(self._subscribers):
                cb(entry)
        # "noop" and "join" commands change nothing observable here

    def subscribe(self, cb: Callable[[dict], None]) -> Callable[[], None]:
        self._subscribers.append(cb)
        return lambda: self._subscribers.remove(cb) if cb in self._subscribers else None

    def transcript(self) -> list[str]:
        return [f"{m['seq']:>3} <{m['sender']}> {m['text']}" for m in self.messages]
