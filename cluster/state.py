from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import RLock


@dataclass
class Entry:
    index: int
    term: int
    key: str
    value: str


class PersistentState:
    """Durable node state stored as one atomically replaced JSON document."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = RLock()
        self.term = 0
        self.voted_for: str | None = None
        self.log: list[Entry] = []
        self.commit_index = 0
        self.data: dict[str, str] = {}
        self.load()

    def load(self) -> None:
        with self.lock:
            if not self.path.exists():
                self._save_locked()
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.term = int(raw.get("term", 0))
            self.voted_for = raw.get("voted_for")
            self.log = [Entry(**item) for item in raw.get("log", [])]
            self.commit_index = int(raw.get("commit_index", 0))
            self.data = {str(k): str(v) for k, v in raw.get("data", {}).items()}

    def _save_locked(self) -> None:
        payload = {
            "term": self.term,
            "voted_for": self.voted_for,
            "log": [asdict(item) for item in self.log],
            "commit_index": self.commit_index,
            "data": self.data,
        }
        fd, temporary = tempfile.mkstemp(dir=self.path.parent, prefix=".state-", text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def save(self) -> None:
        with self.lock:
            self._save_locked()

    @property
    def last_index(self) -> int:
        return len(self.log)

    @property
    def last_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def append(self, entry: Entry) -> None:
        with self.lock:
            if entry.index != len(self.log) + 1:
                raise ValueError("log index must be sequential")
            self.log.append(entry)
            self._save_locked()

    def reconcile(self, entries: list[Entry]) -> None:
        """Replace the suffix at the first conflict, preserving committed data."""
        with self.lock:
            for incoming in entries:
                position = incoming.index - 1
                if position < len(self.log):
                    if self.log[position] == incoming:
                        continue
                    if incoming.index <= self.commit_index:
                        raise ValueError("cannot truncate a committed entry")
                    self.log = self.log[:position]
                if incoming.index != len(self.log) + 1:
                    raise ValueError("received a log gap")
                self.log.append(incoming)
            self._save_locked()

    def truncate_to(self, index: int) -> None:
        with self.lock:
            if index < self.commit_index:
                raise ValueError("cannot truncate committed entries")
            if index < 0 or index > len(self.log):
                raise ValueError("invalid truncation index")
            if index != len(self.log):
                self.log = self.log[:index]
                self._save_locked()

    def apply_commit(self, commit_index: int) -> None:
        with self.lock:
            target = min(commit_index, len(self.log))
            if target < self.commit_index:
                return
            for entry in self.log[self.commit_index:target]:
                self.data[entry.key] = entry.value
            self.commit_index = target
            self._save_locked()
