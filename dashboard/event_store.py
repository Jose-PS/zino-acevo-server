"""Persistent SQLite store for AC EVO server log lines, exposed by id cursor."""

from __future__ import annotations

import hashlib
import os
import queue
import sqlite3
import sys
import threading
import time
from pathlib import Path

DB_PATH = Path(os.environ.get("ACEVO_EVENTS_DB", "/data/events.sqlite"))
IGNORED_MARKER = "] Received Ping"
DEFAULT_LIMIT = 1000
MAX_LIMIT = 5000
BATCH_SIZE = 500
BATCH_WINDOW_SECONDS = 0.5
FLUSH_TIMEOUT_SECONDS = 10.0

_INSERT = "INSERT OR IGNORE INTO events (line, line_hash) VALUES (?, ?)"

_queue: queue.SimpleQueue = queue.SimpleQueue()
_cond = threading.Condition()
_pending = 0
_writer: threading.Thread | None = None
_writer_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "line TEXT NOT NULL, "
        "line_hash TEXT NOT NULL UNIQUE, "
        "recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    return conn


def _normalize(line: str | bytes) -> str:
    if isinstance(line, bytes):
        line = line.decode("utf-8", errors="replace")
    return line.rstrip("\r\n")


def _accepted(line: str) -> bool:
    return bool(line.strip()) and IGNORED_MARKER not in line


def _hash(line: str) -> str:
    return hashlib.sha256(line.encode("utf-8", errors="replace")).hexdigest()


def _report(action: str, exc: Exception) -> None:
    try:
        sys.stderr.write(f"[dashboard] event store {action} failed: {exc}\n")
        sys.stderr.flush()
    except Exception:  # noqa: BLE001
        pass


def _insert_batch(conn: sqlite3.Connection, lines: list[str]) -> int:
    rows = [(text, _hash(text)) for text in lines]
    with conn:
        before = conn.total_changes
        conn.executemany(_INSERT, rows)
        return conn.total_changes - before


def _collect_batch() -> list[str]:
    batch = [_queue.get()]
    deadline = time.monotonic() + BATCH_WINDOW_SECONDS
    while len(batch) < BATCH_SIZE:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            batch.append(_queue.get(timeout=remaining))
        except queue.Empty:
            break
    return batch


def _writer_loop() -> None:
    global _pending
    conn: sqlite3.Connection | None = None
    conn_path: Path | None = None
    while True:
        batch = _collect_batch()
        try:
            if conn is None or conn_path != Path(DB_PATH):
                if conn is not None:
                    conn.close()
                    conn = None
                conn = _connect()
                conn_path = Path(DB_PATH)
            _insert_batch(conn, batch)
        except Exception as exc:  # noqa: BLE001 - the writer must stay alive whatever SQLite does
            _report("write", exc)
            try:
                if conn is not None:
                    conn.close()
            except Exception:  # noqa: BLE001
                pass
            conn = None
        finally:
            with _cond:
                _pending -= len(batch)
                _cond.notify_all()


def _ensure_writer() -> None:
    global _writer
    if _writer is not None and _writer.is_alive():
        return
    with _writer_lock:
        if _writer is None or not _writer.is_alive():
            _writer = threading.Thread(target=_writer_loop, name="event-store-writer", daemon=True)
            _writer.start()


def record(line: str | bytes) -> bool:
    global _pending
    text = _normalize(line)
    if not _accepted(text):
        return False
    _ensure_writer()
    with _cond:
        _pending += 1
    _queue.put(text)
    return True


def flush(timeout: float = FLUSH_TIMEOUT_SECONDS) -> bool:
    deadline = time.monotonic() + timeout
    with _cond:
        while _pending > 0:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            _cond.wait(remaining)
    return True


def import_lines(lines) -> int:
    flush()
    accepted = []
    for raw in lines:
        text = _normalize(raw)
        if _accepted(text):
            accepted.append(text)
    if not accepted:
        return 0
    conn = _connect()
    try:
        inserted = 0
        for start in range(0, len(accepted), BATCH_SIZE):
            inserted += _insert_batch(conn, accepted[start : start + BATCH_SIZE])
        return inserted
    finally:
        conn.close()


def import_file(path: Path) -> int:
    if not path.exists():
        return 0
    with open(path, "rb") as handle:
        return import_lines(handle)


def fetch(after: int = 0, limit: int = DEFAULT_LIMIT) -> dict:
    limit = max(1, min(limit, MAX_LIMIT))
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT id, line FROM events WHERE id > ? ORDER BY id ASC LIMIT ?", (after, limit)
        ).fetchall()
    finally:
        conn.close()
    events = [{"id": row[0], "line": row[1]} for row in rows]
    return {
        "events": events,
        "first_id": events[0]["id"] if events else None,
        "last_id": events[-1]["id"] if events else None,
    }
