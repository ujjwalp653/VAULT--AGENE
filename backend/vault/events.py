import asyncio
import json
import time
from typing import Any
from . import db

_clients: set[asyncio.Queue] = set()


def emit(level: str, kind: str, message: str, details: Any | None = None):
    payload = {
        "ts": time.time(),
        "level": level,
        "kind": kind,
        "message": message,
        "details": details,
    }
    db.execute(
        "INSERT INTO events(ts, level, kind, message, details_json) VALUES(?,?,?,?,?)",
        (payload["ts"], level, kind, message, json.dumps(details) if details is not None else None),
    )
    for q in list(_clients):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            pass


async def subscribe():
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    _clients.add(q)
    try:
        while True:
            yield await q.get()
    finally:
        _clients.discard(q)
