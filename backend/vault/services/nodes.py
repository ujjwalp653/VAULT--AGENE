import time
from typing import Iterable
import httpx
from ..config import settings
from .. import db


class NodeService:
    def __init__(self):
        self.timeout = settings.http_timeout

    def seed_nodes(self):
        now = time.time()
        for node_id in range(1, settings.node_count + 1):
            url = settings.node_url(node_id)
            existing = db.fetchone("SELECT node_id FROM nodes WHERE node_id=?", (node_id,))
            if not existing:
                db.execute(
                    "INSERT INTO nodes(node_id,url,status,created_at) VALUES(?,?,?,?)",
                    (node_id, url, "UNKNOWN", now),
                )

    async def heartbeat(self, node_id: int):
        node = db.fetchone("SELECT * FROM nodes WHERE node_id=?", (node_id,))
        if not node:
            return None
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                r = await client.get(f"{node['url']}/heartbeat")
                r.raise_for_status()
                info = r.json()
            now = time.time()
            db.execute(
                """UPDATE nodes
                   SET status=?,capacity_bytes=?,used_bytes=?,load=?,storage_utilization=?,activity_load=?,active_ops=?,total_ops=?,last_operation_at=?,last_heartbeat=?,last_error=NULL
                 WHERE node_id=?""",
                (
                    info.get("status", "HEALTHY"),
                    info.get("capacity_bytes", 0),
                    info.get("used_bytes", 0),
                    info.get("load", 0),
                    info.get("storage_utilization", 0),
                    info.get("activity_load", 0),
                    info.get("active_ops", 0),
                    info.get("total_ops", 0),
                    info.get("last_operation_at"),
                    now,
                    node_id,
                ),
            )
            return info
        except Exception as exc:
            current = node["status"]
            preserved = current if current in {"FAILED", "PARTITIONED"} else "UNREACHABLE"
            db.execute(
                "UPDATE nodes SET status=?,last_error=? WHERE node_id=?",
                (preserved, str(exc)[:300], node_id),
            )
            return None

    async def all_heartbeats(self):
        import asyncio
        results = await asyncio.gather(*(self.heartbeat(i) for i in range(1, settings.node_count + 1)), return_exceptions=True)
        return results

    def get_nodes(self):
        return db.fetchall("SELECT * FROM nodes ORDER BY node_id")

    def available_nodes(self, exclude: Iterable[int] = ()): 
        excluded = set(exclude)
        nodes = self.get_nodes()
        now = time.time()
        out = []
        for n in nodes:
            fresh = n["last_heartbeat"] is not None and now - n["last_heartbeat"] <= settings.failure_timeout
            if n["node_id"] not in excluded and n["status"] in {"HEALTHY", "DEGRADED"} and fresh:
                out.append(n)
        out.sort(key=lambda x: (x["load"], x["used_bytes"]))
        return out

    def node(self, node_id: int):
        return db.fetchone("SELECT * FROM nodes WHERE node_id=?", (node_id,))

    async def admin(self, node_id: int, action: str):
        node = self.node(node_id)
        if not node:
            raise ValueError("Node not found")
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(f"{node['url']}/admin/{action}")
            r.raise_for_status()
            return r.json()
