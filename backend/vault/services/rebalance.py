import asyncio
import time
from .. import db
from ..config import settings
from ..events import emit
from .nodes import NodeService
from .storage import fetch_chunk, store_chunk, delete_chunk, sha256_bytes, fetch_checksum

node_service = NodeService()
_rebalance_lock = asyncio.Lock()


async def rebalance_once(force: bool = False):
    async with _rebalance_lock:
        nodes = node_service.available_nodes()
        if len(nodes) < 2:
            return {"moved": 0, "reason": "not enough healthy nodes"}

        loads = [(n, (n["used_bytes"] / n["capacity_bytes"]) if n["capacity_bytes"] else n["load"]) for n in nodes]
        low_node, low_load = min(loads, key=lambda x: x[1])
        high_node, high_load = max(loads, key=lambda x: x[1])
        gap = high_load - low_load
        if not force and gap < settings.rebalance_threshold:
            return {"moved": 0, "reason": "balanced", "gap": gap}
        if high_node["node_id"] == low_node["node_id"]:
            return {"moved": 0, "reason": "single node"}

        # Move an existing replica while keeping RF unchanged. The target must not already hold it.
        candidate = db.fetchone(
            """SELECT r.chunk_id,c.object_id,c.chunk_index,c.version,c.checksum,c.size_bytes,
                       o.replication_factor
               FROM replicas r
               JOIN chunks c ON c.chunk_id=r.chunk_id
               JOIN objects o ON o.object_id=c.object_id
               WHERE r.node_id=? AND r.status='HEALTHY'
                 AND (SELECT COUNT(*) FROM replicas rr WHERE rr.chunk_id=r.chunk_id AND rr.status='HEALTHY') >= o.replication_factor
                 AND NOT EXISTS (SELECT 1 FROM replicas tx WHERE tx.chunk_id=r.chunk_id AND tx.node_id=?)
                 AND (SELECT COUNT(*) FROM replicas keep WHERE keep.chunk_id=r.chunk_id AND keep.status='HEALTHY' AND keep.node_id<>r.node_id) >= 1
               ORDER BY c.size_bytes DESC LIMIT 1""",
            (high_node["node_id"], low_node["node_id"]),
        )
        if not candidate:
            return {"moved": 0, "reason": "no safe movable replica", "gap": gap}

        src = node_service.node(high_node["node_id"])
        dst = node_service.node(low_node["node_id"])
        data, checksum = await fetch_chunk(src["url"], candidate["chunk_id"])
        if checksum != candidate["checksum"] or sha256_bytes(data) != candidate["checksum"]:
            emit("WARN", "REBALANCE_SKIPPED", "Source replica failed integrity check", {"chunk_id": candidate["chunk_id"]})
            return {"moved": 0, "reason": "source integrity failure"}

        await store_chunk(
            dst["url"], candidate["object_id"], candidate["chunk_id"], candidate["chunk_index"],
            candidate["version"], candidate["checksum"], data,
        )
        meta = await fetch_checksum(dst["url"], candidate["chunk_id"])
        if meta.get("actual_checksum") != candidate["checksum"] or int(meta.get("version", 0)) != int(candidate["version"]):
            try:
                await delete_chunk(dst["url"], candidate["chunk_id"])
            except Exception:
                pass
            return {"moved": 0, "reason": "target verification failed"}

        db.execute(
            "INSERT OR REPLACE INTO replicas(chunk_id,node_id,status,last_verified) VALUES(?,?,?,?)",
            (candidate["chunk_id"], low_node["node_id"], "HEALTHY", time.time()),
        )

        # Target is verified before the source is removed; RF therefore never drops below its policy.
        await delete_chunk(src["url"], candidate["chunk_id"])
        db.execute("DELETE FROM replicas WHERE chunk_id=? AND node_id=?", (candidate["chunk_id"], high_node["node_id"]))
        emit(
            "INFO",
            "REBALANCE_COMPLETED",
            f"Moved {candidate['chunk_id']} from node {high_node['node_id']} to node {low_node['node_id']}",
            {"chunk_id": candidate["chunk_id"], "source": high_node["node_id"], "target": low_node["node_id"], "gap": gap},
        )
        return {
            "moved": 1,
            "source": high_node["node_id"],
            "target": low_node["node_id"],
            "chunk_id": candidate["chunk_id"],
            "gap": gap,
        }


async def rebalance_loop(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            await rebalance_once()
        except Exception as exc:
            emit("ERROR", "REBALANCE_ERROR", str(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.rebalance_interval)
        except asyncio.TimeoutError:
            pass
