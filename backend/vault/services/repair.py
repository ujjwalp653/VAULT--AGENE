import asyncio
import time
from .. import db
from ..config import settings
from ..events import emit
from .nodes import NodeService
from .storage import fetch_checksum, fetch_chunk, store_chunk, sha256_bytes

node_service = NodeService()
_repair_lock = asyncio.Lock()


def replica_rows(chunk_id: str):
    return db.fetchall(
        """SELECT r.*,n.url,n.status AS node_status,n.last_heartbeat
           FROM replicas r JOIN nodes n ON n.node_id=r.node_id
           WHERE r.chunk_id=? ORDER BY r.node_id""",
        (chunk_id,),
    )


def desired_policy(chunk_id: str):
    row = db.fetchone(
        """SELECT o.replication_factor,o.read_quorum,o.write_quorum
           FROM chunks c JOIN objects o ON o.object_id=c.object_id
           WHERE c.chunk_id=?""",
        (chunk_id,),
    )
    return (
        int(row["replication_factor"]) if row else settings.default_rf,
        int(row["read_quorum"]) if row else settings.default_r,
        int(row["write_quorum"]) if row else settings.default_w,
    )


async def verify_replica(chunk_id: str, node_id: int, expected_checksum: str, expected_version: int | None = None):
    node = node_service.node(node_id)
    if not node:
        return False, "node missing"
    try:
        meta = await fetch_checksum(node["url"], chunk_id)
        actual = meta.get("actual_checksum", "")
        manifest_checksum = meta.get("checksum", "")
        actual_version = int(meta.get("version", 0))
        if actual != expected_checksum or manifest_checksum != expected_checksum:
            db.execute(
                "UPDATE replicas SET status=?,last_verified=? WHERE chunk_id=? AND node_id=?",
                ("CORRUPTED", time.time(), chunk_id, node_id),
            )
            return False, "checksum mismatch"
        if expected_version is not None and actual_version != int(expected_version):
            db.execute(
                "UPDATE replicas SET status=?,last_verified=? WHERE chunk_id=? AND node_id=?",
                ("INCONSISTENT", time.time(), chunk_id, node_id),
            )
            return False, "version mismatch"
        db.execute(
            "UPDATE replicas SET status=?,last_verified=? WHERE chunk_id=? AND node_id=?",
            ("HEALTHY", time.time(), chunk_id, node_id),
        )
        return True, "verified"
    except Exception as exc:
        db.execute("UPDATE replicas SET status=? WHERE chunk_id=? AND node_id=?", ("UNREACHABLE", chunk_id, node_id))
        return False, str(exc)


async def repair_chunk(chunk_id: str, reason: str):
    async with _repair_lock:
        chunk = db.fetchone("SELECT * FROM chunks WHERE chunk_id=?", (chunk_id,))
        if not chunk:
            return {"ok": False, "reason": "chunk missing"}
        rf, _, _ = desired_policy(chunk_id)
        replicas = replica_rows(chunk_id)

        healthy = []
        for r in replicas:
            if r["status"] != "HEALTHY":
                continue
            ok, _ = await verify_replica(chunk_id, r["node_id"], chunk["checksum"], chunk["version"])
            if ok:
                healthy.append(r["node_id"])

        if len(healthy) >= rf:
            return {"ok": True, "status": "already healthy", "healthy_replicas": len(healthy), "desired": rf}

        # Any healthy node can be the source. Fetch actual bytes only once.
        source = None
        source_data = None
        for node_id in healthy:
            n = node_service.node(node_id)
            if not n:
                continue
            try:
                data, header_checksum = await fetch_chunk(n["url"], chunk_id)
                if header_checksum == chunk["checksum"] and sha256_bytes(data) == chunk["checksum"]:
                    source = node_id
                    source_data = data
                    break
            except Exception:
                continue

        if source_data is None:
            emit("ERROR", "REPAIR_FAILED", f"No healthy source for {chunk_id}", {"reason": reason})
            return {"ok": False, "reason": "no healthy source"}

        # Prefer a healthy/available node that does not already have a valid copy.
        replicas_by_node = {r["node_id"]: r for r in replicas}
        candidates = node_service.available_nodes(exclude=healthy)
        ordered = []
        for target in candidates:
            existing = replicas_by_node.get(target["node_id"])
            if existing and existing["status"] == "HEALTHY":
                continue
            ordered.append(target)

        for target in ordered:
            if len(healthy) >= rf:
                break
            start = time.time()
            repair_id = db.execute(
                "INSERT INTO repair_events(ts_started,chunk_id,source_node,target_node,reason,status) VALUES(?,?,?,?,?,?)",
                (start, chunk_id, source, target["node_id"], reason, "RUNNING"),
            )
            emit(
                "INFO",
                "REPAIR_STARTED",
                f"Repairing {chunk_id}: node {source} → node {target['node_id']}",
                {"chunk_id": chunk_id, "source": source, "target": target["node_id"], "reason": reason},
            )
            try:
                await store_chunk(target["url"], chunk["object_id"], chunk_id, chunk["chunk_index"], chunk["version"], chunk["checksum"], source_data)
                ok, detail = await verify_replica(chunk_id, target["node_id"], chunk["checksum"], chunk["version"])
                if not ok:
                    raise RuntimeError(detail)
                db.execute(
                    "INSERT OR REPLACE INTO replicas(chunk_id,node_id,status,last_verified) VALUES(?,?,?,?)",
                    (chunk_id, target["node_id"], "HEALTHY", time.time()),
                )
                healthy.append(target["node_id"])
                duration_ms = (time.time() - start) * 1000
                db.execute("UPDATE repair_events SET ts_completed=?,status=? WHERE id=?", (time.time(), "COMPLETED", repair_id))
                emit(
                    "INFO",
                    "REPAIR_COMPLETED",
                    f"Replica restored on node {target['node_id']}",
                    {"chunk_id": chunk_id, "source": source, "target": target["node_id"], "duration_ms": round(duration_ms, 1)},
                )
            except Exception as exc:
                db.execute(
                    "UPDATE repair_events SET ts_completed=?,status=?,error=? WHERE id=?",
                    (time.time(), "FAILED", str(exc)[:500], repair_id),
                )
                emit("ERROR", "REPAIR_FAILED", f"Repair failed for {chunk_id} on node {target['node_id']}", {"error": str(exc)[:300]})

        ok = len(healthy) >= rf
        return {"ok": ok, "healthy_replicas": len(healthy), "desired": rf}


async def repair_all():
    chunks = db.fetchall("SELECT chunk_id FROM chunks")
    results = []
    for c in chunks:
        try:
            results.append(await repair_chunk(c["chunk_id"], "background reconciliation"))
        except Exception as exc:
            emit("ERROR", "REPAIR_LOOP_ERROR", str(exc))
            results.append({"ok": False, "reason": str(exc)})
    return results


async def integrity_scan(limit: int = 500):
    chunks = db.fetchall("SELECT chunk_id,checksum,version FROM chunks ORDER BY chunk_id LIMIT ?", (limit,))
    checked = 0
    for chunk in chunks:
        for r in replica_rows(chunk["chunk_id"]):
            if r["node_status"] not in {"HEALTHY", "DEGRADED"}:
                continue
            if r["status"] not in {"HEALTHY", "CORRUPTED", "INCONSISTENT"}:
                continue
            checked += 1
            ok, detail = await verify_replica(chunk["chunk_id"], r["node_id"], chunk["checksum"], chunk["version"])
            if not ok:
                kind = "CORRUPTION_DETECTED" if detail == "checksum mismatch" else "REPLICA_INCONSISTENCY_DETECTED"
                emit("WARN", kind, f"Replica integrity problem on node {r['node_id']}", {"chunk_id": chunk["chunk_id"], "node": r["node_id"], "reason": detail})
                await repair_chunk(chunk["chunk_id"], "corruption" if detail == "checksum mismatch" else "inconsistency")
    return checked


async def repair_loop(stop_event: asyncio.Event):
    last_integrity = 0.0
    while not stop_event.is_set():
        try:
            await repair_all()
            now = time.time()
            if now - last_integrity >= settings.integrity_interval:
                await integrity_scan()
                last_integrity = now
        except Exception as exc:
            emit("ERROR", "REPAIR_LOOP_ERROR", str(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.repair_interval)
        except asyncio.TimeoutError:
            pass
