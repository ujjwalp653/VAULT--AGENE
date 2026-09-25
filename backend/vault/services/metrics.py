import json
import time
from .. import db, runtime
from ..config import settings


def get_metrics():
    now = time.time()
    nodes = db.fetchall("SELECT capacity_bytes, used_bytes FROM nodes")
    total_capacity = sum(int(n.get("capacity_bytes") or 0) for n in nodes)
    total_used = sum(int(n.get("used_bytes") or 0) for n in nodes)

    replica_rows = db.fetchall("SELECT status, COUNT(*) AS c FROM replicas GROUP BY status")
    counts = {r["status"]: int(r["c"]) for r in replica_rows}
    checks = sum(counts.values())
    healthy_replica = counts.get("HEALTHY", 0)
    corrupted = counts.get("CORRUPTED", 0)
    inconsistent = counts.get("INCONSISTENT", 0)

    repair_stats = db.fetchone(
        """SELECT COUNT(*) AS total,
                  SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed,
                  AVG(CASE WHEN ts_completed IS NOT NULL THEN (ts_completed-ts_started)*1000 END) AS avg_ms,
                  MAX(CASE WHEN ts_completed IS NOT NULL THEN (ts_completed-ts_started)*1000 END) AS max_ms
           FROM repair_events"""
    ) or {}

    activity = []
    start_bucket = int((now - 19 * 60) // 60) * 60
    events = db.fetchall(
        "SELECT ts,kind,details_json FROM events WHERE ts>=? ORDER BY ts ASC",
        (start_bucket,),
    )
    buckets = {
        b: {"ts": b, "read_bytes": 0, "write_bytes": 0, "events": 0}
        for b in range(start_bucket, int(now // 60) * 60 + 60, 60)
    }
    for e in events:
        b = int(e["ts"] // 60) * 60
        if b not in buckets:
            continue
        buckets[b]["events"] += 1
        try:
            details = json.loads(e["details_json"]) if e.get("details_json") else {}
        except Exception:
            details = {}
        if e["kind"] == "OBJECT_CREATED":
            buckets[b]["write_bytes"] += int(details.get("size_bytes") or 0)
        elif e["kind"] == "OBJECT_READ":
            buckets[b]["read_bytes"] += int(details.get("size_bytes") or 0)
    activity = list(buckets.values())[-20:]

    total_chunks = int((db.fetchone("SELECT COUNT(*) AS c FROM chunks") or {"c": 0})["c"])
    avg_replicas = (healthy_replica / total_chunks) if total_chunks else 0.0
    objects = db.fetchall("SELECT status,COUNT(*) AS c FROM objects GROUP BY status")
    object_status = {r["status"]: int(r["c"]) for r in objects}
    event_rebalance = int((db.fetchone("SELECT COUNT(*) AS c FROM events WHERE kind IN ('REBALANCE_COMPLETED','REBALANCE_RUN')") or {"c": 0})["c"])
    node_partitions = int((db.fetchone("SELECT COUNT(*) AS c FROM nodes WHERE status='PARTITIONED'") or {"c": 0})["c"])
    healthy_nodes = int((db.fetchone("SELECT COUNT(*) AS c FROM nodes WHERE status IN ('HEALTHY','DEGRADED')") or {"c": 0})["c"])
    total_nodes = int((db.fetchone("SELECT COUNT(*) AS c FROM nodes") or {"c": 0})["c"])

    runtime_stats = runtime.snapshot()

    return {
        "default_write_quorum": settings.default_w,
        "default_read_quorum": settings.default_r,
        "healthy_replicas": healthy_replica,
        "avg_replicas": avg_replicas,
        "avg_repair_ms": float(repair_stats.get("avg_ms") or 0),
        "max_repair_ms": float(repair_stats.get("max_ms") or 0),
        "repair_completed": int(repair_stats.get("completed") or 0),
        "object_status": object_status,
        "integrity": {
            "checks": checks,
            "corrupted": corrupted,
            "inconsistent": inconsistent,
            "repaired": int(repair_stats.get("completed") or 0),
            "checksum_ok_pct": (healthy_replica / checks * 100) if checks else 100.0,
        },
        "storage_utilization_pct": (total_used / total_capacity * 100) if total_capacity else 0.0,
        "total_chunks": total_chunks,
        "activity": activity,
        "rebalancing": {"runs": event_rebalance},
        "availability": {"healthy_nodes": healthy_nodes, "total_nodes": total_nodes, "partitions": node_partitions},
        "concurrency": runtime_stats,
    }
