import asyncio
import json
import os
import random
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse

from .config import settings
from . import db
from .events import emit, subscribe
from .services.nodes import NodeService
from .services.coordinator import create_object, object_detail, stream_object, cluster_status
from .services.repair import repair_all
from .services.rebalance import rebalance_loop, rebalance_once
from .services.metrics import get_metrics
from .services.repair import repair_loop

node_service = NodeService()
stop_event = asyncio.Event()
workers = []
node_processes = []



def _auto_start_enabled():
    return os.getenv("AUTO_START_NODES", "0").strip().lower() in {"1", "true", "yes", "on"}


async def _node_is_reachable(node_id: int) -> bool:
    node = node_service.node(node_id)
    if not node:
        return False
    try:
        import httpx
        async with httpx.AsyncClient(timeout=0.8) as client:
            r = await client.get(f"{node['url']}/health")
            return r.status_code == 200
    except Exception:
        return False


async def start_local_nodes_if_needed():
    """Start local storage agents automatically for the Windows demo."""
    if not _auto_start_enabled():
        return
    backend_dir = Path(__file__).resolve().parent.parent
    storage_root = Path(settings.data_root).resolve()
    storage_root.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update({
        "DATA_ROOT": str(storage_root),
        "NODE_BIND_HOST": "127.0.0.1",
        "PYTHONUNBUFFERED": "1",
    })
    log_dir = storage_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    for node_id in range(1, settings.node_count + 1):
        if await _node_is_reachable(node_id):
            continue
        port = settings.node_start_port + node_id - 1
        log_path = log_dir / f"node-{node_id:02d}.log"
        log = log_path.open("a", encoding="utf-8")
        cmd = [
            sys.executable, "-m", "vault.node_agent",
            "--id", str(node_id),
            "--port", str(port),
            "--host", "127.0.0.1",
            "--storage", str(storage_root),
        ]
        kwargs = {"cwd": str(backend_dir), "env": env, "stdout": log, "stderr": log}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True
        proc = subprocess.Popen(cmd, **kwargs)
        node_processes.append((proc, log))

    # Give agents a few seconds to come up, but do not hold the API hostage forever.
    deadline = time.time() + 8
    while time.time() < deadline:
        statuses = await asyncio.gather(*(
            _node_is_reachable(i) for i in range(1, settings.node_count + 1)
        ))
        if all(statuses):
            break
        await asyncio.sleep(0.35)


async def stop_local_nodes():
    for proc, log in node_processes:
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    await asyncio.to_thread(proc.wait, timeout=3)
                except Exception:
                    proc.kill()
        except Exception:
            pass
        try:
            log.close()
        except Exception:
            pass
    node_processes.clear()


def refresh_object_statuses():
    rows = db.fetchall("SELECT object_id,replication_factor FROM objects")
    for row in rows:
        chunks = db.fetchall("SELECT chunk_id FROM chunks WHERE object_id=?", (row["object_id"],))
        all_good = True
        for c in chunks:
            count = db.fetchone(
                "SELECT COUNT(*) AS c FROM replicas WHERE chunk_id=? AND status='HEALTHY'",
                (c["chunk_id"],),
            )["c"]
            if count < row["replication_factor"]:
                all_good = False
                break
        db.execute(
            "UPDATE objects SET status=?,updated_at=? WHERE object_id=?",
            ("HEALTHY" if all_good else "UNDER_REPLICATED", time.time(), row["object_id"]),
        )


async def heartbeat_loop():
    while not stop_event.is_set():
        await node_service.all_heartbeats()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.heartbeat_interval)
        except asyncio.TimeoutError:
            pass


async def object_status_loop():
    while not stop_event.is_set():
        try:
            refresh_object_statuses()
        except Exception as exc:
            emit("ERROR", "STATUS_REFRESH_ERROR", str(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.ensure_dirs()
    db.init_db()
    node_service.seed_nodes()
    await start_local_nodes_if_needed()
    await node_service.all_heartbeats()
    emit("INFO", "SYSTEM_START", "VAULT coordinator started", {"nodes": settings.node_count, "auto_start_nodes": _auto_start_enabled()})
    stop_event.clear()
    workers.extend([
        asyncio.create_task(heartbeat_loop()),
        asyncio.create_task(repair_loop(stop_event)),
        asyncio.create_task(rebalance_loop(stop_event)),
        asyncio.create_task(object_status_loop()),
    ])
    yield
    stop_event.set()
    for task in workers:
        task.cancel()
    workers.clear()
    await stop_local_nodes()


app = FastAPI(title="VAULT Distributed Object Storage", version="2.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins if settings.cors_origins else ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


FRONTEND_DIR = Path(__file__).resolve().parents[2] / "frontend"


@app.get("/", include_in_schema=False)
async def frontend_index():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"service": "VAULT", "message": "Frontend files not found"}


@app.get("/styles.css", include_in_schema=False)
async def frontend_styles():
    return FileResponse(FRONTEND_DIR / "styles.css")


@app.get("/app.js", include_in_schema=False)
async def frontend_app():
    return FileResponse(FRONTEND_DIR / "app.js")


@app.get("/config.js", include_in_schema=False)
async def frontend_config():
    return FileResponse(FRONTEND_DIR / "config.js")


@app.get("/api/health")
async def health():
    return {"ok": True, "service": "vault-coordinator", "time": time.time()}


@app.get("/api/cluster")
async def cluster():
    nodes = node_service.get_nodes()
    objects = db.fetchall(
        """SELECT o.*,
           COALESCE((SELECT MIN(rep_count) FROM (
               SELECT c.chunk_id, SUM(CASE WHEN r.status='HEALTHY' THEN 1 ELSE 0 END) AS rep_count
               FROM chunks c LEFT JOIN replicas r ON r.chunk_id=c.chunk_id
               WHERE c.object_id=o.object_id GROUP BY c.chunk_id
           )), 0) AS healthy_replica_count,
           COALESCE((SELECT COUNT(*) FROM chunks c2 WHERE c2.object_id=o.object_id), 0) AS chunk_count
           FROM objects o ORDER BY o.updated_at DESC"""
    )
    for obj in objects:
        first_chunk = db.fetchone("SELECT chunk_id FROM chunks WHERE object_id=? ORDER BY chunk_index LIMIT 1", (obj["object_id"],))
        if first_chunk:
            replica_rows = db.fetchall("SELECT node_id,status FROM replicas WHERE chunk_id=?", (first_chunk["chunk_id"],))
            obj["replica_node_ids"] = [int(r["node_id"]) for r in replica_rows]
        else:
            obj["replica_node_ids"] = []
    events = db.fetchall("SELECT id,ts,level,kind,message,details_json FROM events ORDER BY id DESC LIMIT 100")
    repair_events = db.fetchall("SELECT * FROM repair_events ORDER BY id DESC LIMIT 50")
    now = time.time()
    healthy = sum(1 for n in nodes if n["status"] in {"HEALTHY", "DEGRADED"} and n["last_heartbeat"] and now - n["last_heartbeat"] <= settings.failure_timeout)
    failed = len(nodes) - healthy
    logical = sum(o["size_bytes"] for o in objects if o["status"] != "WRITE_FAILED")
    physical = sum(n["used_bytes"] for n in nodes)
    return {
        "summary": {
            "nodes": len(nodes),
            "healthy_nodes": healthy,
            "failed_nodes": failed,
            "objects": len([o for o in objects if o["status"] != "WRITE_FAILED"]),
            "logical_bytes": logical,
            "physical_bytes": physical,
            "replication_overhead": round((physical / logical), 2) if logical else 0,
            "repairs": len(repair_events),
        },
        "nodes": nodes,
        "objects": objects,
        "events": [{**e, "details": json.loads(e["details_json"]) if e.get("details_json") else None} for e in events],
        "repairs": repair_events,
    }


@app.get("/api/metrics")
async def metrics():
    return get_metrics()


@app.get("/api/objects")
async def objects():
    return db.fetchall("SELECT * FROM objects ORDER BY updated_at DESC")


@app.get("/api/objects/{object_id}")
async def get_object(object_id: str):
    detail = object_detail(object_id)
    if not detail:
        raise HTTPException(404, "Object not found")
    return detail


@app.post("/api/objects")
async def upload_object(
    file: UploadFile = File(...),
    replication_factor: int = Form(settings.default_rf),
    write_quorum: int = Form(settings.default_w),
    read_quorum: int = Form(settings.default_r),
):
    return await create_object(file, replication_factor, write_quorum, read_quorum)

@app.post("/api/objects/batch")
async def upload_objects_batch(
    files: List[UploadFile] = File(...),
    replication_factor: int = Form(settings.default_rf),
    write_quorum: int = Form(settings.default_w),
    read_quorum: int = Form(settings.default_r),
):
    if not files:
        raise HTTPException(400, "Choose at least one file")
    if len(files) > 20:
        raise HTTPException(413, "Maximum 20 files per batch")
    healthy_nodes = len(node_service.available_nodes())
    if replication_factor > healthy_nodes:
        raise HTTPException(503, f"Replication factor {replication_factor} requires {replication_factor} healthy nodes; only {healthy_nodes} are available")
    semaphore = asyncio.Semaphore(3)

    async def one(upload: UploadFile):
        async with semaphore:
            try:
                result = await create_object(upload, replication_factor, write_quorum, read_quorum)
                return {"ok": True, "object": result, "name": upload.filename}
            except HTTPException as exc:
                return {"ok": False, "name": upload.filename, "status": exc.status_code, "error": str(exc.detail)}
            except Exception as exc:
                return {"ok": False, "name": upload.filename, "status": 500, "error": str(exc)}

    results = await asyncio.gather(*(one(f) for f in files))
    successful = [r["object"] for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]
    emit(
        "INFO" if not failed else "WARN",
        "BATCH_UPLOAD_COMPLETED",
        f"Batch upload finished: {len(successful)} succeeded, {len(failed)} failed",
        {"total": len(results), "succeeded": len(successful), "failed": len(failed)},
    )
    return {"ok": not failed, "total": len(results), "succeeded": len(successful), "failed": len(failed), "objects": successful, "errors": failed}


@app.get("/api/objects/{object_id}/download")
async def download_object(object_id: str):
    detail = object_detail(object_id)
    if not detail:
        raise HTTPException(404, "Object not found")
    headers = {"Content-Disposition": f'attachment; filename="{detail["name"].replace(chr(34), "_")}"'}
    return StreamingResponse(stream_object(object_id), media_type="application/octet-stream", headers=headers)


@app.delete("/api/objects/{object_id}")
async def delete_object(object_id: str):
    detail = object_detail(object_id)
    if not detail:
        raise HTTPException(404, "Object not found")
    # Best effort distributed delete; metadata is authoritative afterwards.
    for chunk in detail["chunks"]:
        for rep in chunk["replicas"]:
            node = node_service.node(rep["node_id"])
            if node:
                from .services.storage import delete_chunk
                try:
                    await delete_chunk(node["url"], chunk["chunk_id"])
                except Exception:
                    pass
    db.execute("DELETE FROM objects WHERE object_id=?", (object_id,))
    emit("INFO", "OBJECT_DELETED", f"Deleted {detail['name']}", {"object_id": object_id})
    return {"ok": True}


@app.post("/api/nodes/{node_id}/kill")
async def kill_node(node_id: int):
    try:
        result = await node_service.admin(node_id, "kill")
    except Exception as exc:
        raise HTTPException(502, str(exc))
    db.execute("UPDATE nodes SET status=?,last_error=? WHERE node_id=?", ("FAILED", "simulated kill", node_id))
    emit("WARN", "NODE_FAILED", f"Node {node_id} was killed", {"node": node_id})
    return result


@app.post("/api/nodes/{node_id}/revive")
async def revive_node(node_id: int):
    try:
        result = await node_service.admin(node_id, "revive")
    except Exception as exc:
        raise HTTPException(502, str(exc))
    db.execute("UPDATE nodes SET status=?,last_error=NULL WHERE node_id=?", ("UNKNOWN", node_id))
    emit("INFO", "NODE_REVIVED", f"Node {node_id} revived", {"node": node_id})
    return result


@app.post("/api/nodes/{node_id}/partition")
async def partition_node(node_id: int):
    try:
        result = await node_service.admin(node_id, "partition")
    except Exception as exc:
        raise HTTPException(502, str(exc))
    db.execute("UPDATE nodes SET status=? WHERE node_id=?", ("PARTITIONED", node_id))
    emit("WARN", "NETWORK_PARTITION", f"Node {node_id} partitioned", {"node": node_id})
    return result


@app.post("/api/nodes/{node_id}/heal")
async def heal_node(node_id: int):
    try:
        result = await node_service.admin(node_id, "heal")
    except Exception as exc:
        raise HTTPException(502, str(exc))
    db.execute("UPDATE nodes SET status=? WHERE node_id=?", ("UNKNOWN", node_id))
    emit("INFO", "NODE_HEALED", f"Node {node_id} network healed", {"node": node_id})
    return result


@app.post("/api/nodes/{node_id}/corrupt/{chunk_id}")
async def corrupt_chunk(node_id: int, chunk_id: str):
    node = node_service.node(node_id)
    if not node:
        raise HTTPException(404, "Node not found")
    if node["status"] not in {"HEALTHY", "DEGRADED"}:
        raise HTTPException(409, f"Node {node_id} is not currently available for corruption injection")
    try:
        import httpx
        async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
            r = await client.post(f"{node['url']}/admin/corrupt/{chunk_id}")
            r.raise_for_status()
            result = r.json()
    except Exception as exc:
        raise HTTPException(502, str(exc))
    db.execute("UPDATE replicas SET status=? WHERE chunk_id=? AND node_id=?", ("CORRUPTED", chunk_id, node_id))
    emit("WARN", "CORRUPTION_INJECTED", f"Corruption injected into {chunk_id} on node {node_id}", {"node": node_id, "chunk_id": chunk_id})
    return result


@app.post("/api/nodes/{node_id}/inconsistent/{chunk_id}")
async def inconsistent_chunk(node_id: int, chunk_id: str):
    node = node_service.node(node_id)
    if not node:
        raise HTTPException(404, "Node not found")
    try:
        import httpx
        async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
            r = await client.post(f"{node['url']}/admin/inconsistent/{chunk_id}")
            r.raise_for_status()
            result = r.json()
    except Exception as exc:
        raise HTTPException(502, str(exc))
    db.execute("UPDATE replicas SET status=? WHERE chunk_id=? AND node_id=?", ("INCONSISTENT", chunk_id, node_id))
    emit("WARN", "REPLICA_INCONSISTENCY", f"Stale replica version injected on node {node_id}", {"node": node_id, "chunk_id": chunk_id})
    return result


@app.post("/api/chaos")
async def chaos_burst():
    """Inject one safe failure event while preserving at least one healthy node."""
    nodes = node_service.get_nodes()
    healthy = [n for n in nodes if n["status"] in {"HEALTHY", "DEGRADED"}]
    if not healthy:
        raise HTTPException(409, "No healthy nodes available for chaos simulation")
    target = random.choice(healthy)
    action = random.choice(["partition", "corrupt", "inconsistent"])
    if action == "corrupt":
        replica = db.fetchone("SELECT r.chunk_id FROM replicas r WHERE r.node_id=? AND r.status='HEALTHY' LIMIT 1", (target["node_id"],))
        if replica:
            import httpx
            try:
                endpoint = "corrupt" if action == "corrupt" else "inconsistent"
                rstatus = "CORRUPTED" if action == "corrupt" else "INCONSISTENT"
                event_kind = "CHAOS_CORRUPTION" if action == "corrupt" else "CHAOS_INCONSISTENCY"
                async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
                    r = await client.post(f"{target['url']}/admin/{endpoint}/{replica['chunk_id']}")
                    r.raise_for_status()
                db.execute("UPDATE replicas SET status=? WHERE chunk_id=? AND node_id=?", (rstatus, replica["chunk_id"], target["node_id"]))
                emit("WARN", event_kind, f"Chaos {action} injected for {replica['chunk_id']} on node {target['node_id']}", {"node": target["node_id"], "chunk_id": replica["chunk_id"]})
                return {"ok": True, "action": action, "node": target["node_id"], "chunk_id": replica["chunk_id"]}
            except Exception:
                pass
        action = "partition"
    result = await node_service.admin(target["node_id"], "partition")
    db.execute("UPDATE nodes SET status='PARTITIONED' WHERE node_id=?", (target["node_id"],))
    emit("WARN", "CHAOS_PARTITION", f"Chaos partitioned node {target['node_id']}", {"node": target["node_id"]})
    return {"ok": True, "action": action, "node": target["node_id"], "result": result}


@app.post("/api/repair")
async def manual_repair():
    await repair_all()
    refresh_object_statuses()
    return {"ok": True}


@app.get("/api/policies")
async def policies():
    return {
        "node_count": settings.node_count,
        "default_replication_factor": settings.default_rf,
        "default_write_quorum": settings.default_w,
        "default_read_quorum": settings.default_r,
        "chunk_size": settings.chunk_size,
        "max_upload": settings.max_upload,
        "heartbeat_interval": settings.heartbeat_interval,
        "failure_timeout": settings.failure_timeout,
        "repair_interval": settings.repair_interval,
        "rebalance_interval": settings.rebalance_interval,
        "rebalance_threshold": settings.rebalance_threshold,
    }


@app.post("/api/rebalance")
async def manual_rebalance():
    result = await rebalance_once(force=True)
    refresh_object_statuses()
    emit("INFO", "REBALANCE_RUN", "Manual rebalancing pass completed", result)
    return result


@app.get("/api/events")
async def events(limit: int = Query(100, ge=1, le=500)):
    rows = db.fetchall("SELECT id,ts,level,kind,message,details_json FROM events ORDER BY id DESC LIMIT ?", (limit,))
    return [{**r, "details": json.loads(r["details_json"]) if r.get("details_json") else None} for r in rows]


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        async for item in subscribe():
            await websocket.send_json(item)
    except WebSocketDisconnect:
        return
