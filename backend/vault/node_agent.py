import argparse
import hashlib
import json
import math
import os
import time
from contextlib import contextmanager
from pathlib import Path
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
import uvicorn


def build_app(node_id: int, port: int, storage_root: str, capacity_bytes: int):
    app = FastAPI(title=f"VAULT Storage Node {node_id}", version="2.0.0")
    root = Path(storage_root).resolve() / f"node-{node_id:02d}"
    root.mkdir(parents=True, exist_ok=True)
    state = {
        "killed": False,
        "partitioned": False,
        "started_at": time.time(),
        "active_ops": 0,
        "total_ops": 0,
        "activity_load": 0.0,
        "last_load_tick": time.time(),
        "last_operation_at": None,
    }

    def manifest_path(chunk_id: str) -> Path:
        return root / f"{chunk_id}.json"

    def data_path(chunk_id: str) -> Path:
        return root / f"{chunk_id}.bin"

    def ensure_available():
        if state["killed"]:
            raise HTTPException(503, "NODE_KILLED")
        if state["partitioned"]:
            raise HTTPException(503, "NODE_PARTITIONED")

    @contextmanager
    def track_operation():
        state["active_ops"] += 1
        state["total_ops"] += 1
        state["last_operation_at"] = time.time()
        try:
            yield
        finally:
            state["active_ops"] = max(0, state["active_ops"] - 1)
            state["last_operation_at"] = time.time()

    def telemetry(used: int):
        now = time.time()
        dt = max(0.05, now - state["last_load_tick"])
        storage_ratio = (used / capacity_bytes) if capacity_bytes else 0.0
        if state["active_ops"] > 0:
            instant = min(1.0, state["active_ops"] / 4.0)
            state["activity_load"] = max(state["activity_load"] * math.exp(-dt / 8.0), instant)
        else:
            state["activity_load"] *= math.exp(-dt / 8.0)
        state["last_load_tick"] = now
        return {
            "storage_utilization": round(storage_ratio, 6),
            "activity_load": round(state["activity_load"], 4),
            "load": round(min(1.0, max(storage_ratio, state["activity_load"])), 4),
            "active_ops": state["active_ops"],
            "total_ops": state["total_ops"],
            "last_operation_at": state["last_operation_at"],
        }

    def used_bytes():
        total = 0
        for p in root.glob("*.bin"):
            try:
                total += p.stat().st_size
            except OSError:
                pass
        return total

    def load_manifest(chunk_id: str):
        p = manifest_path(chunk_id)
        if not p.exists():
            raise HTTPException(404, "Chunk not found")
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as exc:
            raise HTTPException(500, f"Invalid manifest: {exc}")

    @app.get("/heartbeat")
    async def heartbeat():
        ensure_available()
        used = used_bytes()
        t = telemetry(used)
        return {
            "node_id": node_id,
            "status": "HEALTHY",
            "capacity_bytes": capacity_bytes,
            "used_bytes": used,
            **t,
            "uptime": round(time.time() - state["started_at"], 2),
        }

    @app.get("/health")
    async def health():
        return {"ok": not state["killed"], "node_id": node_id, "killed": state["killed"], "partitioned": state["partitioned"]}

    @app.post("/store")
    async def store(
        object_id: str = Query(...),
        chunk_id: str = Query(...),
        chunk_index: int = Query(...),
        version: int = Query(...),
        checksum: str = Query(...),
        file: UploadFile = File(...),
    ):
        ensure_available()
        with track_operation():
            data = await file.read()
            actual = hashlib.sha256(data).hexdigest()
            if actual != checksum:
                raise HTTPException(400, "CHECKSUM_MISMATCH_BEFORE_STORE")
            if used_bytes() + len(data) > capacity_bytes:
                raise HTTPException(507, "NODE_CAPACITY_EXCEEDED")
            data_path(chunk_id).write_bytes(data)
            manifest = {
                "node_id": node_id,
                "object_id": object_id,
                "chunk_id": chunk_id,
                "chunk_index": chunk_index,
                "version": version,
                "checksum": checksum,
                "size_bytes": len(data),
                "stored_at": time.time(),
            }
            manifest_path(chunk_id).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            return {"ok": True, **manifest}

    @app.get("/fetch/{chunk_id}")
    async def fetch(chunk_id: str):
        ensure_available()
        with track_operation():
            meta = load_manifest(chunk_id)
            p = data_path(chunk_id)
            if not p.exists():
                raise HTTPException(404, "Chunk data missing")
            return FileResponse(str(p), media_type="application/octet-stream", headers={"x-vault-checksum": meta["checksum"]})

    @app.get("/checksum/{chunk_id}")
    async def checksum(chunk_id: str):
        ensure_available()
        with track_operation():
            meta = load_manifest(chunk_id)
            dp = data_path(chunk_id)
            if not dp.exists():
                raise HTTPException(404, "Chunk data missing")
            h = hashlib.sha256()
            with dp.open("rb") as f:
                while True:
                    part = f.read(1024 * 1024)
                    if not part:
                        break
                    h.update(part)
            actual = h.hexdigest()
            return {"chunk_id": chunk_id, "checksum": meta["checksum"], "actual_checksum": actual, "size_bytes": meta["size_bytes"], "version": meta["version"]}

    @app.delete("/chunk/{chunk_id}")
    async def delete(chunk_id: str):
        ensure_available()
        with track_operation():
            dp = data_path(chunk_id)
            mp = manifest_path(chunk_id)
            dp.unlink(missing_ok=True)
            mp.unlink(missing_ok=True)
            return {"ok": True, "chunk_id": chunk_id}

    @app.post("/admin/kill")
    async def kill():
        state["killed"] = True
        return {"ok": True, "node_id": node_id, "status": "FAILED"}

    @app.post("/admin/revive")
    async def revive():
        state["killed"] = False
        state["partitioned"] = False
        return {"ok": True, "node_id": node_id, "status": "HEALTHY"}

    @app.post("/admin/partition")
    async def partition():
        state["partitioned"] = True
        return {"ok": True, "node_id": node_id, "status": "PARTITIONED"}

    @app.post("/admin/heal")
    async def heal():
        state["partitioned"] = False
        return {"ok": True, "node_id": node_id, "status": "HEALTHY"}

    @app.post("/admin/corrupt/{chunk_id}")
    async def corrupt(chunk_id: str):
        ensure_available()
        with track_operation():
            dp = data_path(chunk_id)
            if not dp.exists():
                raise HTTPException(404, "Chunk not found")
            with dp.open("ab") as f:
                f.write(b"VAULT_CORRUPTION_MARKER")
            return {"ok": True, "node_id": node_id, "chunk_id": chunk_id, "status": "CORRUPTED"}

    @app.post("/admin/inconsistent/{chunk_id}")
    async def inconsistent(chunk_id: str):
        ensure_available()
        with track_operation():
            meta = load_manifest(chunk_id)
            stale_version = max(1, int(meta.get("version", 1)) - 1)
            meta["version"] = stale_version
            manifest_path(chunk_id).write_text(json.dumps(meta, indent=2), encoding="utf-8")
            return {"ok": True, "node_id": node_id, "chunk_id": chunk_id, "status": "INCONSISTENT", "version": stale_version}

    @app.get("/admin/stats")
    async def stats():
        used = used_bytes()
        return {
            "node_id": node_id,
            "killed": state["killed"],
            "partitioned": state["partitioned"],
            "capacity_bytes": capacity_bytes,
            "used_bytes": used,
            "chunks": len(list(root.glob("*.bin"))),
            **telemetry(used),
        }

    return app


def main():
    parser = argparse.ArgumentParser(description="VAULT independent storage node")
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default=os.getenv("NODE_BIND_HOST", "0.0.0.0"))
    parser.add_argument("--storage", default=os.getenv("DATA_ROOT", "./data"))
    parser.add_argument("--capacity", type=int, default=int(os.getenv("NODE_CAPACITY_BYTES", str(10 * 1024 * 1024 * 1024))))
    args = parser.parse_args()
    app = build_app(args.id, args.port, args.storage, args.capacity)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
