import asyncio
import hashlib
import time
import uuid
from pathlib import Path
from typing import AsyncIterator

from fastapi import UploadFile, HTTPException

from ..config import settings
from .. import db, runtime
from ..events import emit
from .nodes import NodeService
from .storage import make_temp_upload, sha256_file, sha256_bytes, store_chunk, fetch_chunk

node_service = NodeService()


def object_row(object_id: str):
    return db.fetchone("SELECT * FROM objects WHERE object_id=?", (object_id,))


def object_detail(object_id: str):
    obj = object_row(object_id)
    if not obj:
        return None
    chunks = db.fetchall("SELECT * FROM chunks WHERE object_id=? ORDER BY chunk_index", (object_id,))
    out_chunks = []
    for c in chunks:
        reps = db.fetchall(
            """SELECT r.node_id,r.status,r.last_verified,n.status AS node_status,
                      n.used_bytes,n.capacity_bytes,n.load,n.url
               FROM replicas r JOIN nodes n ON n.node_id=r.node_id
               WHERE r.chunk_id=? ORDER BY r.node_id""",
            (c["chunk_id"],),
        )
        healthy = sum(1 for r in reps if r["status"] == "HEALTHY" and r["node_status"] in {"HEALTHY", "DEGRADED"})
        out_chunks.append({**c, "replicas": reps, "healthy_replica_count": healthy})
    return {**obj, "chunks": out_chunks}


def validate_policy(rf: int, write_quorum: int | None, read_quorum: int | None):
    if rf < 1 or rf > settings.node_count:
        raise HTTPException(400, f"replication_factor must be between 1 and {settings.node_count}")
    w = settings.default_w if write_quorum is None else int(write_quorum)
    r = settings.default_r if read_quorum is None else int(read_quorum)
    if not 1 <= w <= rf:
        raise HTTPException(400, "write_quorum must be between 1 and replication_factor")
    if not 1 <= r <= rf:
        raise HTTPException(400, "read_quorum must be between 1 and replication_factor")
    return rf, w, r


def choose_nodes(rf: int, exclude=()):
    return node_service.available_nodes(exclude)[:rf]


async def create_object(upload: UploadFile, replication_factor: int | None = None,
                        write_quorum_value: int | None = None, read_quorum_value: int | None = None):
    rf, w, r = validate_policy(
        settings.default_rf if replication_factor is None else int(replication_factor),
        write_quorum_value,
        read_quorum_value,
    )
    runtime.write_started()
    tmp = make_temp_upload()
    try:
        total_size = 0
        while True:
            block = await upload.read(4 * 1024 * 1024)
            if not block:
                break
            total_size += len(block)
            if total_size > settings.max_upload:
                raise HTTPException(413, f"Maximum upload size is {settings.max_upload} bytes")
            tmp.write(block)

        tmp.seek(0)
        checksum, size = sha256_file(tmp)
        object_id = f"obj_{uuid.uuid4().hex}"
        now = time.time()
        db.execute(
            """INSERT INTO objects(
                object_id,name,size_bytes,checksum,version,replication_factor,
                write_quorum,read_quorum,chunk_size,status,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (object_id, Path(upload.filename or "object.bin").name, size, checksum, 1, rf, w, r,
             settings.chunk_size, "UPLOADING", now, now),
        )

        chunk_infos = []
        index = 0
        while True:
            data = tmp.read(settings.chunk_size)
            if not data:
                break
            chunk_id = f"chk_{uuid.uuid4().hex}"
            csum = hashlib.sha256(data).hexdigest()
            db.execute(
                "INSERT INTO chunks(chunk_id,object_id,chunk_index,size_bytes,checksum,version) VALUES(?,?,?,?,?,?)",
                (chunk_id, object_id, index, len(data), csum, 1),
            )

            targets = choose_nodes(rf)
            if len(targets) < w:
                db.execute("DELETE FROM objects WHERE object_id=?", (object_id,))
                emit("ERROR", "WRITE_FAILED", f"Write quorum unavailable for {object_id}",
                     {"object_id": object_id, "required": w, "available": len(targets)})
                raise HTTPException(503, f"Write quorum unavailable: {len(targets)}/{w} healthy nodes")

            tasks = [
                store_chunk(n["url"], object_id, chunk_id, index, 1, csum, data)
                for n in targets
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successes = 0
            successful_nodes = []
            for n, result in zip(targets, results):
                status = "HEALTHY" if not isinstance(result, Exception) else "UNREACHABLE"
                verified = time.time() if status == "HEALTHY" else None
                db.execute(
                    "INSERT OR REPLACE INTO replicas(chunk_id,node_id,status,last_verified) VALUES(?,?,?,?)",
                    (chunk_id, n["node_id"], status, verified),
                )
                if status == "HEALTHY":
                    successes += 1
                    successful_nodes.append(n["node_id"])

            if successes < w:
                from .storage import delete_chunk
                for node_id in successful_nodes:
                    n = node_service.node(node_id)
                    if n:
                        try:
                            await delete_chunk(n["url"], chunk_id)
                        except Exception:
                            pass
                db.execute("DELETE FROM objects WHERE object_id=?", (object_id,))
                emit("ERROR", "WRITE_FAILED", f"Write quorum failed for {object_id}",
                     {"object_id": object_id, "chunk_id": chunk_id, "successes": successes, "required": w})
                raise HTTPException(503, f"Write quorum failed: {successes}/{w}")

            chunk_infos.append({"chunk_id": chunk_id, "replicas": successful_nodes})
            index += 1

        object_status = "HEALTHY" if all(len(c["replicas"]) >= rf for c in chunk_infos) else "UNDER_REPLICATED"
        db.execute(
            "UPDATE objects SET status=?,updated_at=? WHERE object_id=?",
            (object_status, time.time(), object_id),
        )
        emit("INFO", "OBJECT_CREATED", f"Object {upload.filename} stored", {
            "object_id": object_id,
            "replication_factor": rf,
            "write_quorum": w,
            "read_quorum": r,
            "chunks": len(chunk_infos),
            "size_bytes": size,
        })
        return object_detail(object_id)
    except HTTPException:
        raise
    except Exception as exc:
        emit("ERROR", "OBJECT_CREATE_ERROR", str(exc), {"filename": upload.filename})
        raise HTTPException(500, "Object creation failed")
    finally:
        tmp.close()
        runtime.write_finished()


async def _verified_candidates(chunk: dict, read_quorum: int):
    raw = db.fetchall(
        """SELECT r.node_id,r.status,n.status AS node_status,n.url
           FROM replicas r JOIN nodes n ON n.node_id=r.node_id
           WHERE r.chunk_id=? AND r.status='HEALTHY'
             AND n.status IN ('HEALTHY','DEGRADED')""",
        (chunk["chunk_id"],),
    )
    if not raw:
        return []

    from .repair import verify_replica
    results = await asyncio.gather(*(
        verify_replica(chunk["chunk_id"], r["node_id"], chunk["checksum"], chunk["version"])
        for r in raw
    ), return_exceptions=True)
    verified = []
    for r, result in zip(raw, results):
        if result is True or (isinstance(result, tuple) and result[0] is True):
            verified.append(r)
    if len(verified) < read_quorum:
        return []
    return verified


async def stream_object(object_id: str) -> AsyncIterator[bytes]:
    detail = object_detail(object_id)
    if not detail:
        raise HTTPException(404, "Object not found")
    runtime.read_started()
    delivered = 0
    try:
        read_quorum = int(detail.get("read_quorum") or settings.default_r)
        for chunk in detail["chunks"]:
            verified = await _verified_candidates(chunk, read_quorum)
            if len(verified) < read_quorum:
                emit("ERROR", "READ_QUORUM_FAILED", f"Read quorum unavailable for {chunk['chunk_id']}",
                     {"object_id": object_id, "required": read_quorum, "verified": len(verified)})
                raise HTTPException(503, f"Read quorum unavailable for chunk {chunk['chunk_id']}")

            success = False
            # Verified candidates are good enough for the read; fetch from the first healthy one.
            for replica in verified:
                try:
                    data, header_checksum = await fetch_chunk(replica["url"], chunk["chunk_id"])
                    actual = sha256_bytes(data)
                    if actual != chunk["checksum"] or header_checksum != chunk["checksum"]:
                        db.execute("UPDATE replicas SET status=? WHERE chunk_id=? AND node_id=?", ("CORRUPTED", chunk["chunk_id"], replica["node_id"]))
                        emit("WARN", "CORRUPTION_DETECTED", "Corrupt replica detected during read", {"chunk_id": chunk["chunk_id"], "node": replica["node_id"]})
                        continue
                    delivered += len(data)
                    yield data
                    success = True
                    break
                except Exception as exc:
                    db.execute("UPDATE replicas SET status=? WHERE chunk_id=? AND node_id=?", ("UNREACHABLE", chunk["chunk_id"], replica["node_id"]))
                    emit("WARN", "READ_REPLICA_FAILED", f"Read failed on node {replica['node_id']}", {"chunk_id": chunk["chunk_id"], "error": str(exc)[:200]})
            if not success:
                raise HTTPException(503, f"No valid replica available for chunk {chunk['chunk_id']}")
        emit("INFO", "OBJECT_READ", f"Object {detail['name']} downloaded", {"object_id": object_id, "size_bytes": delivered})
    finally:
        runtime.read_finished()


def cluster_status():
    nodes = db.fetchall("SELECT * FROM nodes ORDER BY node_id")
    objects = db.fetchall(
        "SELECT object_id,name,size_bytes,version,replication_factor,write_quorum,read_quorum,status,created_at,updated_at FROM objects ORDER BY updated_at DESC"
    )
    return {"nodes": nodes, "objects": objects}
