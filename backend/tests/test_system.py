import asyncio
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


async def wait_json(client, url, timeout=15, predicate=None):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            last = data
            if predicate is None or predicate(data):
                return data
        except Exception:
            pass
        await asyncio.sleep(0.25)
    raise AssertionError(f"Timed out waiting for {url}: {last}")


def start_cluster(data_root: Path):
    env = os.environ.copy()
    env.update({
        "PYTHONPATH": str(ROOT),
        "COORDINATOR_PORT": "18000",
        "NODE_COUNT": "8",
        "NODE_START_PORT": "18101",
        "NODE_BASE_URL": "http://127.0.0.1",
        "NODE_BIND_HOST": "127.0.0.1",
        "AUTO_START_NODES": "1",
        "DATA_ROOT": str(data_root),
        "DB_PATH": str(data_root / "vault.db"),
        "CHUNK_SIZE_BYTES": "1024",
        "MAX_UPLOAD_BYTES": str(16 * 1024 * 1024),
        "HEARTBEAT_INTERVAL": "0.5",
        "FAILURE_TIMEOUT": "2",
        "REPAIR_INTERVAL": "0.5",
        "INTEGRITY_INTERVAL": "1",
        "REBALANCE_INTERVAL": "60",
        "REBALANCE_THRESHOLD": "0.0",
        "HTTP_TIMEOUT": "2",
    })
    log = open(data_root / "coordinator.log", "w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "vault.main:app", "--host", "127.0.0.1", "--port", "18000"],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    return proc, log


async def run():
    temp = Path(tempfile.mkdtemp(prefix="vault_test_"))
    proc = None
    log = None
    base = "http://127.0.0.1:18000"
    try:
        proc, log = start_cluster(temp)
        async with httpx.AsyncClient(timeout=10) as client:
            await wait_json(client, f"{base}/api/health", predicate=lambda x: x.get("ok") is True)
            cluster = await wait_json(client, f"{base}/api/cluster", predicate=lambda x: x.get("summary", {}).get("healthy_nodes") == 8)
            assert len(cluster["nodes"]) == 8
            assert all("activity_load" in n and "storage_utilization" in n and "total_ops" in n for n in cluster["nodes"])

            # 1. Configurable policy + chunking + concurrent writes.
            payload = b"VAULT-DEMO-DATA-" * 400
            async def upload(name, rf=3, w=2, r=1):
                files = {"file": (name, payload, "application/octet-stream")}
                data = {"replication_factor": rf, "write_quorum": w, "read_quorum": r}
                resp = await client.post(f"{base}/api/objects", files=files, data=data)
                resp.raise_for_status()
                return resp.json()

            objects = await asyncio.gather(
                upload("concurrent-1.bin"), upload("concurrent-2.bin"), upload("quorum-read.bin", 3, 2, 2), upload("concurrent-4.bin")
            )
            # Batch/multi-file upload path.
            batch_files = [("files", ("batch-a.bin", b"A" * 2048, "application/octet-stream")), ("files", ("batch-b.bin", b"B" * 3072, "application/octet-stream")), ("files", ("batch-c.bin", b"C" * 4096, "application/octet-stream"))]
            batch_resp = await client.post(f"{base}/api/objects/batch", files=batch_files, data={"replication_factor": 3, "write_quorum": 2, "read_quorum": 1})
            batch_resp.raise_for_status()
            batch = batch_resp.json()
            assert batch["succeeded"] == 3 and batch["failed"] == 0
            obj = objects[2]
            assert obj["replication_factor"] == 3
            assert obj["write_quorum"] == 2
            assert obj["read_quorum"] == 2
            assert len(obj["chunks"]) >= 2, "Chunking did not occur"
            assert all(c["healthy_replica_count"] >= 2 for c in obj["chunks"])

            # Telemetry must reflect stored bytes on at least one node.
            telemetry_cluster = await wait_json(client, f"{base}/api/cluster", predicate=lambda x: any(float(n.get("storage_utilization") or 0) > 0 for n in x["nodes"]))
            assert any(float(n.get("storage_utilization") or 0) > 0 for n in telemetry_cluster["nodes"])

            # 2. Concurrent reads.
            async def read_once():
                r = await client.get(f"{base}/api/objects/{obj['object_id']}/download")
                r.raise_for_status()
                return r.content
            reads = await asyncio.gather(*(read_once() for _ in range(5)))
            assert all(x == payload for x in reads)

            # 3. Node failure + automatic repair + availability.
            detail = await client.get(f"{base}/api/objects/{obj['object_id']}")
            detail.raise_for_status()
            detail = detail.json()
            victim = next(r["node_id"] for c in detail["chunks"] for r in c["replicas"] if r["status"] == "HEALTHY")
            await client.post(f"{base}/api/nodes/{victim}/kill")
            await wait_json(client, f"{base}/api/cluster", predicate=lambda x: any(n["node_id"] == victim and n["status"] == "FAILED" for n in x["nodes"]))
            read_after_failure = await client.get(f"{base}/api/objects/{obj['object_id']}/download")
            read_after_failure.raise_for_status()
            repaired = await wait_json(client, f"{base}/api/objects/{obj['object_id']}", timeout=12, predicate=lambda d: all(c["healthy_replica_count"] >= 3 for c in d["chunks"]))
            assert all(c["healthy_replica_count"] >= 3 for c in repaired["chunks"])

            # 4. Byte corruption -> detection -> automatic repair.
            detail = repaired
            corruption_target = next(r["node_id"] for c in detail["chunks"] for r in c["replicas"] if r["status"] == "HEALTHY" and r["node_id"] != victim)
            corruption_chunk = detail["chunks"][0]["chunk_id"]
            await client.post(f"{base}/api/nodes/{corruption_target}/corrupt/{corruption_chunk}")
            fixed = await wait_json(client, f"{base}/api/objects/{obj['object_id']}", timeout=12, predicate=lambda d: d["chunks"][0]["healthy_replica_count"] >= 3)
            assert fixed["chunks"][0]["healthy_replica_count"] >= 3
            metrics = (await client.get(f"{base}/api/metrics")).json()
            assert metrics["integrity"]["corrupted"] == 0 or metrics["integrity"]["repaired"] >= 1

            # 5. Replica inconsistency (stale version) -> detection -> repair.
            detail = fixed
            incons_target = next(r["node_id"] for r in detail["chunks"][0]["replicas"] if r["status"] == "HEALTHY")
            await client.post(f"{base}/api/nodes/{incons_target}/inconsistent/{detail['chunks'][0]['chunk_id']}")
            repaired_again = await wait_json(client, f"{base}/api/objects/{obj['object_id']}", timeout=12, predicate=lambda d: d["chunks"][0]["healthy_replica_count"] >= 3)
            assert repaired_again["chunks"][0]["healthy_replica_count"] >= 3

            # 6. Partial network partition + heal.
            partition_target = next(n["node_id"] for n in (await client.get(f"{base}/api/cluster")).json()["nodes"] if n["node_id"] != victim)
            await client.post(f"{base}/api/nodes/{partition_target}/partition")
            await wait_json(client, f"{base}/api/cluster", predicate=lambda x: any(n["node_id"] == partition_target and n["status"] == "PARTITIONED" for n in x["nodes"]))
            await client.post(f"{base}/api/nodes/{partition_target}/heal")
            await wait_json(client, f"{base}/api/cluster", predicate=lambda x: any(n["node_id"] == partition_target and n["status"] == "HEALTHY" for n in x["nodes"]))

            # 7. Rebalance endpoint and RF preservation.
            before = await client.get(f"{base}/api/objects/{obj['object_id']}")
            before.raise_for_status()
            before = before.json()
            reb = await client.post(f"{base}/api/rebalance")
            reb.raise_for_status()
            after = await client.get(f"{base}/api/objects/{obj['object_id']}")
            after.raise_for_status()
            after = after.json()
            assert all(c["healthy_replica_count"] >= 3 for c in after["chunks"]), reb.text

            # 8. Navigation/API contract endpoints.
            for path in ("/", "/styles.css", "/app.js", "/config.js", "/api/health", "/api/cluster", "/api/metrics", "/api/policies", "/api/objects", "/docs"):
                r = await client.get(f"{base}{path}")
                assert r.status_code == 200, (path, r.status_code, r.text[:300])

            report = {
                "nodes": 8,
                "concurrent_writes": 4,
                "batch_uploads": 3,
                "concurrent_reads": 5,
                "chunking": len(obj["chunks"]),
                "node_failure_repaired": True,
                "corruption_repaired": True,
                "inconsistency_repaired": True,
                "partition_healed": True,
                "rebalancing_endpoint": True,
                "quorum_policy": "RF=3 W=2 R=2 tested",
                "api_contract": "passed",
            }
            print("VAULT_INTEGRATION_TESTS_PASSED")
            for k, v in report.items():
                print(f"{k}: {v}")
            return report
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        if log:
            log.close()
        shutil.rmtree(temp, ignore_errors=True)


def test_full_system():
    asyncio.run(run())
