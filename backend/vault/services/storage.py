import hashlib
from pathlib import Path
from tempfile import SpooledTemporaryFile
from typing import BinaryIO
import httpx
from ..config import settings


def sha256_file(file_obj: BinaryIO, chunk_size: int = 1024 * 1024):
    h = hashlib.sha256()
    size = 0
    pos = file_obj.tell()
    file_obj.seek(0)
    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        h.update(chunk)
        size += len(chunk)
    file_obj.seek(pos)
    return h.hexdigest(), size


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_name(name: str) -> str:
    return Path(name).name.replace("\\", "_").replace("/", "_")[:200]


def make_temp_upload() -> SpooledTemporaryFile:
    return SpooledTemporaryFile(max_size=16 * 1024 * 1024, mode="w+b")


async def store_chunk(node_url: str, object_id: str, chunk_id: str, chunk_index: int, version: int, checksum: str, data: bytes):
    files = {"file": (f"{chunk_id}.bin", data, "application/octet-stream")}
    params = {
        "object_id": object_id,
        "chunk_id": chunk_id,
        "chunk_index": chunk_index,
        "version": version,
        "checksum": checksum,
    }
    async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
        r = await client.post(f"{node_url}/store", params=params, files=files)
        r.raise_for_status()
        return r.json()


async def fetch_chunk(node_url: str, chunk_id: str):
    async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
        r = await client.get(f"{node_url}/fetch/{chunk_id}")
        r.raise_for_status()
        return r.content, r.headers.get("x-vault-checksum", "")


async def fetch_checksum(node_url: str, chunk_id: str):
    async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
        r = await client.get(f"{node_url}/checksum/{chunk_id}")
        r.raise_for_status()
        return r.json()


async def delete_chunk(node_url: str, chunk_id: str):
    async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
        r = await client.delete(f"{node_url}/chunk/{chunk_id}")
        if r.status_code not in (200, 404):
            r.raise_for_status()
        return r.json() if r.content else {"ok": True}
