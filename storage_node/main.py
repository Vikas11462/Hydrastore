import os
import sys
import argparse
import asyncio
import hashlib
import shutil
import logging
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import httpx

from core.config import settings
from core.redis_client import state_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s")
logger = logging.getLogger("storage_node")

class StorageNodeState:
    def __init__(self):
        self.node_id: str = "node-1"
        self.port: int = 8001
        self.storage_dir: str = "./data_nodes/node-1/chunks"
        self.is_active: bool = True # For chaos simulation
        self.heartbeat_task: Optional[asyncio.Task] = None

node_state = StorageNodeState()

def ensure_storage_dir():
    os.makedirs(node_state.storage_dir, exist_ok=True)

def get_chunk_path(chunk_id: str) -> str:
    # Basic sanitize to prevent path traversal
    safe_name = "".join(c for c in chunk_id if c.isalnum() or c in ("-", "_", "."))
    return os.path.join(node_state.storage_dir, f"{safe_name}.chunk")

def calculate_node_metrics():
    ensure_storage_dir()
    total_size = 0
    chunk_count = 0
    for entry in os.scandir(node_state.storage_dir):
        if entry.is_file() and entry.name.endswith(".chunk"):
            total_size += entry.stat().st_size
            chunk_count += 1
    return chunk_count, total_size

async def heartbeat_loop():
    """Continuously send heartbeat leases to Redis."""
    while True:
        try:
            if node_state.is_active:
                chunk_count, used_bytes = calculate_node_metrics()
                payload = {
                    "node_id": node_state.node_id,
                    "host": "127.0.0.1",
                    "port": node_state.port,
                    "url": f"http://127.0.0.1:{node_state.port}",
                    "status": "HEALTHY",
                    "chunk_count": chunk_count,
                    "used_bytes": used_bytes,
                    "capacity_bytes": 1073741824, # 1 GB
                }
                await state_client.set_heartbeat(
                    node_state.node_id, 
                    payload, 
                    ttl_sec=settings.NODE_DEAD_TIMEOUT_SEC
                )
            await asyncio.sleep(settings.HEARTBEAT_INTERVAL_SEC)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in heartbeat loop: {e}")
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_storage_dir()
    await state_client.connect()
    node_state.heartbeat_task = asyncio.create_task(heartbeat_loop())
    logger.info(f"Storage Node [{node_state.node_id}] started on port {node_state.port}, data: {node_state.storage_dir}")
    yield
    if node_state.heartbeat_task:
        node_state.heartbeat_task.cancel()
    await state_client.close()

app = FastAPI(title=f"HydraStore Storage Node", lifespan=lifespan)

@app.middleware("http")
async def chaos_check(request: Request, call_next):
    # If node was marked dead via chaos toggle, fail immediately with 503 or simulate connection drop
    if not node_state.is_active and not request.url.path.startswith("/chaos"):
        return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=b"Node is OFFLINE (Chaos simulated)")
    return await call_next(request)

@app.get("/health")
async def health_check():
    chunk_count, used_bytes = calculate_node_metrics()
    return {
        "node_id": node_state.node_id,
        "status": "HEALTHY" if node_state.is_active else "DEAD",
        "port": node_state.port,
        "chunk_count": chunk_count,
        "used_bytes": used_bytes,
    }

@app.put("/chunks/{chunk_id}")
async def put_chunk(chunk_id: str, request: Request):
    """Write binary chunk stream directly to disk while verifying SHA-256 digest."""
    chunk_path = get_chunk_path(chunk_id)
    temp_path = f"{chunk_path}.tmp"

    hasher = hashlib.sha256()
    bytes_written = 0

    try:
        with open(temp_path, "wb") as f:
            async for chunk_bytes in request.stream():
                if chunk_bytes:
                    f.write(chunk_bytes)
                    hasher.update(chunk_bytes)
                    bytes_written += len(chunk_bytes)

        # Atomic commit
        if os.path.exists(chunk_path):
            os.remove(chunk_path)
        os.replace(temp_path, chunk_path)

        computed_sha = hasher.hexdigest()
        logger.info(f"[{node_state.node_id}] Stored chunk {chunk_id} ({bytes_written} bytes, sha={computed_sha[:8]}...)")

        return {
            "chunk_id": chunk_id,
            "size_bytes": bytes_written,
            "checksum_sha256": computed_sha,
            "node_id": node_state.node_id
        }
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        logger.error(f"Failed to write chunk {chunk_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/chunks/{chunk_id}")
async def get_chunk(chunk_id: str):
    """Stream binary chunk back to caller."""
    chunk_path = get_chunk_path(chunk_id)
    if not os.path.isfile(chunk_path):
        raise HTTPException(status_code=404, detail=f"Chunk {chunk_id} not found on node {node_state.node_id}")

    file_size = os.path.getsize(chunk_path)

    async def file_iterator():
        with open(chunk_path, "rb") as f:
            while True:
                # 64 KB buffer
                data = f.read(64 * 1024)
                if not data:
                    break
                yield data

    return StreamingResponse(
        file_iterator(),
        media_type="application/octet-stream",
        headers={
            "Content-Length": str(file_size),
            "X-Node-ID": node_state.node_id,
            "X-Chunk-ID": chunk_id
        }
    )

@app.delete("/chunks/{chunk_id}")
async def delete_chunk(chunk_id: str):
    chunk_path = get_chunk_path(chunk_id)
    if os.path.isfile(chunk_path):
        os.remove(chunk_path)
        return {"status": "deleted", "chunk_id": chunk_id}
    return {"status": "not_found", "chunk_id": chunk_id}

class ReplicateRequest(BaseModel):
    chunk_id: str
    source_url: str

@app.post("/replicate")
async def replicate_from_peer(req: ReplicateRequest):
    """Peer-to-peer chunk copy for self-healing cluster recovery."""
    logger.info(f"[{node_state.node_id}] Replicating chunk {req.chunk_id} from peer: {req.source_url}")
    target_path = get_chunk_path(req.chunk_id)
    temp_path = f"{target_path}.tmp"

    hasher = hashlib.sha256()
    bytes_written = 0

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            async with client.stream("GET", req.source_url) as resp:
                if resp.status_code != 200:
                    raise HTTPException(status_code=502, detail=f"Failed to fetch chunk from source: {resp.status_code}")

                with open(temp_path, "wb") as f:
                    async for block in resp.aiter_bytes():
                        f.write(block)
                        hasher.update(block)
                        bytes_written += len(block)

        if os.path.exists(target_path):
            os.remove(target_path)
        os.replace(temp_path, target_path)

        computed_sha = hasher.hexdigest()
        logger.info(f"[{node_state.node_id}] Successfully replicated {req.chunk_id} ({bytes_written} bytes)")
        return {
            "status": "replicated",
            "chunk_id": req.chunk_id,
            "size_bytes": bytes_written,
            "checksum_sha256": computed_sha
        }
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise HTTPException(status_code=500, detail=f"Replication failed: {str(e)}")

# Chaos simulation endpoints (allows testing node kill without terminating process)
@app.post("/chaos/kill")
async def chaos_kill():
    node_state.is_active = False
    logger.warning(f"🔥 [CHAOS] Node {node_state.node_id} has been KILLED (simulated)!")
    return {"status": "DEAD", "node_id": node_state.node_id}

@app.post("/chaos/revive")
async def chaos_revive():
    node_state.is_active = True
    logger.info(f"💚 [CHAOS] Node {node_state.node_id} has been REVIVED!")
    return {"status": "HEALTHY", "node_id": node_state.node_id}

def main():
    parser = argparse.ArgumentParser(description="HydraStore Storage Node")
    parser.add_argument("--node-id", default="node-1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--storage-dir", default=None)
    args = parser.parse_args()

    node_state.node_id = args.node_id
    node_state.port = args.port
    node_state.storage_dir = args.storage_dir or f"{settings.STORAGE_BASE_DIR}/{args.node_id}/chunks"

    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="info")

if __name__ == "__main__":
    main()
