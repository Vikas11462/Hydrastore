import asyncio
import hashlib
import logging
from typing import List, AsyncGenerator, Dict, Tuple, Optional
import httpx
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from core.config import settings
from core.models import FileModel, ChunkModel, ChunkReplicaModel, StorageNodeModel
from core.placement import ConsistentHashRing
from core.redis_client import state_client

logger = logging.getLogger("coordinator.stream")

async def upload_file_pipeline(
    file_name: str,
    file_stream: AsyncGenerator[bytes, None],
    content_type: str,
    db: AsyncSession,
    ring: ConsistentHashRing,
    node_urls: Dict[str, str],
    healthy_nodes: set[str],
    chunk_size: int = settings.DEFAULT_CHUNK_SIZE,
    rf: int = settings.REPLICATION_FACTOR
) -> FileModel:
    """
    Slices incoming file stream into fixed-size chunks, computes SHA-256 for each chunk
    and total file, determines replica placement on hash ring, and concurrently writes
    to Primary and Replica storage nodes.
    """
    total_hasher = hashlib.sha256()
    total_size = 0
    chunk_index = 0
    chunks_metadata = []

    # Read stream and accumulate buffer
    buffer = bytearray()
    
    # Pre-generate file id
    import uuid
    file_id = str(uuid.uuid4())

    async for block in file_stream:
        if not block:
            continue
        buffer.extend(block)
        total_hasher.update(block)
        total_size += len(block)

        # When buffer reaches chunk size, flush chunk to storage nodes
        while len(buffer) >= chunk_size:
            chunk_data = bytes(buffer[:chunk_size])
            del buffer[:chunk_size]
            
            chunk_info = await _process_and_store_chunk(
                file_id, chunk_index, chunk_data, ring, node_urls, healthy_nodes, rf
            )
            chunks_metadata.append(chunk_info)
            chunk_index += 1

    # Flush any remaining bytes in buffer
    if len(buffer) > 0 or chunk_index == 0:
        chunk_data = bytes(buffer)
        chunk_info = await _process_and_store_chunk(
            file_id, chunk_index, chunk_data, ring, node_urls, healthy_nodes, rf
        )
        chunks_metadata.append(chunk_info)
        chunk_index += 1

    total_checksum = total_hasher.hexdigest()

    # Commit metadata to database
    file_record = FileModel(
        id=file_id,
        name=file_name,
        size_bytes=total_size,
        mime_type=content_type,
        checksum_sha256=total_checksum,
        total_chunks=chunk_index,
        chunk_size_bytes=chunk_size,
        status="ACTIVE"
    )
    db.add(file_record)
    await db.flush()

    for c_info in chunks_metadata:
        chunk_record = ChunkModel(
            id=c_info["chunk_id"],
            file_id=file_id,
            chunk_index=c_info["chunk_index"],
            size_bytes=c_info["size_bytes"],
            checksum_sha256=c_info["checksum_sha256"]
        )
        db.add(chunk_record)
        await db.flush()

        for node_id, is_primary in c_info["placements"]:
            replica_record = ChunkReplicaModel(
                chunk_id=c_info["chunk_id"],
                node_id=node_id,
                is_primary=is_primary,
                status="ONLINE"
            )
            db.add(replica_record)

    await db.commit()
    logger.info(f"File '{file_name}' ({total_size} bytes, {chunk_index} chunks) committed successfully.")
    
    # Publish UI event
    await state_client.publish_event("cluster:events", "FILE_UPLOADED", {
        "file_id": file_id,
        "name": file_name,
        "size_bytes": total_size,
        "total_chunks": chunk_index,
        "checksum": total_checksum
    })

    return file_record

async def _process_and_store_chunk(
    file_id: str,
    chunk_index: int,
    data: bytes,
    ring: ConsistentHashRing,
    node_urls: Dict[str, str],
    healthy_nodes: set[str],
    rf: int
) -> dict:
    chunk_id = f"{file_id}_chunk_{chunk_index:04d}"
    chunk_hasher = hashlib.sha256(data)
    chunk_sha = chunk_hasher.hexdigest()

    # Determine placement
    placed_nodes = ring.get_placement(chunk_id, count=rf, healthy_nodes=healthy_nodes)
    if not placed_nodes:
        # Fallback to any node if ring filtered all
        placed_nodes = list(node_urls.keys())[:rf]

    placements = []
    upload_tasks = []

    async with httpx.AsyncClient(timeout=15.0) as client:
        for i, node_id in enumerate(placed_nodes):
            is_primary = (i == 0)
            placements.append((node_id, is_primary))
            node_url = node_urls.get(node_id)
            if node_url:
                target_url = f"{node_url}/chunks/{chunk_id}"
                upload_tasks.append(client.put(target_url, content=data))

        # Concurrently send chunk to all assigned replica nodes
        responses = await asyncio.gather(*upload_tasks, return_exceptions=True)
        for node_id, resp in zip(placed_nodes, responses):
            if isinstance(resp, Exception) or resp.status_code != 200:
                logger.error(f"Failed writing chunk {chunk_id} to {node_id}: {resp}")
            else:
                logger.info(f"Wrote chunk {chunk_id} to {node_id} (primary={node_id == placed_nodes[0]})")

    # Broadcast real-time chunk dispatch for visualizer UI
    await state_client.publish_event("cluster:events", "CHUNK_DISPATCHED", {
        "chunk_id": chunk_id,
        "chunk_index": chunk_index,
        "size_bytes": len(data),
        "primary_node": placed_nodes[0] if placed_nodes else "unknown",
        "replica_nodes": placed_nodes[1:] if len(placed_nodes) > 1 else [],
        "checksum": chunk_sha[:12]
    })

    return {
        "chunk_id": chunk_id,
        "chunk_index": chunk_index,
        "size_bytes": len(data),
        "checksum_sha256": chunk_sha,
        "placements": placements
    }

async def resilient_download_generator(
    file_id: str,
    chunks: List[ChunkModel],
    node_urls: Dict[str, str],
    db: AsyncSession
) -> AsyncGenerator[bytes, None]:
    """
    Zero-Downtime Streaming Download Generator:
    Streams chunks in order. For each chunk, attempts reading from Primary node.
    If the Primary node crashes, times out, or returns 5xx:
    - Catches exception
    - Publishes LIVE FAILOVER alert to Redis for the UI
    - Immediately falls back to replica node and streams seamlessly!
    """
    for chunk in chunks:
        # Load replica nodes for this chunk ordered by is_primary DESC
        res = await db.execute(
            select(ChunkReplicaModel)
            .where(ChunkReplicaModel.chunk_id == chunk.id)
            .order_by(ChunkReplicaModel.is_primary.desc())
        )
        replicas = res.scalars().all()
        candidate_nodes = [r.node_id for r in replicas]

        chunk_delivered = False
        last_error = None

        for idx, node_id in enumerate(candidate_nodes):
            node_url = node_urls.get(node_id)
            if not node_url:
                continue

            target_url = f"{node_url}/chunks/{chunk.id}"
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    async with client.stream("GET", target_url) as resp:
                        if resp.status_code == 200:
                            async for byte_slice in resp.aiter_bytes():
                                yield byte_slice
                            chunk_delivered = True
                            break # Chunk succeeded, move to next chunk
                        else:
                            raise httpx.HTTPStatusError(f"HTTP {resp.status_code}", request=resp.request, response=resp)
            except Exception as e:
                last_error = e
                logger.warning(
                    f"⚠️ [FAILOVER] Node {node_id} failed while streaming chunk {chunk.id} ({e}). "
                    f"Switching to next replica!"
                )
                # Broadcast failover event to UI visualizer
                fallback_target = candidate_nodes[idx + 1] if idx + 1 < len(candidate_nodes) else "NONE"
                await state_client.publish_event("cluster:events", "FAILOVER_TRIGGERED", {
                    "file_id": file_id,
                    "chunk_id": chunk.id,
                    "chunk_index": chunk.chunk_index,
                    "failed_node": node_id,
                    "fallback_node": fallback_target,
                    "error": str(e)
                })

        if not chunk_delivered:
            logger.critical(f"All replicas failed for chunk {chunk.id}!")
            raise HTTPException(status_code=500, detail=f"All storage nodes for chunk {chunk.id} are unreachable.")
