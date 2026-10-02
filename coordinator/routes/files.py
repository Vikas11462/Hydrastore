import urllib.parse
from typing import List, Optional
import httpx
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from core.database import get_db
from core.models import FileModel, ChunkModel, ChunkReplicaModel
from core.config import settings
from coordinator.stream import upload_file_pipeline, resilient_download_generator

router = APIRouter(prefix="/api/v1/files", tags=["files"])

# Injected by main coordinator app
node_urls = settings.parse_storage_nodes()

@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Stream upload a file:
    Slices into chunks, places across nodes via consistent hashing,
    replicates to secondary nodes, and records metadata in PostgreSQL.
    """
    from coordinator.main import app_state

    async def file_byte_generator():
        while chunk := await file.read(64 * 1024):
            yield chunk

    try:
        file_record = await upload_file_pipeline(
            file_name=file.filename or "untitled",
            file_stream=file_byte_generator(),
            content_type=file.content_type or "application/octet-stream",
            db=db,
            ring=app_state.ring,
            node_urls=node_urls,
            healthy_nodes=app_state.health_monitor.healthy_nodes,
            chunk_size=settings.DEFAULT_CHUNK_SIZE,
            rf=settings.REPLICATION_FACTOR
        )

        return {
            "status": "success",
            "file_id": file_record.id,
            "name": file_record.name,
            "size_bytes": file_record.size_bytes,
            "total_chunks": file_record.total_chunks,
            "checksum_sha256": file_record.checksum_sha256
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@router.get("")
async def list_files(db: AsyncSession = Depends(get_db)):
    """List all stored files."""
    res = await db.execute(
        select(FileModel).where(FileModel.is_deleted == False).order_by(FileModel.created_at.desc())
    )
    files = res.scalars().all()
    return [
        {
            "id": f.id,
            "name": f.name,
            "size_bytes": f.size_bytes,
            "mime_type": f.mime_type,
            "total_chunks": f.total_chunks,
            "checksum_sha256": f.checksum_sha256,
            "created_at": f.created_at.isoformat() if f.created_at else None
        }
        for f in files
    ]

@router.get("/{file_id}")
async def get_file_details(file_id: str, db: AsyncSession = Depends(get_db)):
    """Get file metadata and detailed chunk topology map."""
    res = await db.execute(
        select(FileModel)
        .options(selectinload(FileModel.chunks).selectinload(ChunkModel.replicas))
        .where(FileModel.id == file_id, FileModel.is_deleted == False)
    )
    file_record = res.scalar_one_or_none()
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    chunk_list = []
    for c in file_record.chunks:
        replicas_info = [
            {"node_id": r.node_id, "is_primary": r.is_primary, "status": r.status}
            for r in c.replicas
        ]
        chunk_list.append({
            "chunk_id": c.id,
            "chunk_index": c.chunk_index,
            "size_bytes": c.size_bytes,
            "checksum_sha256": c.checksum_sha256,
            "replicas": replicas_info
        })

    return {
        "id": file_record.id,
        "name": file_record.name,
        "size_bytes": file_record.size_bytes,
        "mime_type": file_record.mime_type,
        "total_chunks": file_record.total_chunks,
        "checksum_sha256": file_record.checksum_sha256,
        "created_at": file_record.created_at.isoformat() if file_record.created_at else None,
        "chunks": chunk_list
    }

@router.get("/{file_id}/download")
async def download_file(file_id: str, db: AsyncSession = Depends(get_db)):
    """
    Stream download file with zero-downtime failover:
    If any storage node fails or is killed mid-stream,
    the coordinator transparently switches to its replica node.
    """
    res = await db.execute(
        select(FileModel)
        .options(selectinload(FileModel.chunks).selectinload(ChunkModel.replicas))
        .where(FileModel.id == file_id, FileModel.is_deleted == False)
    )
    file_record = res.scalar_one_or_none()
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    # PRE-FLIGHT AVAILABILITY CHECK:
    # Ensure at least one replica node is currently alive for EVERY chunk before committing HTTP 200 headers!
    from coordinator.main import app_state
    healthy_nodes = app_state.health_monitor.healthy_nodes if (app_state and app_state.health_monitor) else set()

    for chunk in file_record.chunks:
        alive_chunk_replicas = [r.node_id for r in chunk.replicas if r.node_id in healthy_nodes]
        if not alive_chunk_replicas:
            raise HTTPException(
                status_code=503,
                detail=f"Storage Cluster Unavailable: All storage nodes holding chunk #{chunk.chunk_index} are offline. System cannot serve this object until at least one replica node is revived."
            )

    # Safe RFC 5987 / URL-encoded filename for Content-Disposition header
    safe_ascii_name = file_record.name.encode('ascii', 'ignore').decode('ascii') or "downloaded_file"
    encoded_utf8_name = urllib.parse.quote(file_record.name)

    return StreamingResponse(
        resilient_download_generator(
            file_id=file_record.id,
            chunks=file_record.chunks,
            node_urls=node_urls,
            db=db
        ),
        media_type=file_record.mime_type,
        headers={
            "Content-Disposition": f'attachment; filename="{safe_ascii_name}"; filename*=UTF-8\'\'{encoded_utf8_name}',
            "Content-Length": str(file_record.size_bytes),
            "X-Hydra-Checksum": file_record.checksum_sha256
        }
    )

@router.delete("/{file_id}")
async def delete_file(file_id: str, db: AsyncSession = Depends(get_db)):
    """Soft delete file record and clean up chunks on storage nodes."""
    res = await db.execute(
        select(FileModel)
        .options(selectinload(FileModel.chunks).selectinload(ChunkModel.replicas))
        .where(FileModel.id == file_id)
    )
    file_record = res.scalar_one_or_none()
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    # Async delete chunks from nodes
    async with httpx.AsyncClient(timeout=5.0) as client:
        for chunk in file_record.chunks:
            for rep in chunk.replicas:
                node_url = node_urls.get(rep.node_id)
                if node_url:
                    try:
                        await client.delete(f"{node_url}/chunks/{chunk.id}")
                    except Exception:
                        pass

    file_record.is_deleted = True
    file_record.status = "DELETED"
    await db.commit()

    return {"status": "deleted", "file_id": file_id}
