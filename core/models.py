import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Integer, BigInteger, Boolean, DateTime, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship
from core.database import Base

def utc_now():
    return datetime.now(timezone.utc)

class StorageNodeModel(Base):
    __tablename__ = "storage_nodes"

    id = Column(String(50), primary_key=True) # e.g. "node-1"
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    capacity_bytes = Column(BigInteger, default=1073741824) # 1 GB default
    used_bytes = Column(BigInteger, default=0)
    status = Column(String(20), default="HEALTHY") # HEALTHY, DEGRADED, DEAD
    last_heartbeat = Column(DateTime, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, default=utc_now)

    replicas = relationship("ChunkReplicaModel", back_populates="node", cascade="all, delete-orphan")


class FileModel(Base):
    __tablename__ = "files"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(255), nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    mime_type = Column(String(100), default="application/octet-stream")
    checksum_sha256 = Column(String(64), nullable=False)
    total_chunks = Column(Integer, nullable=False)
    chunk_size_bytes = Column(Integer, nullable=False)
    status = Column(String(20), default="UPLOADING") # UPLOADING, ACTIVE, DELETED
    is_deleted = Column(Boolean, default=False)
    created_at = Column(DateTime, default=utc_now)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)

    chunks = relationship("ChunkModel", back_populates="file", cascade="all, delete-orphan", order_by="ChunkModel.chunk_index")


class ChunkModel(Base):
    __tablename__ = "chunks"

    id = Column(String(64), primary_key=True) # e.g. "{file_id}_chunk_{idx}" or UUID
    file_id = Column(String(36), ForeignKey("files.id", ondelete="CASCADE"), nullable=False)
    chunk_index = Column(Integer, nullable=False)
    size_bytes = Column(Integer, nullable=False)
    checksum_sha256 = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=utc_now)

    file = relationship("FileModel", back_populates="chunks")
    replicas = relationship("ChunkReplicaModel", back_populates="chunk", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("file_id", "chunk_index", name="uq_file_chunk_index"),
        Index("idx_chunks_file_id", "file_id"),
    )


class ChunkReplicaModel(Base):
    __tablename__ = "chunk_replicas"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    chunk_id = Column(String(64), ForeignKey("chunks.id", ondelete="CASCADE"), nullable=False)
    node_id = Column(String(50), ForeignKey("storage_nodes.id", ondelete="CASCADE"), nullable=False)
    is_primary = Column(Boolean, default=False)
    status = Column(String(20), default="ONLINE") # ONLINE, DEGRADED, OFFLINE
    created_at = Column(DateTime, default=utc_now)

    chunk = relationship("ChunkModel", back_populates="replicas")
    node = relationship("StorageNodeModel", back_populates="replicas")

    __table_args__ = (
        UniqueConstraint("chunk_id", "node_id", name="uq_chunk_node"),
        Index("idx_replicas_node_id", "node_id"),
        Index("idx_replicas_chunk_id", "chunk_id"),
    )
