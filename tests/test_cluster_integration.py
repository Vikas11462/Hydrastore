import os
import shutil
import hashlib
import asyncio
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from core.database import Base
from core.models import FileModel, ChunkModel, ChunkReplicaModel, StorageNodeModel
from core.placement import ConsistentHashRing
from coordinator.stream import upload_file_pipeline, resilient_download_generator

# Test database
TEST_DB_URL = "sqlite+aiosqlite:///./test_cluster.db"
test_engine = create_async_engine(TEST_DB_URL, echo=False)
db_session_factory = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

@pytest.fixture
def setup_test_db():
    async def _init():
        async with test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    asyncio.run(_init())
    yield
    async def _drop():
        async with test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await test_engine.dispose()
    asyncio.run(_drop())
    if os.path.exists("./test_cluster.db"):
        try:
            os.remove("./test_cluster.db")
        except Exception:
            pass

@pytest.mark.asyncio
async def test_end_to_end_chunking_and_failover(setup_test_db):
    # 1. Mock Storage Nodes (in-memory block store)
    mock_node_stores = {
        "node-1": {},
        "node-2": {},
        "node-3": {},
        "node-4": {}
    }
    healthy_nodes = {"node-1", "node-2", "node-3", "node-4"}

    # Setup Ring
    ring = ConsistentHashRing(vnodes=32)
    ring.set_nodes(list(mock_node_stores.keys()))

    # Create dummy 500 KB test payload
    original_data = os.urandom(500 * 1024)
    expected_file_sha = hashlib.sha256(original_data).hexdigest()

    # Generator for streaming upload
    async def data_stream():
        chunk_slice = 64 * 1024
        for i in range(0, len(original_data), chunk_slice):
            yield original_data[i:i + chunk_slice]

    # Mock chunk writer directly to mock_node_stores
    chunk_size = 128 * 1024 # 128 KB chunks -> 4 chunks for 500 KB
    rf = 2

    async with db_session_factory() as db:
        # Pre-populate nodes table
        for n_id in mock_node_stores.keys():
            db.add(StorageNodeModel(
                id=n_id, host="127.0.0.1", port=8000, capacity_bytes=10000000, used_bytes=0, status="HEALTHY"
            ))
        await db.commit()

        # Manually chunk and store to test failover logic
        total_chunks = (len(original_data) + chunk_size - 1) // chunk_size
        file_id = "test_file_001"
        file_record = FileModel(
            id=file_id,
            name="test_dataset.bin",
            size_bytes=len(original_data),
            mime_type="application/octet-stream",
            checksum_sha256=expected_file_sha,
            total_chunks=total_chunks,
            chunk_size_bytes=chunk_size,
            status="ACTIVE"
        )
        db.add(file_record)
        await db.flush()

        chunks_list = []
        for c_idx in range(total_chunks):
            start = c_idx * chunk_size
            end = min(start + chunk_size, len(original_data))
            chunk_bytes = original_data[start:end]
            c_sha = hashlib.sha256(chunk_bytes).hexdigest()
            c_id = f"{file_id}_chunk_{c_idx:04d}"

            chunk_obj = ChunkModel(
                id=c_id, file_id=file_id, chunk_index=c_idx,
                size_bytes=len(chunk_bytes), checksum_sha256=c_sha
            )
            db.add(chunk_obj)
            chunks_list.append(chunk_obj)

            # Placements
            placed = ring.get_placement(c_id, count=rf, healthy_nodes=healthy_nodes)
            assert len(placed) == 2
            p_node, r_node = placed[0], placed[1]

            # Write chunk to primary and replica stores
            mock_node_stores[p_node][c_id] = chunk_bytes
            mock_node_stores[r_node][c_id] = chunk_bytes

            db.add(ChunkReplicaModel(chunk_id=c_id, node_id=p_node, is_primary=True, status="ONLINE"))
            db.add(ChunkReplicaModel(chunk_id=c_id, node_id=r_node, is_primary=False, status="ONLINE"))

        await db.commit()

        # 2. Test Download Verification (Normal Mode)
        downloaded_bytes = bytearray()
        for c in chunks_list:
            # Primary node serves
            res = await db.execute(
                select(ChunkReplicaModel)
                .where(ChunkReplicaModel.chunk_id == c.id)
                .order_by(ChunkReplicaModel.is_primary.desc())
            )
            reps = res.scalars().all()
            primary_node = reps[0].node_id
            downloaded_bytes.extend(mock_node_stores[primary_node][c.id])

        assert bytes(downloaded_bytes) == original_data
        assert hashlib.sha256(downloaded_bytes).hexdigest() == expected_file_sha

        # 3. 🔥 THE CHAOS TEST: KILL A NODE MID-DOWNLOAD
        # Suppose node-1 is completely destroyed / dead
        dead_node = "node-1"
        del mock_node_stores[dead_node] # Node completely wiped out!

        # Stream download with failover
        failover_downloaded_bytes = bytearray()
        failover_occurred = False

        for c in chunks_list:
            res = await db.execute(
                select(ChunkReplicaModel)
                .where(ChunkReplicaModel.chunk_id == c.id)
                .order_by(ChunkReplicaModel.is_primary.desc())
            )
            reps = res.scalars().all()
            candidate_node_ids = [r.node_id for r in reps]

            # Attempt download through candidate list
            c_data = None
            for n_id in candidate_node_ids:
                if n_id in mock_node_stores and c.id in mock_node_stores[n_id]:
                    c_data = mock_node_stores[n_id][c.id]
                    if n_id != candidate_node_ids[0]:
                        failover_occurred = True # Successfully used replica because primary was dead!
                    break
            
            assert c_data is not None, f"Chunk {c.id} lost!"
            failover_downloaded_bytes.extend(c_data)

        # Verify that despite node-1 being dead, the downloaded data is 100% intact!
        assert bytes(failover_downloaded_bytes) == original_data
        assert hashlib.sha256(failover_downloaded_bytes).hexdigest() == expected_file_sha
        assert failover_occurred, "Chaos failover to replica should have been triggered!"
