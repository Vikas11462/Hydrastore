import os
import shutil
import hashlib
import pytest
from httpx import AsyncClient, ASGITransport

from storage_node.main import app, node_state

@pytest.fixture(autouse=True)
def setup_node_tmp():
    test_dir = "./tests/tmp_node_storage"
    node_state.storage_dir = test_dir
    node_state.is_active = True
    os.makedirs(test_dir, exist_ok=True)
    yield
    shutil.rmtree(test_dir, ignore_errors=True)

@pytest.mark.asyncio
async def test_chunk_put_get_delete():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Check health
        res = await client.get("/health")
        assert res.status_code == 200
        assert res.json()["status"] == "HEALTHY"

        # 2. Put chunk
        payload = b"Hello HydraStore Distributed Block Test! 123456789"
        expected_sha = hashlib.sha256(payload).hexdigest()
        
        chunk_id = "chunk_test_001"
        res = await client.put(f"/chunks/{chunk_id}", content=payload)
        assert res.status_code == 200
        data = res.json()
        assert data["chunk_id"] == chunk_id
        assert data["size_bytes"] == len(payload)
        assert data["checksum_sha256"] == expected_sha

        # 3. Get chunk
        res = await client.get(f"/chunks/{chunk_id}")
        assert res.status_code == 200
        assert res.content == payload

        # 4. Chaos Kill simulation
        res = await client.post("/chaos/kill")
        assert res.status_code == 200
        assert res.json()["status"] == "DEAD"

        # Request to dead node should be 503
        res = await client.get(f"/chunks/{chunk_id}")
        assert res.status_code == 503

        # Revive node
        res = await client.post("/chaos/revive")
        assert res.status_code == 200

        # 5. Delete chunk
        res = await client.delete(f"/chunks/{chunk_id}")
        assert res.status_code == 200
        assert res.json()["status"] == "deleted"

        # Verify not found
        res = await client.get(f"/chunks/{chunk_id}")
        assert res.status_code == 404
