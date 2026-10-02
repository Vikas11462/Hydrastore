import asyncio
import json
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from core.database import get_db
from core.models import StorageNodeModel, ChunkReplicaModel
from core.config import settings
from core.redis_client import state_client

router = APIRouter(prefix="/api/v1/cluster", tags=["cluster"])

node_urls = settings.parse_storage_nodes()

@router.get("/nodes")
async def get_nodes_topology(db: AsyncSession = Depends(get_db)):
    """Fetch live cluster topology and storage node metrics."""
    res = await db.execute(select(StorageNodeModel))
    nodes = res.scalars().all()

    node_data = []
    for n in nodes:
        # Count assigned chunk replicas
        rep_count_res = await db.execute(
            select(ChunkReplicaModel).where(ChunkReplicaModel.node_id == n.id)
        )
        chunk_count = len(rep_count_res.scalars().all())

        node_data.append({
            "id": n.id,
            "host": n.host,
            "port": n.port,
            "url": node_urls.get(n.id, f"http://{n.host}:{n.port}"),
            "status": n.status,
            "used_bytes": n.used_bytes,
            "capacity_bytes": n.capacity_bytes,
            "chunk_count": chunk_count,
            "last_heartbeat": n.last_heartbeat.isoformat() if n.last_heartbeat else None
        })

    return node_data

@router.post("/nodes/{node_id}/kill")
async def kill_node(node_id: str):
    """Chaos Action: Simulate node failure / crash."""
    url = node_urls.get(node_id)
    if not url:
        raise HTTPException(status_code=404, detail="Node not found")

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            await client.post(f"{url}/chaos/kill")
    except Exception:
        pass

    # Instant cluster state invalidation:
    # 1. Clear Redis heartbeat lease immediately so coordinator doesn't see stale heartbeat
    await state_client.delete(f"node:heartbeat:{node_id}")

    # 2. Immediately evict node from coordinator in-memory health monitor
    from coordinator.main import app_state
    if app_state and app_state.health_monitor:
        app_state.health_monitor.healthy_nodes.discard(node_id)

    # 3. Synchronously update database status
    from core.database import async_session_factory
    from sqlalchemy import update
    try:
        async with async_session_factory() as session:
            await session.execute(
                update(StorageNodeModel).where(StorageNodeModel.id == node_id).values(status="DEAD")
            )
            await session.commit()
    except Exception:
        pass

    await state_client.publish_event("cluster:events", "CHAOS_NODE_KILLED", {
        "node_id": node_id,
        "message": f"Storage node [{node_id}] killed by operator!"
    })
    return {"status": "success", "message": f"Node {node_id} killed"}

@router.post("/nodes/{node_id}/revive")
async def revive_node(node_id: str):
    """Chaos Action: Revive a previously killed storage node."""
    url = node_urls.get(node_id)
    if not url:
        raise HTTPException(status_code=404, detail="Node not found")

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.post(f"{url}/chaos/revive")
            if resp.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to revive node")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not reach node: {e}")

    # Instant state sync
    from coordinator.main import app_state
    if app_state and app_state.health_monitor:
        app_state.health_monitor.healthy_nodes.add(node_id)
        asyncio.create_task(app_state.health_monitor.check_all_nodes())

    from core.database import async_session_factory
    from sqlalchemy import update
    try:
        async with async_session_factory() as session:
            await session.execute(
                update(StorageNodeModel).where(StorageNodeModel.id == node_id).values(status="HEALTHY")
            )
            await session.commit()
    except Exception:
        pass

    await state_client.publish_event("cluster:events", "CHAOS_NODE_REVIVED", {
        "node_id": node_id,
        "message": f"Storage node [{node_id}] revived by operator!"
    })
    return {"status": "success", "message": f"Node {node_id} revived"}

@router.get("/events")
async def cluster_event_stream(request: Request):
    """
    Server-Sent Events (SSE) stream for real-time Mission Control UI updates:
    Broadcasts heartbeats, chunk dispatches, failover alerts, and self-healing rebalances.
    """
    queue = state_client.get_fallback_queue()

    async def event_generator():
        try:
            # Initial ping to open connection
            yield f"data: {json.dumps({'type': 'CONNECTED', 'timestamp': 0, 'payload': {'msg': 'Connected to HydraStore Event Stream'}})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    # Wait for next event or send keepalive
                    raw_msg = await asyncio.wait_for(queue.get(), timeout=10.0)
                    yield f"data: {raw_msg}\n\n"
                except asyncio.TimeoutError:
                    # Keepalive heartbeat comment to keep SSE connection alive
                    yield ": keepalive\n\n"
        finally:
            state_client.remove_fallback_queue(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
