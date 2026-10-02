import asyncio
import logging
from sqlalchemy import select
from sqlalchemy.orm import selectinload
import httpx

from core.config import settings
from core.database import async_session_factory
from core.models import ChunkModel, ChunkReplicaModel, StorageNodeModel
from core.redis_client import state_client

logger = logging.getLogger("coordinator.rebalancer")

class ReplicationWorker:
    def __init__(self, node_urls: dict[str, str], rf: int = settings.REPLICATION_FACTOR):
        self.node_urls = node_urls
        self.rf = rf
        self._task: asyncio.Task = None

    async def start(self):
        self._task = asyncio.create_task(self._rebalance_loop())
        logger.info("Automatic Self-Healing Replication Worker started.")

    async def stop(self):
        if self._task:
            self._task.cancel()

    async def _rebalance_loop(self):
        # Initial wait for nodes to register heartbeats
        await asyncio.sleep(5)
        while True:
            try:
                await self.scan_and_rebalance()
                await asyncio.sleep(6) # Check every 6 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in rebalance loop: {e}")
                await asyncio.sleep(4)

    async def scan_and_rebalance(self):
        """Scans for degraded chunks (where active replicas < RF) and triggers peer-to-peer healing."""
        async with async_session_factory() as session:
            # 1. Fetch node statuses
            res_nodes = await session.execute(select(StorageNodeModel))
            all_nodes = res_nodes.scalars().all()
            node_status_map = {n.id: n.status for n in all_nodes}
            healthy_node_ids = {n.id for n in all_nodes if n.status == "HEALTHY"}

            # If fewer than 2 healthy nodes exist, we cannot replicate to a fresh node
            if len(healthy_node_ids) < self.rf:
                return

            # 2. Fetch all chunks with their replicas
            res_chunks = await session.execute(
                select(ChunkModel).options(selectinload(ChunkModel.replicas))
            )
            chunks = res_chunks.scalars().all()

            for chunk in chunks:
                current_replicas = chunk.replicas
                alive_node_ids = set()
                dead_replica_records = []

                for r in current_replicas:
                    if node_status_map.get(r.node_id) == "HEALTHY":
                        alive_node_ids.add(r.node_id)
                    else:
                        dead_replica_records.append(r)

                # If the chunk has surviving copies, but active count is less than RF:
                if 0 < len(alive_node_ids) < self.rf:
                    logger.warning(
                        f"⚠️ Degraded durability for chunk {chunk.id}: "
                        f"Active={len(alive_node_ids)}/{self.rf}. Initiating auto-healing!"
                    )

                    # Pick a target node that is healthy and doesn't have this chunk yet
                    candidate_targets = list(healthy_node_ids - alive_node_ids)
                    if not candidate_targets:
                        continue
                    target_node_id = candidate_targets[0]

                    # Pick a healthy source node to copy from
                    source_node_id = list(alive_node_ids)[0]
                    source_url = f"{self.node_urls[source_node_id]}/chunks/{chunk.id}"
                    target_api = f"{self.node_urls[target_node_id]}/replicate"

                    try:
                        async with httpx.AsyncClient(timeout=10.0) as client:
                            resp = await client.post(
                                target_api,
                                json={"chunk_id": chunk.id, "source_url": source_url}
                            )
                            if resp.status_code == 200:
                                # Update database: Add new replica
                                new_replica = ChunkReplicaModel(
                                    chunk_id=chunk.id,
                                    node_id=target_node_id,
                                    is_primary=False,
                                    status="ONLINE"
                                )
                                session.add(new_replica)

                                # Remove the old dead replica reference
                                for dead_r in dead_replica_records:
                                    await session.delete(dead_r)

                                await session.commit()
                                logger.info(
                                    f"✨ Self-Healing complete: Chunk {chunk.id} replicated "
                                    f"{source_node_id} -> {target_node_id}. RF restored to {self.rf}!"
                                )

                                # Broadcast UI event
                                await state_client.publish_event("cluster:events", "REBALANCE_COMPLETED", {
                                    "chunk_id": chunk.id,
                                    "source_node": source_node_id,
                                    "target_node": target_node_id,
                                    "restored_rf": self.rf
                                })
                    except Exception as e:
                        logger.error(f"Failed to auto-heal chunk {chunk.id} to {target_node_id}: {e}")
                        await session.rollback()
