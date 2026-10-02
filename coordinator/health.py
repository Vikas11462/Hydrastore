import asyncio
import logging
from datetime import datetime, timezone
from sqlalchemy import select, update

from core.config import settings
from core.database import async_session_factory
from core.models import StorageNodeModel
from core.redis_client import state_client
from core.placement import ConsistentHashRing

logger = logging.getLogger("coordinator.health")

class HealthMonitor:
    def __init__(self, ring: ConsistentHashRing, node_urls: dict[str, str]):
        self.ring = ring
        self.node_urls = node_urls
        self.healthy_nodes: set[str] = set(node_urls.keys())
        self._task: asyncio.Task = None

    async def start(self):
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info("Heartbeat and Health Monitor daemon started.")

    async def stop(self):
        if self._task:
            self._task.cancel()

    async def _monitor_loop(self):
        while True:
            try:
                await self.check_all_nodes()
                await asyncio.sleep(settings.HEARTBEAT_INTERVAL_SEC)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
                await asyncio.sleep(2)

    async def check_all_nodes(self):
        """Cross-checks Redis heartbeats against registered nodes and updates PostgreSQL."""
        active_heartbeats = await state_client.get_all_active_heartbeats()
        
        async with async_session_factory() as session:
            res = await session.execute(select(StorageNodeModel))
            db_nodes = res.scalars().all()
            
            # Map of db nodes
            db_map = {n.id: n for n in db_nodes}

            # Register any missing nodes into DB
            for node_id, url in self.node_urls.items():
                if node_id not in db_map:
                    port = int(url.split(":")[-1].replace("/", ""))
                    new_node = StorageNodeModel(
                        id=node_id,
                        host="127.0.0.1",
                        port=port,
                        capacity_bytes=1073741824,
                        used_bytes=0,
                        status="HEALTHY"
                    )
                    session.add(new_node)
                    db_map[node_id] = new_node

            changed = False
            for node_id, node_record in db_map.items():
                hb = active_heartbeats.get(node_id)
                previous_status = node_record.status

                if hb and hb.get("status") == "HEALTHY":
                    node_record.status = "HEALTHY"
                    node_record.used_bytes = hb.get("used_bytes", node_record.used_bytes)
                    node_record.last_heartbeat = datetime.now(timezone.utc)
                    self.healthy_nodes.add(node_id)

                    if previous_status != "HEALTHY":
                        logger.info(f"🟢 Node [{node_id}] recovered! Status -> HEALTHY")
                        changed = True
                        await state_client.publish_event("cluster:events", "NODE_RECOVERED", {
                            "node_id": node_id,
                            "url": self.node_urls.get(node_id)
                        })
                else:
                    node_record.status = "DEAD"
                    if node_id in self.healthy_nodes:
                        self.healthy_nodes.discard(node_id)
                        logger.warning(f"🔴 Node [{node_id}] missed heartbeat! Status -> DEAD")
                        changed = True
                        await state_client.publish_event("cluster:events", "NODE_DIED", {
                            "node_id": node_id,
                            "url": self.node_urls.get(node_id)
                        })

            await session.commit()
            
            # Update hash ring active nodes
            self.ring.set_nodes(list(self.node_urls.keys()))
