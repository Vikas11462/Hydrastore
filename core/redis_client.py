import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any, List

logger = logging.getLogger("hydra.redis")

class InMemoryRedisMock:
    """In-memory Redis fallback for heartbeat tracking, locks, and pub/sub events."""
    def __init__(self):
        self._store: Dict[str, tuple[str, Optional[float]]] = {}
        self._subscribers: List[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    async def ping(self) -> bool:
        return True

    async def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        async with self._lock:
            expires_at = time.time() + ex if ex else None
            self._store[key] = (value, expires_at)
            return True

    async def get(self, key: str) -> Optional[str]:
        async with self._lock:
            if key not in self._store:
                return None
            val, expires_at = self._store[key]
            if expires_at and time.time() > expires_at:
                del self._store[key]
                return None
            return val

    async def delete(self, key: str) -> int:
        async with self._lock:
            if key in self._store:
                del self._store[key]
                return 1
            return 0

    async def keys(self, pattern: str = "*") -> List[str]:
        async with self._lock:
            now = time.time()
            valid_keys = []
            expired = []
            prefix = pattern.replace("*", "")
            for k, (v, exp) in self._store.items():
                if exp and now > exp:
                    expired.append(k)
                elif k.startswith(prefix):
                    valid_keys.append(k)
            for k in expired:
                del self._store[k]
            return valid_keys

    async def publish(self, channel: str, message: str) -> int:
        async with self._lock:
            dead_queues = []
            for q in self._subscribers:
                try:
                    q.put_nowait(message)
                except asyncio.QueueFull:
                    pass
                except Exception:
                    dead_queues.append(q)
            for dq in dead_queues:
                self._subscribers.remove(dq)
            return len(self._subscribers)

    def subscribe_queue(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=100)
        self._subscribers.append(q)
        return q

    def unsubscribe_queue(self, q: asyncio.Queue):
        if q in self._subscribers:
            self._subscribers.remove(q)


class DistributedStateClient:
    """Wrapper that communicates with real Redis (RESP2) or fallback to InMemoryRedisMock."""
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._redis = None
        self._fallback = InMemoryRedisMock()
        self.use_fallback = False

    async def connect(self):
        try:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(
                self.redis_url, 
                decode_responses=True,
                protocol=2, # Required for Windows Redis 3.x compatibility
                socket_timeout=2.0
            )
            await self._redis.ping()
            self.use_fallback = False
            logger.info("Connected to Redis server successfully (RESP2).")
        except Exception as e:
            logger.warning(f"Redis not reachable ({e}). Using robust In-Memory State Engine fallback.")
            self.use_fallback = True

    async def ping(self) -> bool:
        if not self.use_fallback and self._redis:
            try:
                return await self._redis.ping()
            except Exception:
                self.use_fallback = True
        return await self._fallback.ping()

    async def set_heartbeat(self, node_id: str, data: dict, ttl_sec: int = 5):
        key = f"node:heartbeat:{node_id}"
        val = json.dumps(data)
        if not self.use_fallback and self._redis:
            try:
                await self._redis.set(key, val, ex=ttl_sec)
                return
            except Exception as e:
                logger.warning(f"Redis error on set_heartbeat: {e}. Falling back.")
                self.use_fallback = True
        await self._fallback.set(key, val, ex=ttl_sec)

    async def get_heartbeat(self, node_id: str) -> Optional[dict]:
        key = f"node:heartbeat:{node_id}"
        val = None
        if not self.use_fallback and self._redis:
            try:
                val = await self._redis.get(key)
            except Exception:
                self.use_fallback = True
        if val is None:
            val = await self._fallback.get(key)
        return json.loads(val) if val else None

    async def delete(self, key: str) -> int:
        if not self.use_fallback and self._redis:
            try:
                return await self._redis.delete(key)
            except Exception:
                self.use_fallback = True
        return await self._fallback.delete(key)

    async def delete_heartbeat(self, node_id: str) -> int:
        return await self.delete(f"node:heartbeat:{node_id}")

    async def get_all_active_heartbeats(self) -> Dict[str, dict]:
        """Returns dict of node_id -> heartbeat_dict for all nodes currently beating."""
        results = {}
        if not self.use_fallback and self._redis:
            try:
                keys = await self._redis.keys("node:heartbeat:*")
                for k in keys:
                    node_id = k.split(":")[-1]
                    val = await self._redis.get(k)
                    if val:
                        results[node_id] = json.loads(val)
                return results
            except Exception:
                self.use_fallback = True

        keys = await self._fallback.keys("node:heartbeat:*")
        for k in keys:
            node_id = k.split(":")[-1]
            val = await self._fallback.get(k)
            if val:
                results[node_id] = json.loads(val)
        return results

    async def publish_event(self, channel: str, event_type: str, payload: dict):
        msg = json.dumps({
            "type": event_type,
            "timestamp": time.time(),
            "payload": payload
        })
        if not self.use_fallback and self._redis:
            try:
                await self._redis.publish(channel, msg)
            except Exception:
                self.use_fallback = True
        await self._fallback.publish(channel, msg)

    def get_fallback_queue(self) -> asyncio.Queue:
        return self._fallback.subscribe_queue()

    def remove_fallback_queue(self, q: asyncio.Queue):
        self._fallback.unsubscribe_queue(q)

    async def close(self):
        if self._redis:
            await self._redis.aclose()

# Global state client instance
from core.config import settings
state_client = DistributedStateClient(settings.REDIS_URL)
