import os
from typing import List, Dict
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "HydraStore Distributed Storage"
    COORDINATOR_HOST: str = "0.0.0.0"
    COORDINATOR_PORT: int = 8000
    
    # 4MB chunk size default for fast demos and tests
    DEFAULT_CHUNK_SIZE: int = 4 * 1024 * 1024  # 4 MB
    REPLICATION_FACTOR: int = 2
    
    # SQLite async for single-box zero-setup run, or postgresql+asyncpg://...
    DATABASE_URL: str = "sqlite+aiosqlite:///hydrastore_metadata.db"
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # Storage Nodes: id:url pairs
    STORAGE_NODES_CONFIG: str = (
        "node-1:http://127.0.0.1:8001,"
        "node-2:http://127.0.0.1:8002,"
        "node-3:http://127.0.0.1:8003,"
        "node-4:http://127.0.0.1:8004"
    )
    
    STORAGE_BASE_DIR: str = "./data_nodes"
    
    # Heartbeat interval & dead threshold
    HEARTBEAT_INTERVAL_SEC: int = 2
    NODE_DEAD_TIMEOUT_SEC: int = 5
    
    def parse_storage_nodes(self) -> Dict[str, str]:
        """Returns dict of node_id -> base_url"""
        nodes = {}
        for entry in self.STORAGE_NODES_CONFIG.split(","):
            if ":" in entry:
                parts = entry.strip().split(":", 1)
                node_id = parts[0]
                url = parts[1]
                nodes[node_id] = url
        return nodes

    model_config = {"env_file": ".env", "extra": "ignore"}

settings = Settings()
