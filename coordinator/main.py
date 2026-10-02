import os
import sys
from pathlib import Path

# Ensure project root is in sys.path for direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from core.config import settings
from core.database import init_db
from core.redis_client import state_client
from core.placement import ConsistentHashRing
from coordinator.health import HealthMonitor
from coordinator.rebalancer import ReplicationWorker
from coordinator.routes.files import router as files_router
from coordinator.routes.cluster import router as cluster_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s")
logger = logging.getLogger("coordinator")

class CoordinatorAppState:
    def __init__(self):
        self.node_urls = settings.parse_storage_nodes()
        self.ring = ConsistentHashRing(vnodes=128)
        self.health_monitor: HealthMonitor = None
        self.rebalancer: ReplicationWorker = None

app_state = CoordinatorAppState()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing HydraStore Coordinator...")
    
    # 1. Connect Redis / State Engine
    await state_client.connect()

    # 2. Initialize Database Tables
    await init_db()

    # 3. Setup Hash Ring with initial nodes
    app_state.ring.set_nodes(list(app_state.node_urls.keys()))

    # 4. Start Health Monitor Daemon
    app_state.health_monitor = HealthMonitor(app_state.ring, app_state.node_urls)
    await app_state.health_monitor.start()

    # 5. Start Self-Healing Rebalance Worker
    app_state.rebalancer = ReplicationWorker(app_state.node_urls, rf=settings.REPLICATION_FACTOR)
    await app_state.rebalancer.start()

    logger.info("HydraStore Coordinator is ONLINE on port %s", settings.COORDINATOR_PORT)
    yield

    # Teardown
    logger.info("Stopping HydraStore Coordinator...")
    if app_state.health_monitor:
        await app_state.health_monitor.stop()
    if app_state.rebalancer:
        await app_state.rebalancer.stop()
    await state_client.close()

app = FastAPI(
    title="HydraStore Coordinator API",
    description="Fault-Tolerant Distributed Object Storage System (Mini S3)",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routers
app.include_router(files_router)
app.include_router(cluster_router)

# Mount Web Visualizer UI
web_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web"))
if os.path.isdir(web_dir):
    app.mount("/static", StaticFiles(directory=web_dir), name="static")

    @app.get("/", include_in_schema=False)
    async def serve_index():
        return FileResponse(os.path.join(web_dir, "index.html"))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("coordinator.main:app", host=settings.COORDINATOR_HOST, port=settings.COORDINATOR_PORT, reload=False)
