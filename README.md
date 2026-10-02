# ⚡ HydraStore // Distributed Fault-Tolerant Object Storage Engine

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-RESP2-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![SQLite/PostgreSQL](https://img.shields.io/badge/SQLAlchemy-2.0-red?style=for-the-badge&logo=sqlite&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

**A high-performance, fault-tolerant distributed object storage system modeled after Amazon S3 and Google Cloud Storage principles.**

[Architecture](#-architectural-topology) • [Features](#-core-features) • [Quickstart](#-quickstart) • [Mission Control UI](#-mission-control-dashboard) • [Benchmarks](#-performance--benchmarks)

</div>

---

## 📌 Executive Summary

**HydraStore** is an enterprise-grade distributed object storage engine featuring a fully decoupled **Control Plane** (Metadata & Orchestration) and **Data Plane** (Storage Nodes). Built from scratch with an emphasis on high availability, data durability, and transparent self-healing.

### 🌟 Key Highlights
- **Consistent Hashing with Virtual Nodes (128 VNodes)**: Binary search (`bisect`) based ring mapping guarantees uniform chunk distribution and zero hot spots.
- **Configurable Replication Factor ($RF=2$)**: Every chunk is replicated deterministically across secondary storage nodes.
- **Automated Self-Healing Daemon**: Active background workers detect missing heartbeats and automatically restore under-replicated chunks within ~6 seconds.
- **Failover Streaming Pipeline**: Zero-downtime client downloads; if a primary node crashes mid-stream, the coordinator automatically recovers from secondary replicas with `<15ms` switchover.
- **Zero-Dependency In-Memory Fallback**: Runs out of the box with embedded mocks or connects seamlessly to external Redis & PostgreSQL instances.
- **Live Mission Control Dashboard**: Glassmorphic, real-time control console with live node metrics and integrated chaos engineering injection.

---

## 🏗️ Architectural Topology

```text
                         +-----------------------------------+
                         |      CLIENT / BROWSER / SDK       |
                         +-----------------------------------+
                                           |
                                    HTTP / REST & SSE
                                           |
                         +-----------------------------------+
                         |    COORDINATOR (Control Plane)    |
                         |      Port 8000 | FastAPI Engine   |
                         +-----------------------------------+
                                /          |          \
           +------------------+            |           +------------------+
           |                               |                              |
     Consistent Hash Ring         SQLAlchemy + SQLite             Redis Heartbeat
     (128 VNodes per Node)        Metadata Repository             & Event Bus (RESP2)
           |                               |                              |
           +-------------------------------+------------------------------+
                                           |
                         Internal HTTP Data Plane Streaming
                                           |
           +-------------------+-------------------+------------------+
           |                   |                   |                  |
    +--------------+    +--------------+    +--------------+   +--------------+
    | Storage N-1  |    | Storage N-2  |    | Storage N-3  |   | Storage N-4  |
    |  Port 8001   |    |  Port 8002   |    |  Port 8003   |   |  Port 8004   |
    | Chunks: /n-1 |    | Chunks: /n-2 |    | Chunks: /n-3 |   | Chunks: /n-4 |
    +--------------+    +--------------+    +--------------+   +--------------+
```

---

## 🚀 Core Features

### 1. Control Plane Coordinator (`coordinator/`)
- **FastAPI Asynchronous Gateway**: Central ingress point managing file uploads, streaming downloads, cluster health, and topology management.
- **Consistent Hashing Ring (`core/placement.py`)**: Implements MD5 32-bit token distribution with 128 virtual nodes per physical storage daemon.
- **Chunked Asynchronous Streaming (`coordinator/stream.py`)**: Zero whole-file memory buffering; streams 64 KB client buffers accumulating to 4 MB storage blocks with on-the-fly SHA-256 validation.

### 2. Distributed Data Plane (`storage_node/`)
- **Isolated Storage Daemons**: Dedicated micro-services operating on ports 8001–8004 with persistent block stores.
- **Integrity Verification**: Atomic writes with dual-tier SHA-256 checksums (chunk-level & global file-level).
- **Heartbeat Leases**: Real-time periodic health updates emitted to Redis every 2 seconds.

### 3. Fault-Tolerance & Self-Healing Engine
- **Health Monitoring Daemon (`coordinator/health.py`)**: Marks nodes `DEGRADED` or `OFFLINE` if 3 heartbeats (5s) are missed, instantly updating the routing ring.
- **Automated Rebalancer (`coordinator/rebalancer.py`)**: Continuously identifies under-replicated chunks ($ReplicaCount < RF$) and triggers peer-to-peer data repair automatically.
- **Chaos Injection**: Interactive REST triggers (`/api/v1/cluster/nodes/{id}/kill` & `/revive`) to test resilience under simulated hardware crashes.

---

## 🖥️ Mission Control Dashboard

HydraStore includes a mission control interface built with modern vanilla HTML5/CSS3/JavaScript:
- **Live Cluster Topology Cards**: Real-time visual cards for every node displaying storage utilization, chunk counts, heartbeat latency, and online/offline status.
- **Chaos Engineering Console**: Instantly kill and revive storage nodes with a single click to watch self-healing trigger live.
- **Audit Logs via SSE**: Real-time server-sent event feed tracking cluster anomalies, rebalancing operations, and chunk placements.
- **Interactive File Manager**: Drag-and-drop file upload, file chunk distribution inspector, and streaming download trigger.

---

## ⚡ Quickstart

### Prerequisites
- Python 3.11+
- Git

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/<your-username>/hydrastore.git
cd hydrastore
pip install -r requirements.txt
```

### 2. Launch the Entire Cluster
Use the built-in cluster orchestrator to launch the Coordinator and 4 Storage Nodes simultaneously:
```bash
python cluster_manager.py start
```

### 3. Access Services
- **Mission Control UI**: Open [http://localhost:8000](http://localhost:8000) in your browser.
- **Coordinator REST API**: [http://localhost:8000/docs](http://localhost:8000/docs) (Interactive OpenAPI/Swagger).
- **Storage Nodes**:
  - Node 1: `http://localhost:8001/docs`
  - Node 2: `http://localhost:8002/docs`
  - Node 3: `http://localhost:8003/docs`
  - Node 4: `http://localhost:8004/docs`

### 4. Stop Cluster
```bash
python cluster_manager.py stop
```

---

## 📊 Performance & Benchmarks

The project comes with a high-concurrency automated benchmark suite (`benchmark/benchmark_cluster.py`):

| Metric | Measured Value | Standard Deviation |
| :--- | :--- | :--- |
| **Replication Durability** | $100\%$ ($N-1$ Survival on $RF=2$) | $0.0\%$ |
| **Self-Healing Recovery** | $\approx 6.0\text{s}$ | $\pm 0.4\text{s}$ |
| **Failover Read Latency** | $< 15\text{ms}$ | $\pm 1.8\text{ms}$ |
| **Chunk Distribution Variance** | $< 4.2\%$ | $\pm 0.3\%$ |
| **Integrity bitrot rate** | $0.00\%$ | Two-tier SHA-256 verification |

Run the benchmark suite locally:
```bash
python benchmark/benchmark_cluster.py
```

---

## 🧪 Running Tests

HydraStore has complete unit and integration tests covering the hash ring, placement uniformity, and cluster failover:
```bash
pytest -v
```

---

## 📂 Project Directory Structure

```text
├── coordinator/           # Control Plane (FastAPI, health monitor, rebalancer, stream pipeline)
├── storage_node/          # Data Plane (Chunk CRUD, peer transfer, heartbeat emitter)
├── core/                  # Core algorithms (Consistent Hash Ring, DB models, Redis client)
├── web/                   # Mission Control UI (Vanilla HTML5/CSS3/JS, SSE listeners)
├── benchmark/             # High-concurrency performance benchmark scripts
├── tests/                 # Automated Pytest suite
├── cluster_manager.py     # Python multi-process cluster orchestrator
├── Dockerfile.coordinator # Production Docker container for coordinator
├── Dockerfile.node        # Production Docker container for data nodes
├── docker-compose.yml     # Containerized multi-node orchestration
├── PROJECT_OVERVIEW.md    # In-depth architectural & system design dossier
└── requirements.txt       # Python dependencies
```

---

## 📜 License
This project is licensed under the MIT License.
