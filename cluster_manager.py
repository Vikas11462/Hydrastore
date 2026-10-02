"""
HydraStore Cluster Orchestrator
Enables running the full multi-node distributed cluster locally on Windows/macOS/Linux
without requiring Docker Desktop, while also supporting chaos process management.
"""

import os
import sys
import time
import subprocess
import signal
import argparse
from typing import Dict

PYTHON_EXE = sys.executable
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

NODES = [
    {"id": "node-1", "port": 8001, "dir": os.path.join(BASE_DIR, "data_nodes", "node-1", "chunks")},
    {"id": "node-2", "port": 8002, "dir": os.path.join(BASE_DIR, "data_nodes", "node-2", "chunks")},
    {"id": "node-3", "port": 8003, "dir": os.path.join(BASE_DIR, "data_nodes", "node-3", "chunks")},
    {"id": "node-4", "port": 8004, "dir": os.path.join(BASE_DIR, "data_nodes", "node-4", "chunks")},
]

processes: Dict[str, subprocess.Popen] = {}

def start_storage_node(node_cfg):
    node_id = node_cfg["id"]
    port = node_cfg["port"]
    s_dir = node_cfg["dir"]
    os.makedirs(s_dir, exist_ok=True)

    cmd = [
        PYTHON_EXE, "-m", "storage_node.main",
        "--node-id", node_id,
        "--port", str(port),
        "--storage-dir", s_dir
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR

    p = subprocess.Popen(cmd, cwd=BASE_DIR, env=env)
    processes[node_id] = p
    print(f"  [+] Started Storage Node [{node_id}] on http://127.0.0.1:{port} (PID: {p.pid})")

def start_coordinator():
    cmd = [
        PYTHON_EXE, "-m", "coordinator.main"
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR

    p = subprocess.Popen(cmd, cwd=BASE_DIR, env=env)
    processes["coordinator"] = p
    print(f"  [+] Started Storage Coordinator on http://127.0.0.1:8000 (PID: {p.pid})")

def stop_all():
    print("\nShutting down HydraStore cluster...")
    for name, p in list(processes.items()):
        try:
            print(f"  [-] Terminating {name} (PID: {p.pid})...")
            p.terminate()
            p.wait(timeout=2)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass
    print("All cluster services stopped cleanly.")

def main():
    parser = argparse.ArgumentParser(description="HydraStore Cluster Manager")
    parser.add_argument("action", choices=["start", "status"], default="start", nargs="?")
    args = parser.parse_args()

    if args.action == "start":
        print("=" * 70)
        print("   HYDRASTORE // DISTRIBUTED OBJECT STORAGE CLUSTER")
        print("   Control Plane: http://127.0.0.1:8000")
        print("   Storage Nodes: Ports 8001, 8002, 8003, 8004")
        print("   Mission Control UI: http://127.0.0.1:8000/")
        print("=" * 70)

        # 1. Start all 4 storage nodes
        print("\n[Phase 1] Launching Data Plane (4 Storage Nodes)...")
        for node in NODES:
            start_storage_node(node)
            time.sleep(0.3)

        # 2. Start Coordinator
        print("\n[Phase 2] Launching Control Plane (Coordinator)...")
        start_coordinator()

        print("\n[+] Cluster is ONLINE and ready!")
        print("    Open http://127.0.0.1:8000 in your browser to view Mission Control.")
        print("    Press Ctrl+C to terminate the cluster.\n")

        def sig_handler(signum, frame):
            stop_all()
            sys.exit(0)

        signal.signal(signal.SIGINT, sig_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, sig_handler)

        try:
            while True:
                time.sleep(1)
                # Health check child processes
                for name, p in list(processes.items()):
                    if p.poll() is not None:
                        print(f"⚠️ Service [{name}] exited unexpectedly with code {p.returncode}")
        except KeyboardInterrupt:
            stop_all()

if __name__ == "__main__":
    main()
