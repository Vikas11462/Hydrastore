import pytest
from core.placement import ConsistentHashRing

def test_ring_distribution():
    ring = ConsistentHashRing(vnodes=64)
    nodes = ["node-1", "node-2", "node-3", "node-4"]
    ring.set_nodes(nodes)

    # Place 100 test chunks and check distribution
    allocations = {n: 0 for n in nodes}
    for i in range(100):
        chunk_id = f"test_file_chunk_{i}"
        placed = ring.get_placement(chunk_id, count=2)
        assert len(placed) == 2
        assert placed[0] != placed[1] # Primary and Replica must be distinct physical nodes
        allocations[placed[0]] += 1

    # Ensure every node received at least some primary chunks
    for n in nodes:
        assert allocations[n] > 5, f"Node {n} received too few chunks: {allocations[n]}"

def test_ring_node_removal():
    ring = ConsistentHashRing(vnodes=64)
    ring.set_nodes(["node-1", "node-2", "node-3", "node-4"])

    # Exclude node-1 from healthy set
    healthy = {"node-2", "node-3", "node-4"}
    for i in range(20):
        chunk_id = f"sample_chunk_{i}"
        placed = ring.get_placement(chunk_id, count=2, healthy_nodes=healthy)
        assert "node-1" not in placed
        assert len(placed) == 2
