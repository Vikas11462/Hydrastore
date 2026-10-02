import bisect
import hashlib
from typing import List, Dict, Set, Optional

class ConsistentHashRing:
    """
    Consistent Hash Ring with Virtual Nodes (VNodes) for even chunk distribution 
    and deterministic replica assignment across distributed storage nodes.
    """
    def __init__(self, vnodes: int = 128):
        self.vnodes = vnodes
        self.ring: List[int] = [] # Sorted list of virtual node hash keys
        self.ring_map: Dict[int, str] = {} # hash_key -> physical_node_id
        self.nodes: Set[str] = set()

    def _hash(self, key: str) -> int:
        """MD5 32-bit integer hash for fast and uniform ring distribution."""
        digest = hashlib.md5(key.encode("utf-8")).hexdigest()
        return int(digest[:8], 16)

    def add_node(self, node_id: str):
        """Add a physical node with multiple virtual tokens on the ring."""
        if node_id in self.nodes:
            return
        self.nodes.add(node_id)
        for i in range(self.vnodes):
            vnode_key = f"{node_id}#vnode{i}"
            h = self._hash(vnode_key)
            self.ring_map[h] = node_id
            bisect.insort(self.ring, h)

    def remove_node(self, node_id: str):
        """Remove a physical node and its virtual nodes from the ring."""
        if node_id not in self.nodes:
            return
        self.nodes.remove(node_id)
        for i in range(self.vnodes):
            vnode_key = f"{node_id}#vnode{i}"
            h = self._hash(vnode_key)
            if h in self.ring_map:
                del self.ring_map[h]
                idx = bisect.bisect_left(self.ring, h)
                if idx < len(self.ring) and self.ring[idx] == h:
                    del self.ring[idx]

    def set_nodes(self, node_ids: List[str]):
        """Reset and set active nodes."""
        self.ring.clear()
        self.ring_map.clear()
        self.nodes.clear()
        for node_id in node_ids:
            self.add_node(node_id)

    def get_placement(self, chunk_id: str, count: int = 2, healthy_nodes: Optional[Set[str]] = None) -> List[str]:
        """
        Returns an ordered list of [primary_node_id, replica_1, replica_2, ...]
        ensuring distinct physical nodes are chosen clockwise along the ring.
        """
        if not self.ring:
            return []

        h = self._hash(chunk_id)
        idx = bisect.bisect_right(self.ring, h)
        if idx >= len(self.ring):
            idx = 0

        selected_nodes: List[str] = []
        seen_nodes: Set[str] = set()

        # Traverse ring clockwise
        total_ring_len = len(self.ring)
        for step in range(total_ring_len):
            curr_idx = (idx + step) % total_ring_len
            node_id = self.ring_map[self.ring[curr_idx]]

            if healthy_nodes is not None and node_id not in healthy_nodes:
                continue

            if node_id not in seen_nodes:
                seen_nodes.add(node_id)
                selected_nodes.append(node_id)
                if len(selected_nodes) == count:
                    break

        return selected_nodes
