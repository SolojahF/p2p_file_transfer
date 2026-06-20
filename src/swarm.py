"""Swarm: the set of peers that share a particular file."""


class Swarm:
    """
    Tracks all DataPeerNode instances that hold a specific file.

    Supports membership testing by peer_id string or by peer object,
    length queries, and iteration over the peer set.
    """

    def __init__(self, file_hash: str):
        """
        Args:
            file_hash: SHA-256 hash of the file this swarm tracks.
        """
        self._file_hash = file_hash
        self._peers: set = set()

    @property
    def file_hash(self) -> str:
        """SHA-256 hash of the file shared by this swarm."""
        return self._file_hash

    def add_peer(self, peer) -> None:
        """Add a peer node to the swarm so it is discoverable by any peer looking to download the tracked file. Uses a set internally so the same peer cannot be added twice."""
        self._peers.add(peer)

    def remove_peer(self, peer) -> None:
        """Remove a peer node from the swarm (no-op if not present)."""
        self._peers.discard(peer)

    def peers_list(self) -> list:
        """Return a list of all peers in this swarm."""
        return list(self._peers)

    # --- Dunder methods ---

    def __len__(self) -> int:
        return len(self._peers)

    def __contains__(self, item) -> bool:
        """Support both peer_id string and peer object membership tests."""
        if isinstance(item, str):
            return any(p.node_id == item for p in self._peers)
        return item in self._peers

    def __iter__(self):
        return iter(self._peers)

    def __str__(self) -> str:
        return f'Swarm(file={self._file_hash[:8]}..., peers={len(self._peers)})'

    def __repr__(self) -> str:
        ids = [p.node_id for p in self._peers]
        return f'Swarm(file_hash={self._file_hash!r}, peers={ids!r})'
