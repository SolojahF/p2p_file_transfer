"""Node hierarchy: abstract Node, DataPeerNode, MetadataTrackerNode, SeedNode."""

from abc import ABC, abstractmethod
from typing import Optional

from src.chunk_data import ChunkData
from src.exceptions import DuplicateFileError, PeerNotFoundError
from src.file_metadata import FileMetadata
from src.swarm import Swarm


class Node(ABC):
    """
    Abstract base class for all participants in the P2P network.

    Every node has a unique node_id and a network address. Concrete
    subclasses implement send() and receive() to simulate message passing.
    """

    def __init__(self, node_id: str, address: str, **kwargs):
        """
        Args:
            node_id: Unique identifier for this node.
            address: Simulated network address (e.g. '192.168.1.10:8080').
            **kwargs: Passed through the cooperative MRO chain (e.g. to RateLimiterMixin).
        """
        if not node_id:
            raise ValueError('node_id must be a non-empty string')
        super().__init__(**kwargs)   # cooperative: passes max_bandwidth_bps to RateLimiterMixin
        self._node_id = node_id
        self._address = address

    @property
    def node_id(self) -> str:
        """Unique identifier for this node."""
        return self._node_id

    @property
    def address(self) -> str:
        """Simulated network address of this node."""
        return self._address

    @abstractmethod
    def send(self, message: dict) -> dict:
        """Send a message dict and return the response dict."""
        ...

    @abstractmethod
    def receive(self) -> dict:
        """Return the next pending inbound message, or empty dict if none."""
        ...

    def __str__(self) -> str:
        return f'{self.__class__.__name__}(id={self._node_id!r}, addr={self._address!r})'

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}('
                f'node_id={self._node_id!r}, address={self._address!r})')


# ---------------------------------------------------------------------------
# RateLimiterMixin
# ---------------------------------------------------------------------------

class RateLimiterMixin:
    """
    Mixin that enforces a per-second bandwidth limit on outgoing data.

    Uses cooperative super() so it composes correctly in multiple-inheritance
    hierarchies.  Call reset_bandwidth() once per simulated second.
    """

    def __init__(self, *args, max_bandwidth_bps: int = 1_000_000, **kwargs):
        super().__init__(*args, **kwargs)
        if max_bandwidth_bps <= 0:
            raise ValueError('max_bandwidth_bps must be positive')
        self._max_bw: int = max_bandwidth_bps
        self._bytes_this_second: int = 0

    @property
    def max_bandwidth_bps(self) -> int:
        """Maximum bytes per second this node will send."""
        return self._max_bw

    @property
    def bytes_sent_this_second(self) -> int:
        """Running tally of bytes sent in the current second."""
        return self._bytes_this_second

    def can_send(self, byte_count: int) -> bool:
        """Return True if sending byte_count bytes stays within the per-second bandwidth limit. Callers should check this before calling record_sent() to avoid exceeding the configured max_bandwidth_bps cap."""
        return (self._bytes_this_second + byte_count) <= self._max_bw

    def record_sent(self, byte_count: int) -> None:
        """Increment the bandwidth counter after sending byte_count bytes."""
        self._bytes_this_second += byte_count

    def reset_bandwidth(self) -> None:
        """Reset the counter at the start of a new second."""
        self._bytes_this_second = 0


# ---------------------------------------------------------------------------
# DataPeerNode
# ---------------------------------------------------------------------------

class DataPeerNode(Node):
    """
    A peer node that stores files locally and serves chunks on request.

    Files are fragmented into ChunkData objects on registration via
    share_file(). Chunks are served by request_chunk(). The bandwidth
    attribute is a simulated score used by the tracker to rank peers.
    """

    def __init__(self, node_id: str, address: str, bandwidth: float = 10.0, **kwargs):
        """
        Args:
            node_id:   Unique peer identifier.
            address:   Simulated network address.
            bandwidth: Simulated bandwidth score (higher = faster, for ranking).
            **kwargs:  Forwarded up the cooperative MRO chain.
        """
        super().__init__(node_id, address, **kwargs)
        if bandwidth < 0:
            raise ValueError('bandwidth score must be non-negative')
        self._bandwidth: float = bandwidth
        self._local_files: dict = {}   # sha256_hash -> (FileMetadata, list[ChunkData])
        self._inbox: list = []

    @property
    def bandwidth(self) -> float:
        """Simulated bandwidth score used by tracker to rank this peer."""
        return self._bandwidth

    @property
    def local_files(self) -> dict:
        """Mapping of file hash -> (FileMetadata, chunks) for locally held files."""
        return self._local_files

    def share_file(self, data: bytes, name: str,
                   chunk_size: int = 512) -> FileMetadata:
        """
        Fragment data into chunks and register the file on this peer.

        Args:
            data:       Raw file bytes.
            name:       Display filename.
            chunk_size: Size of each chunk in bytes.

        Returns:
            FileMetadata describing the registered file.

        Raises:
            DuplicateFileError: If this peer already holds the same file.
        """
        metadata = FileMetadata(name, data, chunk_size)
        if metadata.sha256_hash in self._local_files:
            raise DuplicateFileError(metadata.sha256_hash, name)
        chunks: list[ChunkData] = []
        for i in range(metadata.chunk_count):
            start = i * chunk_size
            chunk_bytes = data[start: start + chunk_size]
            chunks.append(ChunkData(metadata.sha256_hash, i, chunk_bytes))
        self._local_files[metadata.sha256_hash] = (metadata, chunks)
        return metadata

    def request_chunk(self, file_hash: str, chunk_idx: int) -> ChunkData:
        """
        Return the chunk at chunk_idx for the given file.

        Raises:
            PeerNotFoundError: If this peer does not hold the file.
            IndexError:        If chunk_idx is out of range.
        """
        if file_hash not in self._local_files:
            raise PeerNotFoundError(
                file_hash, f'Peer {self._node_id} does not hold this file'
            )
        _, chunks = self._local_files[file_hash]
        if chunk_idx < 0 or chunk_idx >= len(chunks):
            raise IndexError(
                f'Chunk index {chunk_idx} out of range (0–{len(chunks) - 1})'
            )
        return chunks[chunk_idx]

    def has_file(self, file_hash: str) -> bool:
        """Return True if this peer holds the specified file, looked up by SHA-256 hash so the check is content-based rather than filename-based. Two files with different names but identical content share the same hash and are treated as one file."""
        return file_hash in self._local_files

    def send(self, message: dict) -> dict:
        """Handle incoming request messages."""
        msg_type = message.get('type')
        if msg_type == 'request_chunk':
            try:
                chunk = self.request_chunk(
                    message['file_hash'], message['chunk_idx']
                )
                return {'status': 'ok', 'chunk': chunk}
            except (PeerNotFoundError, IndexError) as exc:
                return {'status': 'error', 'message': str(exc)}
        return {'status': 'unknown_message_type'}

    def receive(self) -> dict:
        return self._inbox.pop(0) if self._inbox else {}


# ---------------------------------------------------------------------------
# MetadataTrackerNode
# ---------------------------------------------------------------------------

class MetadataTrackerNode(Node):
    """
    Central tracker that maintains a registry of file → peer mappings.

    Peers register files they hold via register_file(). Downloaders call
    find_peers() to discover which peers have the file they want, returned
    in descending bandwidth order so the fastest peer is tried first.
    """

    def __init__(self, node_id: str, address: str):
        super().__init__(node_id, address)
        self._swarms: dict = {}   # sha256_hash -> Swarm
        self._inbox: list = []

    def register_file(self, metadata: FileMetadata, peer: DataPeerNode) -> None:
        """Record that peer holds the file described by metadata."""
        fh = metadata.sha256_hash
        if fh not in self._swarms:
            self._swarms[fh] = Swarm(fh)
        self._swarms[fh].add_peer(peer)

    def find_peers(self, file_hash: str, limit: int = 5) -> list:
        """
        Return up to `limit` peers that hold the file, ranked by bandwidth.

        Raises:
            PeerNotFoundError: If no peers are registered for this file.
        """
        if file_hash not in self._swarms:
            raise PeerNotFoundError(file_hash, 'No peers registered for this file')
        peers = list(self._swarms[file_hash])
        peers.sort(key=lambda p: p.bandwidth, reverse=True)
        return peers[:limit]

    def get_swarm(self, file_hash: str) -> Optional[Swarm]:
        """Return the Swarm for a file hash, or None if not found."""
        return self._swarms.get(file_hash)

    def send(self, message: dict) -> dict:
        msg_type = message.get('type')
        if msg_type == 'find_peers':
            try:
                peers = self.find_peers(
                    message['file_hash'], message.get('limit', 5)
                )
                return {'status': 'ok', 'peers': peers}
            except PeerNotFoundError as exc:
                return {'status': 'error', 'message': str(exc)}
        return {'status': 'unknown_message_type'}

    def receive(self) -> dict:
        return self._inbox.pop(0) if self._inbox else {}


# ---------------------------------------------------------------------------
# SeedNode  (multiple inheritance: DataPeerNode + RateLimiterMixin)
# ---------------------------------------------------------------------------

class SeedNode(DataPeerNode, RateLimiterMixin):
    """
    An always-available seeder node with configurable rate limiting.

    Inherits file storage and chunk serving from DataPeerNode and bandwidth
    enforcement from RateLimiterMixin via cooperative super() MRO.
    """

    def __init__(self, node_id: str, address: str,
                 bandwidth: float = 100.0,
                 max_bandwidth_bps: int = 10_000_000):
        """
        Args:
            node_id:           Unique seeder identifier.
            address:           Simulated network address.
            bandwidth:         Peer ranking score (default high for seeders).
            max_bandwidth_bps: Hard upload cap in bytes/second.
        """
        super().__init__(
            node_id, address,
            bandwidth=bandwidth,
            max_bandwidth_bps=max_bandwidth_bps
        )

    def request_chunk(self, file_hash: str, chunk_idx: int) -> ChunkData:
        """Serve a chunk and record the bytes sent against the bandwidth cap."""
        chunk = super().request_chunk(file_hash, chunk_idx)
        self.record_sent(len(chunk.data))
        return chunk
        self._bytes_this_second: int = 0
reset to 0 each second via reset_bandwidth(); tracks outgoing bytes in current window
