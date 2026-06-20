"""TransferSession: tracks the live state of a file download."""

import uuid


class TransferSession:
    """
    Records the progress of a single file transfer between two nodes.

    Tracks which chunks are still pending, which have been successfully
    received, and how many retries were needed due to corrupted chunks.
    """

    def __init__(self, requester, provider, file_metadata):
        """
        Args:
            requester:     Node that is downloading the file.
            provider:      Node that is supplying the file chunks initially.
            file_metadata: FileMetadata object describing the target file.
        """
        self._session_id: str = str(uuid.uuid4())
        self._requester = requester
        self._provider = provider
        self._file_metadata = file_metadata
        self._pending_chunks: set = set(range(file_metadata.chunk_count))
        self._received_chunks: dict = {}
        self._retry_count: int = 0

    # --- Read-only properties ---

    @property
    def session_id(self) -> str:
        """Unique identifier for this transfer session (UUID4)."""
        return self._session_id

    @property
    def requester(self):
        """Node that is downloading the file."""
        return self._requester

    @property
    def provider(self):
        """Current provider node supplying chunks."""
        return self._provider

    @property
    def file_metadata(self):
        """Metadata describing the file being transferred."""
        return self._file_metadata

    @property
    def pending_chunks(self) -> set:
        """Set of chunk indices not yet successfully received."""
        return self._pending_chunks

    @property
    def received_chunks(self) -> dict:
        """Dict of chunk_index -> ChunkData for successfully received chunks."""
        return self._received_chunks

    @property
    def retry_count(self) -> int:
        """Total number of chunk retries due to checksum failures."""
        return self._retry_count

    @property
    def progress_pct(self) -> float:
        """Download progress as a percentage (0.0 – 100.0)."""
        total = self._file_metadata.chunk_count
        if total == 0:
            return 100.0
        return (len(self._received_chunks) / total) * 100.0

    # --- Mutating helpers ---

    def set_provider(self, provider) -> None:
        """Switch to a different provider node (used during retry from alternate peer)."""
        self._provider = provider

    def mark_received(self, chunk_data) -> None:
        """Record a successfully verified chunk and remove it from pending."""
        self._received_chunks[chunk_data.chunk_index] = chunk_data
        self._pending_chunks.discard(chunk_data.chunk_index)

    def increment_retry(self) -> None:
        """Increment the retry counter by one."""
        self._retry_count += 1

    def is_complete(self) -> bool:
        """Return True when all chunks have been received."""
        return len(self._pending_chunks) == 0

    def reassemble(self) -> bytes:
        """Concatenate all received chunks in order to reconstruct the file bytes."""
        ordered = [self._received_chunks[i] for i in sorted(self._received_chunks)]
        return b''.join(c.data for c in ordered)

    # --- String representations ---

    def __str__(self) -> str:
        return (f'TransferSession(id={self._session_id[:8]}..., '
                f'progress={self.progress_pct:.1f}%, retries={self._retry_count})')

    def __repr__(self) -> str:
        return (f'TransferSession(session_id={self._session_id!r}, '
                f'requester={self._requester.node_id!r}, '
                f'provider={self._provider.node_id!r}, '
                f'progress={self.progress_pct:.1f}%)')
