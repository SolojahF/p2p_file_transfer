"""Immutable file chunk with SHA-256 integrity verification."""

import hashlib


class ChunkData:
    """
    An immutable snapshot of a single file chunk.

    Every attribute is read-only after construction. verify() re-hashes the
    stored data and compares it against the stored checksum, returning False
    when the data has been corrupted in transit.
    """

    def __init__(self, file_hash: str, chunk_index: int, data: bytes):
        """
        Create a ChunkData and compute its SHA-256 checksum immediately.

        Args:
            file_hash:    SHA-256 hash of the parent file (hex string).
            chunk_index:  Zero-based position of this chunk within the file.
            data:         Raw bytes of this chunk.
        """
        if chunk_index < 0:
            raise ValueError('chunk_index must be non-negative')
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError('data must be bytes or bytearray')
        self._file_hash = file_hash
        self._chunk_index = chunk_index
        self._data = bytes(data)
                self._checksum = hashlib.sha256(self._data).hexdigest() # SHA-256 computed once at construction and stored; verify() recomputes it later
        # convert to immutable bytes so the stored data can never be modified after construction

    
    # --- Factory for corruption simulation (internal use) ---

    @classmethod
    def _from_raw(cls, file_hash: str, chunk_index: int,
                  data: bytes, checksum: str) -> 'ChunkData':
        """Create a ChunkData with an explicitly provided checksum.

        Used internally to simulate transmission corruption: the data is
        modified but the checksum field keeps the original value so that
        verify() will return False.
        """
        instance = cls.__new__(cls)
        instance._file_hash = file_hash
        instance._chunk_index = chunk_index
        instance._data = bytes(data)
        instance._checksum = checksum
        return instance

    # --- Read-only properties ---

    @property
    def file_hash(self) -> str:
        """SHA-256 hash of the parent file."""
        return self._file_hash

    @property
    def chunk_index(self) -> int:
        """Zero-based position of this chunk within the file."""
        return self._chunk_index

    @property
    def data(self) -> bytes:
        """Raw bytes of this chunk."""
        return self._data

    @property
    def checksum(self) -> str:
        """Expected SHA-256 checksum of this chunk's data (hex string)."""
        return self._checksum

    # --- Integrity ---

    def verify(self) -> bool:
        """Return True if the stored data matches the stored checksum - recomputes SHA-256 on the raw bytes and compares it to the value stored at construction time to detect any in-transit corruption."""
        return hashlib.sha256(self._data).hexdigest() == self._checksum

    # --- String representations ---

    def __str__(self) -> str:
        status = 'OK' if self.verify() else 'CORRUPTED'
        return (f'ChunkData(file={self._file_hash[:8]}..., '
                f'idx={self._chunk_index}, size={len(self._data)}B, {status})')

    def __repr__(self) -> str:
        return (f'ChunkData(file_hash={self._file_hash!r}, '
                f'chunk_index={self._chunk_index}, '
                f'data=<{len(self._data)} bytes>, '
                f'checksum={self._checksum[:8]!r}...)')
