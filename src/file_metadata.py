"""File metadata with SHA-256 identification and chunk layout information."""

import hashlib
import math
from functools import total_ordering


@total_ordering
class FileMetadata:
    """
    Describes a file available for transfer.

    Two FileMetadata objects are equal if and only if their SHA-256 hashes
    match, meaning the same content regardless of filename. Ordering is by
    file size (smaller < larger), enabling sorted() on lists of metadata.
    """

    def __init__(self, filename: str, data: bytes, chunk_size_bytes: int = 512):
        """
        Compute metadata from the raw file bytes.

        Args:
            filename:         Original filename (used for display only).
            data:             Complete file content as bytes.
            chunk_size_bytes: Size of each chunk in bytes (must be > 0).

        Raises:
            ValueError: If chunk_size_bytes is not positive.
        """
        if chunk_size_bytes <= 0:
            raise ValueError('chunk_size_bytes must be a positive integer')
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError('data must be bytes or bytearray')

        self._filename = filename
        self._size_bytes = len(data)
        self._sha256_hash = hashlib.sha256(data).hexdigest()
        self._chunk_size_bytes = chunk_size_bytes
        self._chunk_count = (
            math.ceil(self._size_bytes / chunk_size_bytes)
            if self._size_bytes > 0 else 0
        )

    # --- Read-only properties ---

    @property
    def filename(self) -> str:
        """Original filename."""
        return self._filename

    @property
    def size_bytes(self) -> int:
        """Total file size in bytes."""
        return self._size_bytes

    @property
    def sha256_hash(self) -> str:
        """SHA-256 hash of the complete file content (hex string)."""
        return self._sha256_hash

    @property
    def chunk_count(self) -> int:
        """Total number of chunks the file is divided into."""
        return self._chunk_count

    @property
    def chunk_size_bytes(self) -> int:
        """Size of each chunk in bytes (last chunk may be smaller)."""
        return self._chunk_size_bytes

    # --- Dunder methods ---

    def __eq__(self, other: object) -> bool:
        """Two files are equal if they share the same SHA-256 hash."""
        if not isinstance(other, FileMetadata):
            return NotImplemented
        return self._sha256_hash == other._sha256_hash

    def __lt__(self, other: object) -> bool:
        """A file is less than another if it is smaller in bytes."""
        if not isinstance(other, FileMetadata):
            return NotImplemented
        return self._size_bytes < other._size_bytes

    def __hash__(self) -> int:
        return hash(self._sha256_hash)

    def __str__(self) -> str:
        return (f'FileMetadata({self._filename!r}, {self._size_bytes}B, '
                f'{self._chunk_count} chunks x {self._chunk_size_bytes}B)')

    def __repr__(self) -> str:
        return (f'FileMetadata(filename={self._filename!r}, '
                f'size_bytes={self._size_bytes}, '
                f'sha256={self._sha256_hash[:8]!r}..., '
                f'chunk_count={self._chunk_count}, '
                f'chunk_size_bytes={self._chunk_size_bytes})')
