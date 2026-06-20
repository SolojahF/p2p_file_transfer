"""Transfer protocols: abstract base and two concrete strategies."""

import hashlib
import random
from abc import ABC, abstractmethod

from src.chunk_data import ChunkData
from src.exceptions import ChecksumMismatchError
from src.transfer_session import TransferSession


class TransferProtocol(ABC):
    """
    Abstract base defining the chunk-transfer contract.

    Concrete subclasses implement different strategies for ordering
    chunk delivery (push vs pull). Both support optional simulated
    corruption to test the retry and checksum-verification logic.
    """

    def __init__(self, corruption_probability: float = 0.0):
        """
        Args:
            corruption_probability: Probability (0.0–1.0) that any single
                                    delivered chunk has one byte flipped.

        Raises:
            ValueError: If corruption_probability is outside [0.0, 1.0].
        """
        if not 0.0 <= corruption_probability <= 1.0:
            raise ValueError(
                'corruption_probability must be between 0.0 and 1.0'
            )
        self._corruption_probability = corruption_probability

    @property
    def corruption_probability(self) -> float:
        """Probability that a delivered chunk is silently corrupted."""
        return self._corruption_probability

    @abstractmethod
    def initiate(self, session: TransferSession) -> bool:
        """Begin the transfer session; returns True on success."""
        ...

    @abstractmethod
    def transfer_chunk(self, session: TransferSession, idx: int,
                       provider=None) -> ChunkData:
        """
        Fetch and verify chunk at index idx.

        Args:
            session:  Active TransferSession.
            idx:      Chunk index to fetch.
            provider: Optional override peer to fetch from (used during retry).

        Returns:
            Verified ChunkData.

        Raises:
            ChecksumMismatchError: If the chunk arrives corrupted.
        """
        ...

    @abstractmethod
    def finalise(self, session: TransferSession) -> bool:
        """Verify the whole-file SHA-256 after all chunks are received."""
        ...

    # --- Internal corruption helper ---

    def _maybe_corrupt(self, chunk: ChunkData) -> ChunkData:
        """Randomly flip one byte in the chunk data with corruption_probability.

        The stored checksum is left unchanged so that ChunkData.verify() will
        return False, triggering the retry mechanism in the caller.
        """
        if self._corruption_probability > 0.0 and (
            random.random() < self._corruption_probability
        ):
            data = bytearray(chunk.data)
            flip_idx = random.randrange(len(data))
            data[flip_idx] ^= 0xFF
            return ChunkData._from_raw(
                chunk.file_hash,
                chunk.chunk_index,
                bytes(data),
                chunk.checksum,   # keep original checksum so verify() fails
            )
        return chunk

    @staticmethod
    def _verify_whole_file(session: TransferSession) -> bool:
        """Reassemble received chunks and compare hash to expected value."""
        file_bytes = session.reassemble()
        actual = hashlib.sha256(file_bytes).hexdigest()
        return actual == session.file_metadata.sha256_hash

    # --- String representations ---

    def __str__(self) -> str:
        return (f'{self.__class__.__name__}'
                f'(corruption={self._corruption_probability:.0%})')

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}'
                f'(corruption_probability={self._corruption_probability})')


# ---------------------------------------------------------------------------
# PushProtocol
# ---------------------------------------------------------------------------

class PushProtocol(TransferProtocol):
    """
    Push strategy: provider delivers chunks in sequential order (0, 1, 2, …).

    The provider decides the delivery order, which is simple and predictable.
    A ChecksumMismatchError on any chunk causes the caller to retry that
    chunk from a different peer.
    """

    def initiate(self, session: TransferSession) -> bool:
        """Signal the provider that a push transfer is starting."""
        return True

    def transfer_chunk(self, session: TransferSession, idx: int,
                       provider=None) -> ChunkData:
        """Fetch chunk idx from provider (or session.provider), possibly corrupt it."""
        peer = provider if provider is not None else session.provider
        chunk = peer.request_chunk(session.file_metadata.sha256_hash, idx)
        chunk = self._maybe_corrupt(chunk)
        if not chunk.verify():
            actual = hashlib.sha256(chunk.data).hexdigest()
            raise ChecksumMismatchError(
                chunk.file_hash, chunk.chunk_index, chunk.checksum, actual
            )
        return chunk

    def finalise(self, session: TransferSession) -> bool:
        """Verify whole-file hash once all chunks are received."""
        return self._verify_whole_file(session)


# ---------------------------------------------------------------------------
# PullProtocol
# ---------------------------------------------------------------------------

class PullProtocol(TransferProtocol):
    """
    Pull strategy: requester specifies which chunks it wants.

    In a real system the requester would request rarest-first; here the
    caller controls the order. Both protocols share the same core
    transfer and verification logic — the distinction is conceptual
    (push = server-driven, pull = client-driven).
    """

    def initiate(self, session: TransferSession) -> bool:
        """Signal to the provider that a pull transfer is starting."""
        return True

    def transfer_chunk(self, session: TransferSession, idx: int,
                       provider=None) -> ChunkData:
        """Fetch the specific chunk idx the requester has requested."""
        peer = provider if provider is not None else session.provider
        chunk = peer.request_chunk(session.file_metadata.sha256_hash, idx)
        chunk = self._maybe_corrupt(chunk)
        if not chunk.verify():
            actual = hashlib.sha256(chunk.data).hexdigest()
            raise ChecksumMismatchError(
                chunk.file_hash, chunk.chunk_index, chunk.checksum, actual
            )
        return chunk

    def finalise(self, session: TransferSession) -> bool:
        """Verify whole-file hash once all chunks are received."""
        return self._verify_whole_file(session)
