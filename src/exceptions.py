"""Custom exception hierarchy for the P2P file transfer system."""


class P2PError(Exception):
    """Base exception for all P2P system errors."""
    pass


class ChecksumMismatchError(P2PError, IOError):
    """Raised when a received chunk's SHA-256 checksum does not match the expected value."""

    def __init__(self, file_hash: str, chunk_idx: int, expected: str, received: str):
        super().__init__(
            f'Chunk {chunk_idx} of {file_hash[:8]}... checksum mismatch. '
            f'Expected: {expected[:8]}... Got: {received[:8]}...'
        )
        self.file_hash = file_hash
        self.chunk_idx = chunk_idx
        self.expected = expected
        self.received = received


class PeerNotFoundError(P2PError):
    """Raised when no peer can be found that holds the requested file."""

    def __init__(self, file_hash: str, detail: str = ''):
        super().__init__(f'No peer found for file {file_hash[:8]}...: {detail}')
        self.file_hash = file_hash
        self.detail = detail


class TransferTimeoutError(P2PError):
    """Raised when a transfer session exceeds the allowed retry limit."""

    def __init__(self, session_id: str, chunk_idx: int):
        super().__init__(
            f'Transfer session {session_id[:8]}... timed out on chunk {chunk_idx}'
        )
        self.session_id = session_id
        self.chunk_idx = chunk_idx


class DuplicateFileError(P2PError):
    """Raised when a peer attempts to register a file it already holds."""

    def __init__(self, file_hash: str, filename: str):
        super().__init__(
            f'File {filename!r} (hash {file_hash[:8]}...) is already registered on this peer'
        )
        self.file_hash = file_hash
        self.filename = filename
