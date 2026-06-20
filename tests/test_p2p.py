"""
Pytest test suite for the P2P File Transfer and Integrity Verification System.
Minimum 20 test cases covering all OOP concepts from Weeks 1–5.
"""

import hashlib
import pytest

from src.chunk_data import ChunkData
from src.exceptions import (
    ChecksumMismatchError,
    DuplicateFileError,
    P2PError,
    PeerNotFoundError,
    TransferTimeoutError,
)
from src.file_metadata import FileMetadata
from src.nodes import DataPeerNode, MetadataTrackerNode, RateLimiterMixin, SeedNode
from src.protocols import PullProtocol, PushProtocol
from src.swarm import Swarm
from src.transfer_session import TransferSession


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def sample_data():
    return b'Hello from Group 7! ' * 50   # 1000 bytes

@pytest.fixture
def small_data():
    return b'FUOYE'

@pytest.fixture
def seed(sample_data):
    s = SeedNode('seed-01', '10.0.0.1:8080', bandwidth=100.0)
    s.share_file(sample_data, 'test.txt', chunk_size=256)
    return s

@pytest.fixture
def metadata(sample_data):
    return FileMetadata('test.txt', sample_data, chunk_size_bytes=256)

@pytest.fixture
def tracker(seed, metadata):
    t = MetadataTrackerNode('tracker-01', '10.0.0.0:6969')
    t.register_file(metadata, seed)
    return t

@pytest.fixture
def peer():
    return DataPeerNode('peer-01', '10.0.0.2:8080', bandwidth=50.0)

@pytest.fixture
def session(peer, seed, metadata):
    return TransferSession(peer, seed, metadata)


# ============================================================
# 1. FileMetadata — constructor and properties
# ============================================================

def test_filemetadata_basic_properties(sample_data):
    m = FileMetadata('file.txt', sample_data, chunk_size_bytes=256)
    assert m.filename == 'file.txt'
    assert m.size_bytes == len(sample_data)
    assert m.chunk_size_bytes == 256
    assert m.sha256_hash == hashlib.sha256(sample_data).hexdigest()

def test_filemetadata_chunk_count_exact():
    data = b'A' * 512   # exactly 2 chunks of 256
    m = FileMetadata('f', data, chunk_size_bytes=256)
    assert m.chunk_count == 2

def test_filemetadata_chunk_count_remainder():
    data = b'A' * 300   # 2 chunks: 256 + 44
    m = FileMetadata('f', data, chunk_size_bytes=256)
    assert m.chunk_count == 2

def test_filemetadata_invalid_chunk_size():
    with pytest.raises(ValueError):
        FileMetadata('f', b'hello', chunk_size_bytes=0)

def test_filemetadata_equality_same_content(sample_data):
    m1 = FileMetadata('a.txt', sample_data, 256)
    m2 = FileMetadata('b.txt', sample_data, 256)   # different name, same bytes
    assert m1 == m2

def test_filemetadata_inequality_different_content():
    m1 = FileMetadata('f', b'hello', 256)
    m2 = FileMetadata('f', b'world', 256)
    assert m1 != m2

def test_filemetadata_ordering():
    small = FileMetadata('s', b'hi', 256)
    large = FileMetadata('l', b'hello world!', 256)
    assert small < large
    assert not large < small

def test_filemetadata_hash_consistency(sample_data):
    m1 = FileMetadata('f', sample_data, 256)
    m2 = FileMetadata('f', sample_data, 256)
    assert hash(m1) == hash(m2)

def test_filemetadata_str_repr(sample_data):
    m = FileMetadata('notes.txt', sample_data, 256)
    assert 'notes.txt' in str(m)
    assert 'FileMetadata' in repr(m)


# ============================================================
# 2. ChunkData — immutability and verify()
# ============================================================

def test_chunkdata_verify_clean():
    data = b'clean data chunk'
    c = ChunkData('abc123', 0, data)
    assert c.verify() is True

def test_chunkdata_verify_corrupted():
    data = b'clean data chunk'
    c = ChunkData('abc123', 0, data)
    corrupted = ChunkData._from_raw('abc123', 0, b'dirty data chunk', c.checksum)
    assert corrupted.verify() is False

def test_chunkdata_properties():
    data = b'chunk content'
    c = ChunkData('filehash', 3, data)
    assert c.file_hash == 'filehash'
    assert c.chunk_index == 3
    assert c.data == data
    assert c.checksum == hashlib.sha256(data).hexdigest()

def test_chunkdata_invalid_index():
    with pytest.raises(ValueError):
        ChunkData('hash', -1, b'data')

def test_chunkdata_str_and_repr():
    c = ChunkData('myhash', 0, b'test')
    assert 'ChunkData' in str(c)
    assert 'ChunkData' in repr(c)


# ============================================================
# 3. Swarm — __len__, __contains__, __iter__
# ============================================================

def test_swarm_add_and_len():
    swarm = Swarm('filehash')
    peer = DataPeerNode('p1', 'addr')
    swarm.add_peer(peer)
    assert len(swarm) == 1

def test_swarm_contains_by_id():
    swarm = Swarm('filehash')
    peer = DataPeerNode('p-unique', 'addr')
    swarm.add_peer(peer)
    assert 'p-unique' in swarm
    assert 'unknown' not in swarm

def test_swarm_contains_by_object():
    swarm = Swarm('fh')
    peer = DataPeerNode('p1', 'addr')
    swarm.add_peer(peer)
    assert peer in swarm

def test_swarm_remove_peer():
    swarm = Swarm('fh')
    peer = DataPeerNode('p1', 'addr')
    swarm.add_peer(peer)
    swarm.remove_peer(peer)
    assert len(swarm) == 0

def test_swarm_iteration():
    swarm = Swarm('fh')
    peers = [DataPeerNode(f'p{i}', f'addr{i}') for i in range(3)]
    for p in peers:
        swarm.add_peer(p)
    ids = {p.node_id for p in swarm}
    assert ids == {'p0', 'p1', 'p2'}


# ============================================================
# 4. DataPeerNode and MetadataTrackerNode
# ============================================================

def test_peer_share_file(sample_data):
    peer = DataPeerNode('p', 'addr')
    meta = peer.share_file(sample_data, 'file.txt', chunk_size=256)
    assert peer.has_file(meta.sha256_hash)

def test_peer_request_chunk(sample_data):
    peer = DataPeerNode('p', 'addr')
    meta = peer.share_file(sample_data, 'file.txt', chunk_size=256)
    chunk = peer.request_chunk(meta.sha256_hash, 0)
    assert chunk.chunk_index == 0
    assert chunk.verify()

def test_peer_duplicate_file_error(sample_data):
    peer = DataPeerNode('p', 'addr')
    peer.share_file(sample_data, 'file.txt', chunk_size=256)
    with pytest.raises(DuplicateFileError):
        peer.share_file(sample_data, 'file.txt', chunk_size=256)

def test_tracker_find_peers_sorted_by_bandwidth(metadata):
    tracker = MetadataTrackerNode('t', 'addr')
    fast = DataPeerNode('fast', 'a', bandwidth=90.0)
    slow = DataPeerNode('slow', 'b', bandwidth=10.0)
    tracker.register_file(metadata, slow)
    tracker.register_file(metadata, fast)
    peers = tracker.find_peers(metadata.sha256_hash)
    assert peers[0].node_id == 'fast'

def test_tracker_no_peers_raises(metadata):
    tracker = MetadataTrackerNode('t', 'addr')
    with pytest.raises(PeerNotFoundError):
        tracker.find_peers(metadata.sha256_hash)

def test_tracker_find_peers_limit(metadata):
    tracker = MetadataTrackerNode('t', 'addr')
    for i in range(10):
        p = DataPeerNode(f'p{i}', f'addr{i}', bandwidth=float(i))
        tracker.register_file(metadata, p)
    peers = tracker.find_peers(metadata.sha256_hash, limit=3)
    assert len(peers) == 3


# ============================================================
# 5. TransferSession — progress and completion
# ============================================================

def test_session_initial_progress(session, metadata):
    assert session.progress_pct == 0.0
    assert not session.is_complete()
    assert len(session.pending_chunks) == metadata.chunk_count

def test_session_mark_received_updates_progress(session, seed, metadata):
    chunk = seed.request_chunk(metadata.sha256_hash, 0)
    session.mark_received(chunk)
    total = metadata.chunk_count
    expected_pct = (1 / total) * 100.0
    assert abs(session.progress_pct - expected_pct) < 0.01

def test_session_is_complete(seed, metadata):
    peer = DataPeerNode('p', 'addr')
    session = TransferSession(peer, seed, metadata)
    for i in range(metadata.chunk_count):
        chunk = seed.request_chunk(metadata.sha256_hash, i)
        session.mark_received(chunk)
    assert session.is_complete()
    assert session.progress_pct == 100.0

def test_session_str_repr(session):
    assert 'TransferSession' in str(session)
    assert 'TransferSession' in repr(session)


# ============================================================
# 6. Protocols — clean transfer and corruption detection
# ============================================================

def test_push_protocol_clean_transfer(session, seed, metadata):
    proto = PushProtocol(corruption_probability=0.0)
    proto.initiate(session)
    for idx in range(metadata.chunk_count):
        chunk = proto.transfer_chunk(session, idx, provider=seed)
        session.mark_received(chunk)
    assert proto.finalise(session)

def test_pull_protocol_clean_transfer(session, seed, metadata):
    proto = PullProtocol(corruption_probability=0.0)
    proto.initiate(session)
    for idx in range(metadata.chunk_count):
        chunk = proto.transfer_chunk(session, idx, provider=seed)
        session.mark_received(chunk)
    assert proto.finalise(session)

def test_push_protocol_raises_on_corruption(seed, metadata):
    """With 100% corruption every chunk raises ChecksumMismatchError."""
    peer = DataPeerNode('p', 'addr')
    session = TransferSession(peer, seed, metadata)
    proto = PushProtocol(corruption_probability=1.0)
    proto.initiate(session)
    with pytest.raises(ChecksumMismatchError):
        proto.transfer_chunk(session, 0, provider=seed)

def test_pull_protocol_raises_on_corruption(seed, metadata):
    peer = DataPeerNode('p', 'addr')
    session = TransferSession(peer, seed, metadata)
    proto = PullProtocol(corruption_probability=1.0)
    with pytest.raises(ChecksumMismatchError):
        proto.transfer_chunk(session, 0, provider=seed)

def test_protocol_invalid_corruption_probability():
    with pytest.raises(ValueError):
        PushProtocol(corruption_probability=1.5)

def test_protocol_str_repr():
    p = PushProtocol(corruption_probability=0.1)
    assert 'PushProtocol' in str(p)
    assert 'PushProtocol' in repr(p)


# ============================================================
# 7. Custom exceptions — structured fields
# ============================================================

def test_checksum_mismatch_error_fields():
    err = ChecksumMismatchError('abcdef1234', 2, 'expected_hash', 'actual_hash')
    assert err.file_hash == 'abcdef1234'
    assert err.chunk_idx == 2
    assert err.expected == 'expected_hash'
    assert err.received == 'actual_hash'
    assert isinstance(err, P2PError)
    assert isinstance(err, IOError)

def test_peer_not_found_error_fields():
    err = PeerNotFoundError('deadbeef', 'no peers')
    assert err.file_hash == 'deadbeef'
    assert 'deadbeef' in str(err)
    assert isinstance(err, P2PError)

def test_duplicate_file_error_fields():
    err = DuplicateFileError('aabbcc', 'myfile.txt')
    assert err.file_hash == 'aabbcc'
    assert err.filename == 'myfile.txt'
    assert isinstance(err, P2PError)

def test_transfer_timeout_error_fields():
    err = TransferTimeoutError('session-uuid-123', 5)
    assert err.session_id == 'session-uuid-123'
    assert err.chunk_idx == 5
    assert isinstance(err, P2PError)


# ============================================================
# 8. SeedNode — multiple inheritance verification
# ============================================================

def test_seed_node_is_data_peer_and_rate_limiter():
    seed = SeedNode('s', 'addr')
    assert isinstance(seed, DataPeerNode)
    assert isinstance(seed, RateLimiterMixin)

def test_seed_node_bandwidth_tracking(small_data):
    seed = SeedNode('s', 'addr', max_bandwidth_bps=100)
    seed.share_file(small_data, 'tiny.txt', chunk_size=256)
    meta = FileMetadata('tiny.txt', small_data, 256)
    seed.request_chunk(meta.sha256_hash, 0)
    assert seed.bytes_sent_this_second > 0

def test_rate_limiter_can_send():
    seed = SeedNode('s', 'addr', max_bandwidth_bps=1000)
    assert seed.can_send(500)
    seed.record_sent(800)
    assert not seed.can_send(500)   # 800 + 500 > 1000


# ============================================================
# 9. Polymorphism — duck typing
# ============================================================

def test_duck_typed_transfer(seed, metadata):
    """Any object with request_chunk() can serve as a provider."""
    def fetch_all(provider, file_hash, chunk_count):
        return [provider.request_chunk(file_hash, i) for i in range(chunk_count)]

    chunks = fetch_all(seed, metadata.sha256_hash, metadata.chunk_count)
    assert len(chunks) == metadata.chunk_count
    assert all(c.verify() for c in chunks)

def test_sorted_filemetadata():
    files = [
        FileMetadata('c', b'c' * 300, 256),
        FileMetadata('a', b'a' * 100, 256),
        FileMetadata('b', b'b' * 200, 256),
    ]
    ordered = sorted(files)
    assert [f.filename for f in ordered] == ['a', 'b', 'c']


# ============================================================
# 10. Full integration — download with retries
# ============================================================

def test_full_transfer_with_corruption(sample_data):
    """End-to-end: seeder -> peer with 50% corruption, expect success via retries."""
    import random
    random.seed(42)   # deterministic outcome for the test

    seed = SeedNode('seed', '0.0.0.1:9000', bandwidth=100.0)
    meta = seed.share_file(sample_data, 'sample.txt', chunk_size=256)

    tracker = MetadataTrackerNode('tracker', '0.0.0.0:6969')
    tracker.register_file(meta, seed)

    peer = DataPeerNode('peer', '0.0.0.2:9000', bandwidth=50.0)
    proto = PushProtocol(corruption_probability=0.5)

    from tests.test_p2p import _run_transfer
    session, retries, ok = _run_transfer(peer, tracker, meta, proto)
    assert ok, 'Whole-file hash must verify after retried transfer'
    assert session.is_complete()


def _run_transfer(requester, tracker, metadata, protocol, max_retries=200):
    """Internal helper used by the integration test."""
    from src.exceptions import ChecksumMismatchError, PeerNotFoundError
    peers = tracker.find_peers(metadata.sha256_hash)
    session = TransferSession(requester, peers[0], metadata)
    protocol.initiate(session)
    total_retries = 0

    for chunk_idx in range(metadata.chunk_count):
        for peer in peers * max_retries:
            try:
                chunk = protocol.transfer_chunk(session, chunk_idx, provider=peer)
                session.mark_received(chunk)
                break
            except ChecksumMismatchError:
                total_retries += 1
                session.increment_retry()

    return session, total_retries, protocol.finalise(session)
