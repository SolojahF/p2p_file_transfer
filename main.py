"""
CPE 310 — Group 7 Capstone Demo
Peer-to-Peer File Transfer and Integrity Verification System

Demonstrates:
  • 1 SeedNode seeding a file
  • 2 DataPeerNodes each downloading the full file
  • corruption_probability = 0.2 (20% chunk corruption rate)
  • Automatic retry from a different peer on checksum failure
  • Final whole-file SHA-256 verification
"""

import hashlib

from src.exceptions import ChecksumMismatchError, PeerNotFoundError
from src.nodes import DataPeerNode, MetadataTrackerNode, SeedNode
from src.protocols import PullProtocol, PushProtocol
from src.transfer_session import TransferSession


# ---------------------------------------------------------------------------
# Transfer helper
# ---------------------------------------------------------------------------

def download_file(requester, tracker, metadata, protocol, max_retries: int = 50):
    """
    Download a complete file to `requester` using `protocol`.

    Retries any chunk that arrives with a bad checksum by trying the next
    available peer from the tracker. Returns a transfer report dict.

    Args:
        requester:   DataPeerNode that wants the file.
        tracker:     MetadataTrackerNode to query for peer list.
        metadata:    FileMetadata describing the file to fetch.
        protocol:    TransferProtocol (PushProtocol or PullProtocol).
        max_retries: Max retry attempts before raising TransferTimeoutError.

    Returns:
        dict with keys: session, retries, total_chunks, final_ok
    """
    peers = tracker.find_peers(metadata.sha256_hash)
    if not peers:
        raise PeerNotFoundError(metadata.sha256_hash, 'no peers available')

    session = TransferSession(requester, peers[0], metadata)
    protocol.initiate(session)
    total_retries = 0

    for chunk_idx in range(metadata.chunk_count):
        transferred = False
        for attempt, peer in enumerate(peers * 3):   # cycle peers up to 3x
            if total_retries >= max_retries:
                from src.exceptions import TransferTimeoutError
                raise TransferTimeoutError(session.session_id, chunk_idx)
            try:
                chunk = protocol.transfer_chunk(session, chunk_idx, provider=peer)
                session.mark_received(chunk)
                transferred = True
                break
            except ChecksumMismatchError:
                total_retries += 1
                session.increment_retry()
                print(f'     [!] Chunk {chunk_idx} corrupted from {peer.node_id}'
                      f' — retrying (attempt {total_retries})...')

        if not transferred:
            print(f'     [!!] All peers failed for chunk {chunk_idx}')

    final_ok = protocol.finalise(session)
    effective_bytes = sum(len(c.data) for c in session.received_chunks.values())

    return {
        'session': session,
        'retries': total_retries,
        'total_chunks': metadata.chunk_count,
        'effective_bytes': effective_bytes,
        'final_ok': final_ok,
    }


def print_report(peer_name: str, protocol_name: str, report: dict) -> None:
    """Print a formatted transfer summary."""
    s = report['session']
    verdict = 'PASS ✓' if report['final_ok'] else 'FAIL ✗'
    print(f'\n  {"─" * 50}')
    print(f'  Transfer Report for {peer_name} using {protocol_name}')
    print(f'  {"─" * 50}')
    print(f'  Total chunks transferred : {report["total_chunks"]}')
    print(f'  Chunks that needed retry : {report["retries"]}')
    print(f'  Effective bytes received : {report["effective_bytes"]} B')
    print(f'  Whole-file SHA-256 check : {verdict}')
    print(f'  {"─" * 50}')


# ---------------------------------------------------------------------------
# Main demo
# ---------------------------------------------------------------------------

def main():
    sep = '=' * 60
    print(sep)
    print('  CPE 310 — Group 7 — P2P File Transfer Demo')
    print(sep)

    # ── 1. Create the tracker ──────────────────────────────────────────────
    tracker = MetadataTrackerNode('tracker-01', '192.168.0.1:6969')
    print(f'\n[Tracker]  {tracker}')

    # ── 2. Create a SeedNode and load three test files ────────────────────
    seed = SeedNode('seed-01', '192.168.0.10:8080',
                    bandwidth=100.0, max_bandwidth_bps=10_000_000)
    print(f'[Seeder]   {seed}')

    # File A — text-like content
    file_a_data = b'FUOYE CPE310 Group7 P2P System ' * 150   # ~4650 bytes
    # File B — binary-like content (simulated firmware)
    file_b_data = bytes(range(256)) * 20                      # 5120 bytes
    # File C — small file (edge case)
    file_c_data = b'Tiny file!'                               # 10 bytes

    meta_a = seed.share_file(file_a_data, 'notes.txt', chunk_size=256)
    meta_b = seed.share_file(file_b_data, 'firmware.bin', chunk_size=512)
    meta_c = seed.share_file(file_c_data, 'tiny.txt', chunk_size=256)

    for m in (meta_a, meta_b, meta_c):
        tracker.register_file(m, seed)

    print(f'\n[Files registered on seed]')
    print(f'   {meta_a}')
    print(f'   {meta_b}')
    print(f'   {meta_c}')

    # ── 3. Create two DataPeerNodes ───────────────────────────────────────
    peer_a = DataPeerNode('peer-A', '192.168.0.20:8080', bandwidth=50.0)
    peer_b = DataPeerNode('peer-B', '192.168.0.30:8080', bandwidth=30.0)
    print(f'\n[Peers]    {peer_a}')
    print(f'           {peer_b}')

    # ── 4. Peer-A downloads file_a via PushProtocol (corruption 0.2) ─────
    print(f'\n[Step 1] peer-A downloading notes.txt via PushProtocol '
          f'(corruption_probability=0.20)...')
    push_proto = PushProtocol(corruption_probability=0.20)
    report_a = download_file(peer_a, tracker, meta_a, push_proto)
    print_report('peer-A', 'PushProtocol', report_a)

    # ── 5. Peer-B downloads file_b via PullProtocol (corruption 0.2) ─────
    print(f'\n[Step 2] peer-B downloading firmware.bin via PullProtocol '
          f'(corruption_probability=0.20)...')
    pull_proto = PullProtocol(corruption_probability=0.20)
    report_b = download_file(peer_b, tracker, meta_b, pull_proto)
    print_report('peer-B', 'PullProtocol', report_b)

    # ── 6. Both peers now register with tracker and cross-download ────────
    peer_a.share_file(file_a_data, 'notes.txt', chunk_size=256)     # already downloaded
    tracker.register_file(meta_a, peer_a)

    peer_b.share_file(file_b_data, 'firmware.bin', chunk_size=512)
    tracker.register_file(meta_b, peer_b)

    print(f'\n[Step 3] peer-B downloading notes.txt (now 2 peers available)...')
    report_c = download_file(peer_b, tracker, meta_a,
                             PushProtocol(corruption_probability=0.20))
    print_report('peer-B', 'PushProtocol (2 peers)', report_c)

    # ── 7. Small file edge-case ───────────────────────────────────────────
    print(f'\n[Step 4] peer-A downloading tiny.txt (edge case: 1 chunk)...')
    report_d = download_file(peer_a, tracker, meta_c,
                             PullProtocol(corruption_probability=0.0))
    print_report('peer-A', 'PullProtocol (no corruption)', report_d)

    # ── 8. Operator-overloading demo ──────────────────────────────────────
    print(f'\n[Step 5] Operator overloading demos...')
    print(f'   notes.txt == notes.txt (same content) : '
          f'{meta_a == FileMetadata_from_data(file_a_data)}')
    print(f'   notes.txt < firmware.bin (by size)    : {meta_a < meta_b}')
    files_sorted = sorted([meta_b, meta_a, meta_c])
    print(f'   sorted files by size: {[m.filename for m in files_sorted]}')

    swarm = tracker.get_swarm(meta_a.sha256_hash)
    print(f'\n[Swarm]    {swarm}')
    print(f'   len(swarm)               : {len(swarm)}')
    print(f'   "seed-01" in swarm       : {"seed-01" in swarm}')
    print(f'   "unknown" in swarm       : {"unknown" in swarm}')
    print(f'   peers via iteration      : '
          f'{[p.node_id for p in swarm]}')

    print(f'\n{sep}')
    print('  All transfers completed successfully.')
    print(sep)


def FileMetadata_from_data(data: bytes):
    """Helper: create FileMetadata without a peer (for operator demo)."""
    from src.file_metadata import FileMetadata
    return FileMetadata('notes.txt', data, chunk_size_bytes=256)


if __name__ == '__main__':
    main()
