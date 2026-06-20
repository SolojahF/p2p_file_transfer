# Design Notes — Group 7 P2P File Transfer System

## Key Design Decisions

### 1. Composition vs Aggregation

**Composition (DataPeerNode ◆ ChunkData):**  
When a peer calls `share_file()`, it creates `ChunkData` objects internally and stores them in `_local_files`. These chunks have no meaning or existence outside the peer that created them — if the peer is destroyed, the chunks are gone too. This is a classic composition relationship: the whole (DataPeerNode) controls the lifecycle of the parts (ChunkData).

**Aggregation (TransferSession ○ Node):**  
A `TransferSession` holds references to `requester` and `provider` nodes, but it does not own them. Both nodes were created independently and exist before the session starts. The session merely references them. This is aggregation — the session can be destroyed without affecting the nodes.

**Aggregation (MetadataTrackerNode ○ Swarm ○ DataPeerNode):**  
The tracker references `Swarm` objects, and each `Swarm` references `DataPeerNode` instances. Neither relationship is ownership — peers were created independently and can leave swarms (via `remove_peer`) without being destroyed. Both are aggregation.

---

### 2. Why ChunkData is Immutable

`ChunkData` is made immutable by design: all attributes are stored as private backing fields with read-only `@property` accessors and no setters. This is intentional — a chunk is a snapshot of data at a point in time. Mutating a received chunk after checksum verification would silently invalidate the integrity guarantees we worked hard to enforce. Immutability removes that risk entirely.

---

### 3. Why RateLimiterMixin Uses Cooperative `super()`

`SeedNode` inherits from both `DataPeerNode` and `RateLimiterMixin`. Both ultimately inherit from `Node` (or `object`). Without cooperative `super()`, one `__init__` path would be skipped, leaving the object partially initialised. By using `super().__init__(*args, **kwargs)` in both `RateLimiterMixin` and `DataPeerNode`, Python's MRO ensures every `__init__` in the chain is called exactly once.

---

### 4. Duck Typing in `transfer_chunk`

The `provider` parameter in `TransferProtocol.transfer_chunk(session, idx, provider=None)` accepts any object that has a `request_chunk(file_hash, chunk_idx)` method — no `isinstance()` check is performed. This means any future node type (e.g. a `CachedProxyNode`) that implements `request_chunk` can be passed in without changing the protocol code. This is duck typing: "if it has the method I need, it will work."

---

### 5. SHA-256 for Both Chunk and File Integrity

Two levels of verification run independently:
- **Chunk level:** `ChunkData.verify()` recomputes `SHA-256(chunk.data)` and compares it with the stored `checksum`. This detects in-transit corruption of individual chunks and triggers per-chunk retry.
- **File level:** `TransferProtocol.finalise()` reassembles all chunks and computes `SHA-256(full_file_bytes)`, comparing it with `FileMetadata.sha256_hash`. This is the final guarantee that the reconstructed file is exactly what the seeder originally had.

The two checks are complementary: chunk verification catches most corruption quickly (one retry per bad chunk), while whole-file verification is the authoritative final gate.
