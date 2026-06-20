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


---

### 6. Why @property Is Used Instead of Public Attributes

All internal state is stored in private backing attributes (prefixed with `_`) and exposed through read-only
`@property` accessors. If we used public attributes like `self.data = bytes(data)`, any code outside the class could
overwrite them - for example `chunk.data = b"tampered"` - which would silently invalidate the SHA-256 checksum that
was computed on the original bytes. By using `@property` with no setter, the class enforces that attributes can only
be read after construction. This is encapsulation (Week 2): hiding internal state and controlling access through a
public interface only.

---
### 7. Why UUID4 Is Used for Session IDs

`TransferSession` generates its `session_id` using `str(uuid.uuid4())`. UUID4 produces a 128-bit random identifier with a collision probability so low it is treated as zero in practice. We use UUID4 rather than a sequential counter (`session_count += 1`) because sequential IDs reveal how many sessions have run and are predictable. UUID4 IDs are opaque and unpredictable in a real distributed system, session IDs can be used for authentication, so predictability is a security risk. This also demonstrates correct use of Python's standard library `uuid` module as referenced in the project's Week 1-5 00P concepts.
### 9. How @total_ordering Reduces Code Duplication

Python requires six comparison methods for a fully ordered type:
`__eq__`, `__ne__`, `__lt__`, `__le__`, `__gt__`, and `__ge__`.

Writing all six for `FileMetadata` would mean repeating similar size-comparison logic several times.

The `@functools.total_ordering` decorator reduces this duplication. We only define `__eq__` and `__lt__`, and Python automatically generates the remaining comparison methods.

This follows the DRY (Don't Repeat Yourself) principle and still provides full support for operations such as `sorted()`, `min()`, and `max()`.

### 12. Why bytes Is Used Instead of bytearray in ChunkData

In `ChunkData.__init__`, incoming data is stored as `self._data = bytes(data)` even though the parameter accepts both `bytes` and `bytearray`. This conversion is intentional. `bytearray` is mutable - any code holding a reference to it could modify the contents after the SHA-256 checksum has already been computed, silently breaking the integrity guarantee. `bytes` is immutable in Python: once created, its contents can never change. By converting to `bytes` in the constructor, `ChunkData` ensures that `self._data` and `self._checksum` are always consistent for the entire lifetime of the object. This is encapsulation combined with Python's immutability guarantee (Week 2).
### 17. Why chunk_index Is Zero-Based

All chunk indices start from 0, matching Python's own slice and range conventions. In `DataPeerNode.share_file()`, chunks are created using `range(metadata.chunk_count)` — which starts at 0. The byte slice for chunk `i` is computed as `data[i * chunk_size : (i + 1) * chunk_size]`. If indices were one-based, this formula would skip the first `chunk_size` bytes of the file, since `data[1*512 : 2*512]` starts at byte 512, not byte 0. Zero-based indexing keeps the arithmetic consistent with Python's string and list slicing, eliminates off-by-one bugs at the boundary, and means `pending_chunks = set(range(chunk_count))` initialises correctly with no adjustment needed.
### 15. Why sorted() Is Used in reassemble()

`TransferSession.reassemble()` calls `sorted(self._received_chunks)` on the dictionary keys before building the final byte string. This is essential because chunks may not arrive in order - a corrupted chunk 3 might be retried and received after chunks 5, 6, and 7 have already arrived. Dictionary insertion order in Python 3.7+ is preserved (not sorted), so without `sorted()` the byte string would be assembled in arrival order rather than the correct logical index order, producing a scrambled file even though every individual chunk passed its SHA-256 integrity check.
`sorted()` guarantees correctness regardless of the order chunks happen to arrive during the transfer.

### 19. Why random.random() Is Used in Corruption Simulation

`TransferProtocol._maybe_corrupt()` uses `random.random() < self._corruption_probability` to decide whether to corrupt a chunk. `random.random()` returns a float uniformly distributed in `[0.0, 1.0)`. Comparing it to `corruption_probability` gives exactly that probability of returning True: at `corruption_probability = 0.20`, exactly 20% of calls flip a byte. The actual flip is `data[flip_idx] ^= 0xFF`, which XORs every bit of one byte with 1, inverting all 8 bits. This guarantees the byte changes value, making the corrupted chunk reliably fail `ChunkData.verify()`. The original checksum is preserved via `ChunkData._from_raw()` so the mismatch is detectable, which then triggers the retry logic in the caller.
