# Peer-to-Peer File Transfer and Integrity Verification System

## 1. Project Title and Overview

This project implements a Python OOP simulation of a simplified Peer-to-Peer (P2P) file transfer network — conceptually similar to BitTorrent, but requiring no real network sockets. Files are fragmented into fixed-size chunks, each protected with a SHA-256 checksum. A central tracker node maintains a registry of which peers hold which files. When a downloader requests a file, it retrieves chunks from available seeders; if any chunk arrives corrupted (simulated by randomly flipping a byte), the system automatically retries that chunk from a different peer. A final whole-file SHA-256 verification confirms the downloaded file is identical to the original. The system solves the real-world problem of reliable data distribution across unreliable networks, demonstrating fault-tolerant chunk-based transfer, integrity verification, peer discovery, and bandwidth-aware peer ranking — all concepts foundational to distributed computing and content delivery networks.

---

## 2. Team Members

| Full Name | Matric Number | GitHub Username |
|---|---|---|
| Olayinka Moyinoluwa | CPE/2023/1086 | @oakin4809-droid |
| Olayiwola Idowu Lukman | CPE/2023/1087 | @Guruteck |
| _(Member 3 — fill in)_ | _(matric)_ | _(github)_ |
| Olusola Promise Nifemi | CPE/2023/1089 |@Promise12350|
| _(Member 5 — fill in)_ | _(matric)_ | _(github)_ |
| Oluwada-ire Samuel Boluwatife | CPE/2023/1091 | Samueldaire |
| Oluwadare Gideon  | Cpe/2023/1092 | blvck804 |
| _(Member 8 — fill in)_ | _(matric)_ | _(github)_ |
| Omoniyi Joseph Ayomide| CPE/2023/1095 | omoniyijoseph700-glitch|
| Omoniyi abdul-muiz opeyemi  | CPE/2023/1094 | Softwork-gif |
| Omoyeni Samuel Ayomide | CPE/2023/1096 | samuelomoyeni09-pixel |
| onyekwelu Faith solojah| CPE/2023/1097 | _SolojahF |
| _(Member 13 — fill in)_ | _(matric)_ | _(github)_ |
| Ovie promise ogagaoghene  | CPE/2023/1099 | ovieogagaoghene |

---

## 3. OOP Concepts Demonstrated

| OOP Concept | Location in Code | Week |
|---|---|---|
| Classes with constructors, `__str__`, `__repr__` | All classes in `src/` | Week 1 |
| Encapsulation with private backing attributes | `src/chunk_data.py` — `ChunkData._data`, `_checksum` | Week 2 |
| Validated `@property` (read-only, no setters) | `src/chunk_data.py` — all properties; `src/file_metadata.py` | Week 2 |
| Custom exception hierarchy with structured fields | `src/exceptions.py` — `P2PError`, `ChecksumMismatchError`, `PeerNotFoundError`, `TransferTimeoutError`, `DuplicateFileError` | Week 2 |
| Abstract Base Class (`ABC`) | `src/nodes.py` — `Node`; `src/protocols.py` — `TransferProtocol` | Week 3 |
| `@abstractmethod` enforcement | `Node.send()`, `Node.receive()`; `TransferProtocol.initiate()`, `transfer_chunk()`, `finalise()` | Week 3 |
| Single inheritance | `src/nodes.py` — `DataPeerNode(Node)`, `MetadataTrackerNode(Node)` | Week 3 |
| Multiple inheritance with cooperative `super()` | `src/nodes.py` — `SeedNode(DataPeerNode, RateLimiterMixin)` | Week 3 |
| `@total_ordering` for comparison operators | `src/file_metadata.py` — `FileMetadata` (`__eq__`, `__lt__`, `__hash__`) | Week 4 |
| Operator overloading `__len__`, `__contains__`, `__iter__` | `src/swarm.py` — `Swarm` | Week 4 |
| Duck typing via `typing.Protocol` pattern | `src/protocols.py` — `transfer_chunk(provider=None)` accepts any object with `request_chunk()` | Week 4 |
| Polymorphism — same call across subclasses | `PushProtocol` and `PullProtocol` both implement `transfer_chunk()` — called identically in `main.py` | Week 4 |
| UML class diagram with all relationship types | `uml/class_diagram.puml` — generalization, composition, aggregation, dependency, realization | Week 5 |

---

## 4. System Architecture

![UML Class Diagram](uml/class_diagram.png)

The system is structured around two inheritance hierarchies. The `Node` abstract base class defines the network participation contract (`send`, `receive`), with `DataPeerNode` and `MetadataTrackerNode` as concrete implementations and `SeedNode` as a multiple-inheritance leaf that also inherits `RateLimiterMixin`. The `TransferProtocol` abstract base class defines the chunk-delivery contract, with `PushProtocol` and `PullProtocol` as two concrete strategies that differ in who controls delivery order.

The most important composition decision is `DataPeerNode ◆ ChunkData`: when a peer registers a file via `share_file()`, it creates `ChunkData` objects internally. These chunks exist only inside the peer and their lifecycle is tied to it — this is composition (filled diamond). By contrast, `TransferSession ○ Node` is aggregation (hollow diamond): a session holds references to nodes that were created independently and will outlive any single session.

The `Swarm` class aggregates `DataPeerNode` references; removing a peer from a swarm does not destroy the peer. The `MetadataTrackerNode` aggregates `Swarm` objects using a dictionary keyed by file hash. All monetary or size parameters are validated at construction time via `@property` with `ValueError` on bad input.

---

## 5. How to Run

```bash
# 1. Clone the repository
git clone https://github.com/<your-username>/p2p_file_transfer.git
cd p2p_file_transfer

# 2. Create and activate a virtual environment
python -m venv venv

# On Windows:
venv\Scripts\activate

# On Linux/Mac:
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the main demo
python main.py

# 5. Run the test suite
pytest tests/ -v
```

---

## 6. Sample Output

```
============================================================
  CPE 310 — Group 7 — P2P File Transfer Demo
============================================================

[Tracker]  MetadataTrackerNode(id='tracker-01', addr='192.168.0.1:6969')
[Seeder]   SeedNode(id='seed-01', addr='192.168.0.10:8080')

[Files registered on seed]
   FileMetadata('notes.txt', 4650B, 19 chunks x 256B)
   FileMetadata('firmware.bin', 5120B, 10 chunks x 512B)
   FileMetadata('tiny.txt', 10B, 1 chunks x 256B)

[Peers]    DataPeerNode(id='peer-A', addr='192.168.0.20:8080')
           DataPeerNode(id='peer-B', addr='192.168.0.30:8080')

[Step 1] peer-A downloading notes.txt via PushProtocol (corruption_probability=0.20)...
     [!] Chunk 2 corrupted from seed-01 — retrying (attempt 1)...
     [!] Chunk 7 corrupted from seed-01 — retrying (attempt 2)...

  ──────────────────────────────────────────────────────
  Transfer Report for peer-A using PushProtocol
  ──────────────────────────────────────────────────────
  Total chunks transferred : 19
  Chunks that needed retry : 2
  Effective bytes received : 4650 B
  Whole-file SHA-256 check : PASS ✓
  ──────────────────────────────────────────────────────

[Swarm]    Swarm(file=a3f8c1d2..., peers=2)
   len(swarm)               : 2
   "seed-01" in swarm       : True
   "unknown" in swarm       : False
```

---

## 7. Known Limitations

- **No real networking:** All transfers are simulated via Python object method calls. There are no actual TCP/UDP sockets, so the system cannot transfer files between real machines on a network.
- **In-memory storage only:** Files are stored as `bytes` objects in memory. There is no disk persistence; all data is lost when the program exits.
- **Single-threaded:** Transfers happen sequentially, not in parallel. A real P2P client would download chunks from multiple peers simultaneously using threads or async I/O.
- **Bandwidth simulation is simplified:** `RateLimiterMixin` tracks bytes sent but does not simulate actual time delays. The rate limiter enforces a counter but is not tied to a real clock.
- **UML PNG generation:** The `uml/class_diagram.png` requires PlantUML to be installed locally to generate from `class_diagram.puml`. The `.puml` source is complete and accurate.

---

## 8. References

- Python 3 Documentation — `abc` module: https://docs.python.org/3/library/abc.html
- Python 3 Documentation — `hashlib` module: https://docs.python.org/3/library/hashlib.html
- Python 3 Documentation — `functools.total_ordering`: https://docs.python.org/3/library/functools.html#functools.total_ordering
- Python 3 Documentation — `uuid` module: https://docs.python.org/3/library/uuid.html
- BitTorrent Protocol Specification (conceptual reference): https://www.bittorrent.org/beps/bep_0003.html
- CPE 310 Lecture Notes, Weeks 1–5, Engr. Soladoye A.A., FUOYE 2025/2026
