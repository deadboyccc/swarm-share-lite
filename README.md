
# swarm-share-lite

[![Java CI with Gradle](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/gradle.yml/badge.svg)](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/gradle.yml)

**P2P chunk-based file distribution with logarithmic peer scaling.**

Traditional file distribution bottlenecks at the source. swarm-share turns every node that receives a chunk into a server, enabling exponential throughput growth as the swarm expands.

## The Problem

Distributing a 5 GB Linux ISO across 14 machines in a LAN:

**Sequential approach (1–2 sources):**
- Source uploads to machine 2: ~5 GB
- Source uploads to machine 3: ~5 GB
- ... repeat 12 more times
- **Total time: ~70 GB transferred**

**Parallel swarm approach:**
- Round 1: Sources 1–2 each serve 2 machines → 4 total with the file
- Round 2: Sources 1–4 each serve 2 machines → 8 total
- Round 3: Sources 1–8 each serve 2 machines → 16 total (all done)
- **Total time: ~20 GB transferred** (mostly parallel)

The difference: **3.5× faster** with just 14 machines. At 100 nodes, the gap widens further.

## How It Works

### 1. Manifest Creation (Seeder)

The seeder splits the file into fixed-size chunks (default 1 MB) and creates a **manifest** — a JSON contract describing the transfer:

```json
{
  "fileHash": "e3b0c44298fc1c...",
  "fileName": "ubuntu-25.04.iso",
  "totalSize": 5368709120,
  "chunkSize": 1048576,
  "chunks": [
    {
      "index": 0,
      "offset": 0,
      "size": 1048576,
      "sha256": "abc123def456..."
    },
    ...
  ]
}
```

All peers must agree on this manifest to participate.

### 2. Peer Discovery (Leechers)

Before downloading, each leecher asks known peers: *"Which chunks do you have?"*

Peers respond with a **BitSet** — a compact bitmap where bit `i` = 1 means the peer holds chunk `i`. This one-time query allows intelligent peer selection.

### 3. Parallel Download

Each leecher spawns a virtual thread per missing chunk and downloads from whoever holds it:
- **Chunk 0** → fetch from peer A (has it) → verify SHA-256 → write to disk
- **Chunk 1** → fetch from peer B (has it) → verify SHA-256 → write to disk
- **Chunk 2** → fetch from peer C (has it) → verify SHA-256 → write to disk
- ... all in parallel

Writes go directly to the correct byte offset — no sequential assembly step.

### 4. Resume Support

Restart the download? The leecher scans the output file:
- For each chunk on disk, recompute its SHA-256
- If hash matches the manifest, mark as held
- Resume from the next missing chunk

No metadata file needed; the file itself is the recovery log.

### 5. Serving (All Nodes)

As soon as a node completes a chunk and verifies it, it becomes a server for that chunk. Incoming TCP connections are handled by a lightweight ServerSocket with one virtual thread per client. No thread pool sizing — thousands of concurrent clients are routine.

## Why Java 25 & Virtual Threads?

**Traditional threading (OS threads):**
- Each thread ~1 MB stack
- 10,000 concurrent downloads = 10 GB overhead + kernel scheduling tax
- Impractical

**Virtual threads (Project Loom):**
- Each thread ~1 KB stack
- 10,000 concurrent downloads = 10 MB overhead, no kernel overhead
- Blocking I/O (socket.read(), channel.write()) parks the virtual thread, freeing the carrier OS thread for other work
- Write natural blocking code; Loom handles scheduling

Result: **Simple, readable concurrent code at massive scale.**

## Quick Start

### Prerequisites

- Java 25+
- Gradle 8+
- Linux (Ubuntu / Fedora)

### Seed a File

```bash
./gradlew cli:run --args="seed --file ubuntu-25.04.iso --port 7070"
```

Output:
```
Listening on port 7070
Manifest hash: e3b0c44298fc1c...
Total chunks: 5120
```

The seeder listens indefinitely, serving chunks on request.

### Download from Peers

```bash
./gradlew cli:run --args="leech \
  --manifest manifest.json \
  --peer 192.168.1.10:7070 \
  --peer 192.168.1.11:7070 \
  --output ubuntu-25.04.iso"
```

Output:
```
Downloaded 2048 / 5120 chunks... [████░░░░░░] 40%
Downloaded 5120 / 5120 chunks... [██████████] 100%
Transfer complete. Verifying full file hash...
✓ File verified. Hash: e3b0c44298fc1c...
```

Interrupt and re-run — the download resumes from where it left off.

## Architecture

Clean layered design with strict dependency boundaries:

```
┌─────────────────────────────────────────────────┐
│ CLI (seed / leech commands)                     │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│ Transfer Manager (orchestration layer)          │
│ - Coordinates downloads                         │
│ - Manages retry & backpressure                  │
│ - Tracks chunk state                            │
└──────────┬──────────────────────────┬───────────┘
           │                          │
   ┌───────▼──────────┐      ┌────────▼──────────┐
   │ StorageProvider  │      │ PeerConnector    │
   │ (interface)      │      │ (interface)      │
   └───────┬──────────┘      └────────┬──────────┘
           │                          │
   ┌───────▼──────────┐      ┌────────▼──────────┐
   │ FileChannel      │      │ TCP Networking   │
   │ Storage          │      │ (socket-based)   │
   └──────────────────┘      └──────────────────┘
```

**Module structure:**

- **core/** — Pure Java domain model (Records, sealed types, value objects). No I/O.
- **storage/** — FileChannel-based random-access writes + SHA-256 verification.
- **networking/** — TCP binary framing, ServerSocket listener, async chunk fetch.
- **transfer/** — Orchestrator: coordinates parallel downloads, state tracking, retry logic.
- **manifest/** — Builds manifests from files, JSON serialization.
- **cli/** — Entry points: `seed` and `leech` commands.

**Key principle:** The domain never imports infrastructure. Swapping TCP for TLS or FileChannel for S3 requires zero changes to the orchestrator.

## Testing

### Unit Tests

Test core logic with in-memory fakes — no disk, no network:

```bash
./gradlew test
```

Examples:
- `ManifestBuilderTest` — chunk splitting, checksum computation
- `ChunkStateTrackerTest` — state machine transitions, atomic operations
- `TransferManagerTest` — orchestration logic with fake storage/network

### Integration Tests

Two-node end-to-end test on loopback:
1. Start seeder in a background virtual thread
2. Run leecher, download all chunks
3. Verify output file byte-for-byte match

## Phased Roadmap

| Phase | Focus |
|-------|-------|
| **1** | Domain modeling, TDD foundations |
| **2** | FileChannel random-access storage |
| **3** | TCP binary framing & wire protocol |
| **4** | Virtual thread concurrency, backpressure |
| **5** | Multi-peer orchestration, CLI |
| **6** | Failure recovery, exponential backoff retry |

## Current Status

**Phase 2 — In Progress**
- ✅ Domain types (Records, sealed interfaces)
- ✅ FileChannelStorage (preallocate, write, read)
- ✅ ChecksumVerifier (SHA-256 per chunk)
- ✅ Unit test coverage for storage layer
- 🔄 BitSet piece map exchange
- ⏳ Network layer (next)

## Design Highlights

### Binary TCP Protocol

Minimal framing overhead. Messages:

| Type | Purpose |
|------|---------|
| `0x01` | Request piece map (which chunks do you hold?) |
| `0x02` | Respond with BitSet |
| `0x03` | Request chunk data |
| `0x04` | Respond with chunk bytes (or error) |

Big-endian integers, length-prefixed strings. No complexity, no surprises.

### State Machine (Per Chunk)

```
MISSING → SCHEDULED → IN_FLIGHT → VERIFYING → VERIFIED → WRITTEN
                                     ↓ (hash mismatch or timeout)
                                   MISSING (retry)
```

Transitions are atomic CAS operations — no two threads can schedule the same chunk.

### Backpressure & Semaphore

Without limits, 10,000 virtual threads might saturate network buffers simultaneously. A `Semaphore` caps inflight downloads (default: 32), allowing controlled resource usage:

```java
semaphore.acquireUninterruptibly();
try {
    downloadChunk(...);
} finally {
    semaphore.release();
}
```

Virtual threads park cheaply while waiting on the semaphore — no busy-spinning.

## Non-Goals (v1)

- **No DHT / peer discovery** — Static peer list only. Future: mDNS or config file.
- **No encryption** — Plain TCP. Interface boundary exists for TLS wrapper.
- **No GUI** — CLI only.
- **No persistence of peer state** — Manifests are ephemeral; BitSets recomputed on restart.

## Building & Running

```bash
# Clone and build
git clone <repo>
cd swarm-share-lite
./gradlew build

# Run tests
./gradlew test

# Run CLI (see Quick Start above)
./gradlew cli:run --args="seed --file ubuntu-25.04.iso --port 7070"
```

---

**Language:** Java 25 | **Platform:** Linux | **License:** MIT
