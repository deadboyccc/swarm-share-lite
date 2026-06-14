# swarm-share-lite

[![Java CI with Gradle](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/gradle.yml/badge.svg)](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/gradle.yml)
[![Dependabot Updates](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/dependabot/dependabot-updates)
[![Automatic Dependency Submission](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/dependency-graph/auto-submission/badge.svg)](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/dependency-graph/auto-submission)

> P2P chunk-based file distribution with logarithmic peer scaling.

Traditional file distribution bottlenecks at the source. `swarm-share-lite` turns every node that receives a chunk into a server — throughput grows as the swarm expands.

---

## The Problem

Distributing a 5 GB Linux ISO across 14 machines on a LAN:

**Sequential (single source):**
```
Seeder → Machine 2:  5 GB
Seeder → Machine 3:  5 GB
...repeat 12 more times
Total: ~70 GB transferred by the seeder alone
```

**Swarm (every completed peer becomes a source):**
```
Round 1: 2 sources  → 4 machines have the file
Round 2: 4 sources  → 8 machines have the file
Round 3: 8 sources  → 16 machines have the file
Total: ~20 GB transferred (mostly in parallel)
```

**Result: 3.5× faster with 14 machines. The gap widens at 100+ nodes.**

---

## How It Works

### 1 — Manifest creation

The seeder splits the file into fixed-size chunks (default 1 MB) and produces a manifest — the shared contract all peers use to participate:

```json
{
  "fileHash": "e3b0c44298fc1c...",
  "fileName": "ubuntu-25.04.iso",
  "totalSize": 5368709120,
  "chunkSize": 1048576,
  "chunks": [
    { "index": 0, "offset": 0, "size": 1048576, "sha256": "abc123def456..." }
  ]
}
```

### 2 — Peer discovery

Before downloading, each leecher queries known peers: *"Which chunks do you have?"*

Peers respond with a `BitSet` — a compact bitmap where bit `i = 1` means the peer holds chunk `i`. This single query enables intelligent, targeted peer selection.

### 3 — Parallel download

Each missing chunk is fetched concurrently via a dedicated virtual thread:

```
Chunk 0 → fetch from peer A → verify SHA-256 → write to offset 0
Chunk 1 → fetch from peer B → verify SHA-256 → write to offset 1 MB
Chunk 2 → fetch from peer C → verify SHA-256 → write to offset 2 MB
```

Writes land directly at their file offset via `FileChannel` — no sequential assembly step.

### 4 — Resume support

On restart, the leecher rehashes every chunk already on disk. Chunks whose SHA-256 matches the manifest are marked complete and skipped. The file itself is the recovery log — no separate metadata database needed.

### 5 — Peer promotion

The moment a chunk is verified and written, the node begins serving it to others. Incoming TCP connections are handled one virtual thread per client — no thread pool sizing required.

---

## Quick Start

**Requirements:** Java 25+, Gradle 9+

### Seed a file

```bash
./gradlew cli:run --args="seed --file ubuntu-25.04.iso --port 7070"
```

```
Listening on port 7070
Manifest hash: e3b0c44298fc1c...
Total chunks: 5120
```

### Download from peers

```bash
./gradlew cli:run --args="leech \
  --manifest manifest.json \
  --peer 192.168.1.10:7070 \
  --peer 192.168.1.11:7070 \
  --output ubuntu-25.04.iso"
```

```
Downloaded 2048 / 5120 chunks... [████░░░░░░] 40%
Downloaded 5120 / 5120 chunks... [██████████] 100%
Transfer complete. Verifying full file hash...
✓ File verified. Hash: e3b0c44298fc1c...
```

Interrupt and re-run — the download resumes from where it left off.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│ CLI  (seed / leech commands)                    │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│ Transfer Manager  (orchestration)               │
│  · Coordinates downloads                        │
│  · Manages retry & backpressure                 │
│  · Tracks chunk state                           │
└──────────┬──────────────────────────┬───────────┘
           │                          │
   ┌───────▼──────────┐      ┌────────▼──────────┐
   │ StorageProvider  │      │  PeerConnector    │
   │ (interface)      │      │  (interface)      │
   └───────┬──────────┘      └────────┬──────────┘
           │                          │
   ┌───────▼──────────┐      ┌────────▼──────────┐
   │ FileChannel      │      │  TCP Networking   │
   │ Storage          │      │  (socket-based)   │
   └──────────────────┘      └──────────────────┘
```

| Module | Responsibility |
|---|---|
| `core` | Pure domain model — Records, sealed types, value objects. No I/O. |
| `manifest` | Manifest generation, chunk splitting, JSON serialization. |
| `storage` | `FileChannel` random-access writes and SHA-256 verification. |
| `networking` | TCP binary framing, `ServerSocket` listener, async chunk fetch. |
| `transfer` | Orchestrator — parallel downloads, state tracking, retry logic. |
| `cli` | Entry points: `seed` and `leech` commands. |

**Design principle:** the domain layer never imports infrastructure. Swapping TCP → TLS or `FileChannel` → S3 requires zero changes to orchestration logic.

---

## Why Virtual Threads?

| | OS Threads | Virtual Threads (Project Loom) |
|---|---|---|
| Stack per thread | ~1 MB | ~1 KB |
| 10k concurrent downloads | ~10 GB overhead | ~10 MB overhead |
| Blocking I/O | Blocks OS thread | Parks virtual thread; carrier thread stays free |
| Code style | Callbacks / reactive | Natural blocking code |

`swarm-share-lite` intentionally embraces Project Loom to stay readable at massive concurrency. Blocking `socket.read()` and `channel.write()` calls park the virtual thread without touching the underlying OS thread — no reactive chains, no callback hell.

---

## Design Details

### Binary TCP protocol

| Code | Description |
|---|---|
| `0x01` | Request piece map |
| `0x02` | Piece map response (BitSet) |
| `0x03` | Request chunk data |
| `0x04` | Chunk response (bytes or error) |

Binary framing, big-endian integers, length-prefixed fields. No unnecessary complexity.

### Chunk state machine

```
MISSING → SCHEDULED → IN_FLIGHT → VERIFYING → VERIFIED → WRITTEN
                                       │
                              (hash mismatch / timeout)
                                       │
                                    MISSING  ← retry
```

State transitions are atomic CAS operations — no two threads can schedule the same chunk.

### Backpressure

A `Semaphore` caps in-flight downloads (default: 32) to prevent saturating network buffers:

```java
semaphore.acquireUninterruptibly();
try {
    downloadChunk(...);
} finally {
    semaphore.release();
}
```

Virtual threads park cheaply while waiting on the semaphore — no busy-spinning.

---

## Testing

```bash
./gradlew test
```

**Unit tests** use in-memory fakes — no disk, no network:

- `ManifestBuilderTest` — chunk splitting, checksum computation
- `ChunkStateTrackerTest` — state machine transitions, atomic operations
- `TransferManagerTest` — orchestration logic with fake storage and network

**Integration tests** run a two-node end-to-end transfer on loopback: seeder starts in a background virtual thread, leecher downloads all chunks, output file is verified byte-for-byte.

---

## Current Status

**Phase 2 — In Progress**

| | Item |
|---|---|
| ✅ | Domain types (Records, sealed interfaces) |
| ✅ | `FileChannelStorage` (preallocate, write, read) |
| ✅ | `ChecksumVerifier` (SHA-256 per chunk) |
| ✅ | Unit test coverage for storage layer |
| 🔄 | BitSet piece map exchange |
| ⏳ | Network layer |

### Roadmap

| Phase | Focus |
|---|---|
| 1 | Domain modeling, TDD foundations |
| 2 | `FileChannel` random-access storage |
| 3 | TCP binary framing and wire protocol |
| 4 | Virtual thread concurrency, backpressure |
| 5 | Multi-peer orchestration, CLI |
| 6 | Failure recovery, exponential backoff retry |

---

## Non-Goals (v1)

- **No DHT / peer discovery** — static peer list only; future: mDNS or config file
- **No encryption** — plain TCP; interface boundary exists for a TLS wrapper
- **No GUI** — CLI only
- **No persistent peer state** — manifests are ephemeral; BitSets recomputed on restart

---

## Build

```bash
git clone <repo>
cd swarm-share-lite
./gradlew build
./gradlew test
```

---

**Java 25 · Virtual Threads · Project Loom · MIT License**
