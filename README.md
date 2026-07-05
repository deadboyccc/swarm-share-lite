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
...repeat 11 more times
Total: ~65 GB transferred by the seeder alone
```

**Swarm (every completed peer becomes a source):**
```
Round 1: 1 source  → 2 machines have the file
Round 2: 2 sources → 4 machines have the file
Round 3: 4 sources → 8 machines have the file
Round 4: 8 sources → all 14 machines have the file
Total: ~35 GB transferred (mostly in parallel)
```

**Result: ~2× faster with 14 machines. The gap widens at 100+ nodes.**

---

## How It Works

### 1 — Manifest creation

The seeder splits the file into fixed-size chunks (default 1 MB) and produces a manifest — the shared contract all peers use to participate. The manifest is written to disk as `<file>.manifest.json` alongside the source file, so it can be handed to leechers by any means (scp, USB, a shared folder, etc.) before a download begins:

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

### Build

```bash
./gradlew build
```

All tests pass, including end-to-end networking tests.

### CLI Commands

The CLI is built on [picocli](https://picocli.info/) and exposes three subcommands: `seed`, `build-manifest`, and `download`. Run with no arguments, or with `--help`, to see full usage for any command:

```bash
java -cp cli/build/classes/java/main io.swarmshare.cli.Main --help
java -cp cli/build/classes/java/main io.swarmshare.cli.Main seed --help
```

#### Seed a file

Start a seeder that builds a manifest, writes it to `<file>.manifest.json`, and listens on the given port to serve all chunks:

```bash
java -cp cli/build/classes/java/main io.swarmshare.cli.Main seed <filepath> <port>

# Example
java -cp cli/build/classes/java/main io.swarmshare.cli.Main seed /tmp/ubuntu-25.04.iso 7070
```

Output:
```
Manifest written to /tmp/ubuntu-25.04.iso.manifest.json
Seeder running. fileHash=e3b0c44298fc1c7149efe3e05d7734f... port=7070
```

The seeder blocks and keeps serving until interrupted (Ctrl+C).

#### Build a manifest

Compute, print, and persist a manifest for a file without serving it. Useful when you want to generate the manifest ahead of time, or distribute it separately from starting the seeder:

```bash
java -cp cli/build/classes/java/main io.swarmshare.cli.Main build-manifest <filepath> [chunkSize] [-o outputPath]

# Example with default 1 MB chunks — writes ubuntu-25.04.iso.manifest.json
java -cp cli/build/classes/java/main io.swarmshare.cli.Main build-manifest /tmp/ubuntu-25.04.iso

# Example with custom chunk size (512 KB)
java -cp cli/build/classes/java/main io.swarmshare.cli.Main build-manifest /tmp/ubuntu-25.04.iso 524288

# Example writing the manifest to a custom path
java -cp cli/build/classes/java/main io.swarmshare.cli.Main build-manifest /tmp/ubuntu-25.04.iso -o /tmp/shared/manifest.json
```

Output:
```
fileHash=e3b0c44298fc1c7149efe3e05d7734f...
fileName=ubuntu-25.04.iso
totalSize=5368709120
chunkSize=1048576
totalChunks=5120
manifestPath=/tmp/ubuntu-25.04.iso.manifest.json
```

#### Download a file

Fetch a file from one or more peers, using a manifest produced by `seed` or `build-manifest`:

```bash
java -cp cli/build/classes/java/main io.swarmshare.cli.Main download <manifestFile> <output> <peerHost:peerPort>...

# Example: download from a single seeder
java -cp cli/build/classes/java/main io.swarmshare.cli.Main download \
    ubuntu-25.04.iso.manifest.json /tmp/ubuntu-25.04.iso 192.168.1.10:7070

# Example: download from multiple peers at once
java -cp cli/build/classes/java/main io.swarmshare.cli.Main download \
    ubuntu-25.04.iso.manifest.json /tmp/ubuntu-25.04.iso 192.168.1.10:7070 192.168.1.11:7070
```

Output:
```
Downloading ubuntu-25.04.iso (5120 chunks) from 2 peer(s)...
Download complete: /tmp/ubuntu-25.04.iso
```

`download` blocks until every chunk has been fetched, verified, and written, or throws if chunks remain unreachable after retries. If `output` already has partial or complete data on disk (e.g. a retried download), matching chunks are verified and skipped rather than re-fetched — see [Resume support](#4--resume-support).

#### End-to-end example

```bash
# Machine A — seed the file
java -cp cli/build/classes/java/main io.swarmshare.cli.Main seed /tmp/ubuntu.iso 7070

# Copy /tmp/ubuntu.iso.manifest.json to Machine B (scp, USB, shared folder, etc.)

# Machine B — download using that manifest and Machine A's address
java -cp cli/build/classes/java/main io.swarmshare.cli.Main download \
    ubuntu.iso.manifest.json /tmp/ubuntu.iso 192.168.1.10:7070
```

### Programmatic Usage

The orchestrator is designed to be integrated into larger applications. Use `TransferManager` directly:

```java
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.manifest.ManifestSerializer;
import io.swarmshare.networking.TcpPeerConnector;
import io.swarmshare.storage.FileChannelStorage;
import io.swarmshare.transfer.TransferManager;

Path manifestFile = Path.of("manifest.json");
List<PeerInfo> peers = List.of(
    new PeerInfo(UUID.randomUUID(), new InetSocketAddress("192.168.1.10", 7070)),
    new PeerInfo(UUID.randomUUID(), new InetSocketAddress("192.168.1.11", 7070))
);

Manifest manifest = new ManifestSerializer().read(manifestFile);
var storage = new FileChannelStorage(Path.of("output.iso"));
var connector = new TcpPeerConnector();
var manager = new TransferManager(manifest, peers, storage, connector);

// Blocks until all chunks are downloaded, verified, and written
manager.start();
```

For fast testing without real network I/O, use test doubles:

```java
var storage = new InMemoryStorage();  // in-memory chunks
var connector = new FakePeerConnector(manifest, testChunkMap);  // pre-populated
var manager = new TransferManager(manifest, peers, storage, connector);
manager.start();  // completes immediately in tests
```

---

## Modules & Design

This project is structured as a **Gradle multi-module build** with a strict dependency hierarchy:

```
┌─────────────────────────────────────────────────┐
│ CLI ← entry points (Main.java)                  │
└────────────────┬────────────────────────────────┘
                 │ depends on
┌────────────────▼────────────────────────────────┐
│ Transfer ← orchestration (TransferManager)      │
└──────────┬──────────────────────┬───────────────┘
           │                      │
    depends on              depends on
           │                      │
   ┌───────▼──────────┐  ┌────────▼──────────┐
   │ Networking       │  │ Storage           │
   │ (TCP framing)    │  │ (FileChannel I/O) │
   └───────┬──────────┘  └────────┬──────────┘
           │                      │
    depends on              depends on
           │                      │
   ┌───────▴──────────┬───────────▴──────────┐
   │                  │                      │
   │              Core (domain + ports)      │
   │    (no I/O, no framework dependencies)  │
   │                                          │
   │    - Domain records (Manifest, etc.)    │
   │    - Sealed interfaces (PeerConnector)  │
   │    - Value objects (ChunkId, etc.)      │
   └──────────────────────────────────────────┘
```

| Module | Responsibility | Key Files |
|--------|---|---|
| **core** | Pure domain model and abstract ports | `ChunkId`, `Manifest`, `PeerConnector`, `StorageProvider`, `HasherPort` |
| **manifest** | Chunk splitting, JSON serialization,  validation | `ManifestBuilder`, `ManifestSerializer`, `ManifestValidator` |
| **storage** | File I/O via `FileChannel`, SHA-256 verification | `FileChannelStorage` |
| **networking** | TCP binary protocol, chunk fetching, peer serving | `FrameEncoder`, `FrameDecoder`, `TcpPeerConnector`, `TcpChunkServer` |
| **transfer** | Orchestration, concurrency control, retry logic | `TransferManager`, `ChunkStateTracker`, `RetryPolicy` |
| **cli** | User-facing commands for operations | `Main` (picocli subcommands: `seed`, `build-manifest`, `download`) |

**Design principle:** The domain (`core`) layer has zero dependencies on I/O or frameworks. This keeps business logic testable and portable — swapping TCP for TLS or `FileChannel` for S3 requires changes only in `networking` and `storage`, never in `transfer` or `core`.

**CLI dependency:** The `cli` module depends on [picocli](https://picocli.info/) for argument parsing, `--help` generation, and subcommand dispatch. Add it to `cli/build.gradle`:

```gradle
dependencies {
    implementation 'info.picocli:picocli:4.7.6'
    annotationProcessor 'info.picocli:picocli-codegen:4.7.6' // optional, for GraalVM/native-image metadata
}
```

---

## Developing

### Code Organization

Each module follows this structure:

```
module/
  src/
    main/java/io/swarmshare/MODULE/
      ...java files
    test/java/io/swarmshare/MODULE/
      ...Test.java files
      ...Fake*.java (test doubles)
```

### Running Tests

```bash
# Run all tests
./gradlew test

# Run a specific test
./gradlew test --tests "*FrameEncoderDecoderTest*"

# Run with verbose output
./gradlew test --info
```

Tests are organized by concern:

- **Unit tests** (`*Test.java`) use `JUnit 5`, `AssertJ`, and fast test doubles
- **Integration tests** (e.g., `TwoNodeIntegrationTest`) use real sockets and file I/O but run on loopback
- **Test doubles** (`Fake*.java`, `InMemoryStorage`) allow testing without I/O

### Adding a New Feature

1. **Write a failing test first** — define the behavior you want
2. **Implement in the appropriate layer** — respect the dependency hierarchy
3. **Add Javadoc** — explain the "why", not just the "what"
4. **Run full test suite** — ensure no regressions

**Example:** Adding connection pooling to `TcpPeerConnector`

1. Write tests in `networking/src/test/java/.../TcpPeerConnectorTest.java`
2. Modify `TcpPeerConnector` without changing its interface
3. Add testable configuration (poolSize, timeout) to the constructor
4. Document concurrency assumptions in the class Javadoc

**Example:** Adding a new CLI subcommand

1. Add a `static final class` implementing `Callable<Integer>` inside `Main`, annotated with `@Command(name = "...")`
2. Register it in the `subcommands = { ... }` array on the top-level `@Command`
3. Bind arguments with `@Parameters` (positional) or `@Option` (flags like `-o`/`--output`)
4. picocli handles `--help`, usage text, and type conversion — no manual `args[]` parsing needed

### Code Style & Standards

This project follows the standards in `AGENTS.md`:

- **Java 25 idioms:** Records, sealed types, pattern matching, virtual threads
- **Immutability:** Prefer records and defensive copies; minimize mutable state
- **Testing:** TDD — RED → GREEN → REFACTOR
- **Documentation:** Javadoc for public APIs, inline comments for non-obvious logic
- **Error handling:** Fail fast, preserve root causes, never silently swallow exceptions
- **Performance:** Correctness > Simplicity > Readability > Optimization (in that order)

### Architecture Rules

```
                    Domain
                       ↑
Orchestration ← (transfer module)
   Transfer depends on but never exposes:
- Infrastructure (networking, storage)
- Frameworks (Jackson, Gradle, picocli)

Networking and Storage depend on domain
but are independent of each other
```

Violation example: If `TransferManager` ever imports from `io.swarmshare.networking`, the architecture splits and becomes harder to test.

---

## Why Virtual Threads

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

**Phase 6 — Complete Implementation with Full CLI**

| | Item |
|---|---|
| ✅ | Domain types (Records, sealed interfaces, value objects) |
| ✅ | `ManifestBuilder`: single-pass chunk splitting with streaming I/O |
| ✅ | `ManifestSerializer` / `ManifestValidator`: JSON persistence and validation |
| ✅ | `FileChannelStorage`: NIO random-access writes, SHA-256 verification, resume support |
| ✅ | `ChecksumVerifier` (Sha256): constant-time verification, no timing side-channels |
| ✅ | Unit test coverage for all production classes |
| ✅ | `TcpChunkServer`: virtual-thread-per-connection listener |
| ✅ | `TcpPeerConnector`: async chunk and piece-map fetching |
| ✅ | `FrameEncoder` / `FrameDecoder`: binary TCP framing |
| ✅ | `TransferManager`: orchestration with parallel downloads and backpressure |
| ✅ | `ChunkStateTracker`: atomic state transitions via CAS |
| ✅ | `RetryPolicy`: exponential backoff with virtual thread sleep |
| ✅ | Integration tests: two-node end-to-end over loopback TCP |
| ✅ | CLI (picocli): `seed`, `build-manifest`, and `download` commands |
| ✅ | Manifest persistence: `seed` and `build-manifest` write `<file>.manifest.json` |
| ✅ | Comprehensive Javadoc and inline documentation |

### Reality

This is a **production-grade reference implementation** of a P2P chunk-based file distribution system. Every layer is tested, documented, and idiomatic modern Java 25. The CLI now supports the full seed → distribute manifest → download loop end-to-end.

---

## Extending the System

Because this project respects strict architectural boundaries, extending it is straightforward:

### Add TLS encryption (without touching domain or orchestration)

```java
// networking/src/main/java/.../TlsPeerConnector.java
public final class TlsPeerConnector implements PeerConnector {
    private final SSLContext sslContext;
    // ...implement fetchChunkAsync and fetchPieceMapAsync with TLS
}

// Update CLI to select between TcpPeerConnector and TlsPeerConnector
// TransferManager never changes; it only knows PeerConnector interface
```

### Add S3 backend (without touching domain or transfer)

```java
// storage/src/main/java/.../S3Storage.java
public final class S3Storage implements StorageProvider {
    private final S3Client s3;
    // ...implement preallocateSpace, writeChunk, readChunk, checkExistingChunks
}

// Update CLI to select between FileChannelStorage and S3Storage
// TransferManager never changes; it only knows StorageProvider interface
```

### Add peer discovery via mDNS (without touching domain or transfer)

```java
// networking/src/main/java/.../MdnsPeerDiscovery.java
public final class MdnsPeerDiscovery {
    public List<PeerInfo> discoverPeers(String serviceType) { ... }
}

// Instantiate peers via mDNS in CLI, pass them to TransferManager
// TransferManager knows nothing about discovery; it only knows a List<PeerInfo>
```

All three extensions plug in via **interface boundaries** — no changes to core business logic needed.

---

## Non-Goals (v1)

- **No DHT / peer discovery** — static peer list only, supplied as `host:port` arguments to `download`; future: mDNS or config file
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
