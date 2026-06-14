swarm-share-lite

""Java CI with Gradle" (https://github.com/deadboyccc/swarm-share-lite/actions/workflows/gradle.yml/badge.svg)" (https://github.com/deadboyccc/swarm-share-lite/actions/workflows/gradle.yml)

""Dependabot Updates" (https://github.com/deadboyccc/swarm-share-lite/actions/workflows/dependabot/dependabot-updates/badge.svg)" (https://github.com/deadboyccc/swarm-share-lite/actions/workflows/dependabot/dependabot-updates)

""Automatic Dependency Submission" (https://github.com/deadboyccc/swarm-share-lite/actions/workflows/dependency-graph/auto-submission/badge.svg)" (https://github.com/deadboyccc/swarm-share-lite/actions/workflows/dependency-graph/auto-submission)

P2P chunk-based file distribution with logarithmic peer scaling.

Traditional file distribution bottlenecks at the source. swarm-share-lite turns every node that receives a chunk into a server, allowing throughput to grow as the swarm expands.

---

Features

- Chunk-based file distribution
- Parallel downloads from multiple peers
- SHA-256 chunk verification
- Resume interrupted downloads
- Virtual-thread concurrency (Java 25)
- Random-access writes using "FileChannel"
- Clean layered architecture
- Simple TCP wire protocol

---

Why?

Imagine distributing a 5 GB Linux ISO to 14 machines on a LAN.

Traditional Approach

One source sends the file to every machine:

Seeder → Peer 1
Seeder → Peer 2
Seeder → Peer 3
...
Seeder → Peer 14

Approximately 70 GB must be transferred by the source.

Swarm Approach

Round 1: 2 peers
Round 2: 4 peers
Round 3: 8 peers
Round 4: 16 peers

Every completed peer immediately becomes a source for others.

Benefits:

- Lower load on the original seeder
- Faster distribution
- Better scalability as peer count grows

---

How It Works

1. Manifest Generation

The seeder splits a file into fixed-size chunks and generates a manifest describing:

- File metadata
- Chunk boundaries
- SHA-256 checksums

Example:

{
  "fileHash": "e3b0c44298fc1c...",
  "fileName": "ubuntu-25.04.iso",
  "totalSize": 5368709120,
  "chunkSize": 1048576
}

2. Peer Discovery

Peers exchange chunk availability using a compact "BitSet".

A set bit means:

bit[i] = 1

The peer owns chunk "i".

3. Parallel Download

Missing chunks are downloaded concurrently from any peer that owns them.

Each chunk is:

1. Requested
2. Downloaded
3. Hash verified
4. Written directly to its file offset

4. Resume Support

On restart:

- Existing chunks are rehashed
- Valid chunks are marked complete
- Missing chunks continue downloading

No separate metadata database is required.

5. Peer Promotion

The moment a chunk is verified, the peer can begin serving it to others.

---

Why Java 25 & Virtual Threads?

Traditional thread-per-connection designs become expensive at scale.

Virtual threads allow:

- Massive concurrency
- Natural blocking I/O
- Minimal memory overhead
- Simpler code than callback-based approaches

This project intentionally embraces Project Loom to keep the implementation readable while remaining highly concurrent.

---

Quick Start

Requirements

- Java 25+
- Gradle 8+
- Linux (Ubuntu / Fedora)

Seed a File

./gradlew cli:run --args="seed --file ubuntu-25.04.iso --port 7070"

Example output:

Listening on port 7070
Manifest hash: e3b0c44298fc1c...
Total chunks: 5120

Download a File

./gradlew cli:run --args="leech \
  --manifest manifest.json \
  --peer 192.168.1.10:7070 \
  --peer 192.168.1.11:7070 \
  --output ubuntu-25.04.iso"

Example output:

Downloaded 2048 / 5120 chunks... [████░░░░░░] 40%
Downloaded 5120 / 5120 chunks... [██████████] 100%
Transfer complete. Verifying full file hash...
✓ File verified.

Interrupted downloads automatically resume.

---

Architecture

CLI
 │
 ▼
Transfer Manager
 ├── StorageProvider
 └── PeerConnector
      │
      ▼
   TCP Network

Modules

Module| Responsibility
"core"| Domain model and business rules
"manifest"| Manifest generation and serialization
"storage"| File storage and checksum validation
"networking"| TCP communication and protocol framing
"transfer"| Download orchestration and scheduling
"cli"| Command-line interface

Design Principle

The domain layer never depends on infrastructure.

Examples:

- TCP → TLS without changing orchestration logic
- Local disk → S3-backed storage without changing domain code

---

Protocol

Message Types

Code| Description
"0x01"| Request piece map
"0x02"| Piece map response
"0x03"| Request chunk
"0x04"| Chunk response

The protocol uses:

- Binary framing
- Big-endian integers
- Length-prefixed fields

---

Chunk Lifecycle

MISSING
   │
   ▼
SCHEDULED
   │
   ▼
IN_FLIGHT
   │
   ▼
VERIFYING
   │
   ▼
VERIFIED
   │
   ▼
WRITTEN

(hash mismatch / timeout)
          │
          ▼
       MISSING

---

Testing

Run all tests:

./gradlew test

Unit Tests

- Manifest generation
- Checksum verification
- Chunk state tracking
- Transfer orchestration

Integration Tests

End-to-end transfer tests verify:

- Chunk exchange
- File reconstruction
- Hash correctness

---

Non-Goals (v1)

- No DHT-based peer discovery
- No encryption/TLS
- No GUI
- No persistent peer database

---

Build

git clone <repo>
cd swarm-share-lite

./gradlew build
./gradlew test

---

Java 25 • Virtual Threads • Linux • MIT License