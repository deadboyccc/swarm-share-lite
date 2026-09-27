# swarm-share-lite

[![Java CI with Gradle](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/gradle.yml/badge.svg)](https://github.com/deadboyccc/swarm-share-lite/actions/workflows/gradle.yml)

> P2P chunk-based file distribution with logarithmic peer scaling.

Traditional file distribution bottlenecks at the source. `swarm-share-lite` turns every peer that finishes downloading a chunk into a server for that chunk — throughput scales with the swarm instead of the seeder's bandwidth.

## How It Works

1. **Manifest** — the seeder splits the file into fixed-size chunks and writes a `<file>.manifest.json` describing hashes, sizes, and offsets.
2. **Discovery** — peers exchange `BitSet` piece-maps to find who has what.
3. **Parallel transfer** — missing chunks are fetched concurrently (one virtual thread each), verified via SHA-256, and written directly to their file offset.
4. **Resume** — on restart, existing chunks are rehashed and skipped; the file itself is the recovery log.
5. **Promotion** — a peer starts serving a chunk the moment it's verified.

Built on Java 25 virtual threads: natural blocking I/O code, no thread-pool tuning, cheap concurrency at scale.

## Quick Start

**Requirements:** Java 25+, Gradle 9+

```bash
./gradlew build

# Seed a file (writes ubuntu.iso.manifest.json, serves on port 7070)
java -cp cli/build/classes/java/main io.swarmshare.cli.Main seed /tmp/ubuntu.iso 7070

# Download using the manifest + peer address
java -cp cli/build/classes/java/main io.swarmshare.cli.Main download \
    ubuntu.iso.manifest.json /tmp/ubuntu.iso 192.168.1.10:7070
```

Also available: `build-manifest` (generate a manifest without seeding). Run any command with `--help` for full usage.

## Architecture

Strict dependency hierarchy, domain layer has zero I/O or framework dependencies:

```
cli → transfer → { networking, storage } → core
```

| Module | Responsibility |
|---|---|
| `core` | Domain model, ports (no I/O) |
| `manifest` | Chunk splitting, JSON (de)serialization |
| `storage` | `FileChannel` I/O, SHA-256 verification |
| `networking` | TCP binary protocol, chunk fetch/serve |
| `transfer` | Orchestration, concurrency, retries |
| `cli` | picocli commands: `seed`, `build-manifest`, `download` |

This separation means swapping TCP for TLS, or local disk for S3, only touches `networking`/`storage` — `transfer` and `core` never change.

## Testing

```bash
./gradlew test
```

Unit tests use in-memory fakes; integration tests run a real two-node transfer over loopback TCP.

## Status

Production-grade reference implementation — fully tested, documented, idiomatic Java 25. End-to-end seed → distribute → download loop is complete.

## Non-Goals (v1)

- No DHT/peer discovery — static peer list only
- No encryption — plain TCP (interface boundary exists for TLS)
- No GUI
- No persistent peer state

---

**Java 25 · Virtual Threads · MIT License**
