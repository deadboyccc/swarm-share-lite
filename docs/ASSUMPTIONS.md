# Assumptions and Decisions

[`ARCHITECTURE.md`](../ARCHITECTURE.md) is the specification of swarm-share-lite.
This file records every place where the specification was **ambiguous, silent, or
contradicted itself**, and what the code does instead — always the most
conservative reading that keeps documented behaviour working.

The decisions were taken against version 1.0 of the specification. Version 2.0
of `ARCHITECTURE.md` describes the system as built and already reflects them.
Entry IDs are stable; release notes refer to them.

**Contents**

- [Contradictions in the specification](#contradictions-in-the-specification)
- [Behaviour the specification left open](#behaviour-the-specification-left-open)
- [Command line](#command-line)
- [Resilience](#resilience)
- [Roadmap items deliberately not implemented](#roadmap-items-deliberately-not-implemented)
- [Packaging and process](#packaging-and-process)

---

## Contradictions in the specification

### A1 — Code listings vs. prose

**Spec (v1.0):** §4–§13 presented "full code" sketches.
**Decision:** The sketches were treated as illustrative. Prose, the wire-format
tables and the roadmap were authoritative. Where the code already deviated for a
documented reason — a streaming `ManifestBuilder` instead of `readAllBytes`,
`Sha256` behind a core `HasherPort` instead of a storage-module
`ChecksumVerifier` — the deviation stayed.

### A2 — Response frames carry no message-type byte

**Spec (v1.0):** The layout tables for `CHUNK_RESPONSE` and `PIECE_MAP_RESPONSE`
start with the status byte, and the client sketch reads the status first; the
`FrameEncoder` sketch wrote an extra type byte before it.
**Decision:** No type byte on responses. Two of the three sources agree, and on a
one-request connection the request/response pairing is implicit.

### A3 — When status `0x02 ERROR` is sent

**Spec (v1.0):** Defined, but never said when.
**Decision:** For an unknown message type (as the sketch did) and when the server
fails internally while serving (for example a storage error). The client then fails
fast and tries another peer instead of waiting for a timeout.

---

## Behaviour the specification left open

| ID | Topic | Decision | Why |
|----|-------|----------|-----|
| B1 | Manifest validation | `fileHash` and every chunk `sha256` must be 64 hex characters. Every chunk except the last is exactly `chunkSize`; the last is at most `chunkSize`. | §2 defines both hashes as SHA-256 and chunks as fixed-size slices. A malformed hash can never verify, so every chunk would be downloaded and then thrown away. |
| B2 | Server read timeout | Accepted sockets get a 10 s read timeout. | Mirrors the client's 10 s timeout. Without it an idle or half-open client holds a handler and a file descriptor forever. |
| B3 | Peer selection | "First peer that holds the chunk" means first **in the order the user listed peers**. | The previous code iterated a hash map, so the choice was effectively random and failures were not reproducible. |
| B4 | Nothing left to download | When resume finds every chunk valid on disk, no peer is contacted. | Piece maps only serve scheduling; a finished file should verify with every peer offline. |
| B6 | Storage lifecycle | `FileChannelStorage.close()` is terminal; reads and writes afterwards throw `IllegalStateException`. Writes require `preallocateSpace()` first. | A read after close silently reopened (and leaked) a channel; a write after a read-only open failed deep inside NIO. |
| B7 | Logging | One line per record, `[LEVEL] message`, on stderr. | JUL's default two-line, timestamped layout buries the message in CLI output. |

## Command line

| ID | Topic | Decision | Why |
|----|-------|----------|-----|
| B5 | Failure reporting | A failing command prints one line, `<command> failed: <reason>`, to stderr and exits `1`; usage errors exit `2`. | The v1.0 sketch caught every exception and exited `0`, so a scripted download would report success after failing. |
| B8 | Help | Every command accepts `-h`/`--help`, and `help <command>` works (§13 lists `HelpCommand`). No `--version`. | The README promised `--help`; nothing asks for a version flag. |
| B9 | Where `seed` writes the manifest | Without `--manifest-out`, still to `<file>.manifest.json`. | The v1.0 sketch wrote nothing unless `--manifest-out` was given — then no peer could ever get the manifest — and earlier releases always wrote it. |
| B10 | Earlier CLI forms | `seed <file> [<port>]` and `download <manifest> <output> <peer>...` keep working next to `seed --file …` and `leech --manifest … --peer … --output …`. Giving a value both ways, differently, is a usage error. | The spec adds options; it does not say to break existing commands. |
| B11 | `seed` output | Prints `Manifest hash:`, `Total chunks:` (as in §13), `Manifest file:` and `Listening on port N` to stdout. | The port line lets scripts and tests use `--port 0`. |

## Resilience

| ID | Topic | Decision | Why |
|----|-------|----------|-----|
| B13 | Corrupt-peer ban threshold — phase 6 says "ban after N checksum failures" | N = 3 chunks that fail SHA-256, counted per peer across the whole transfer. Bans last for that transfer only. Timeouts and refused connections never count. | 3 matches the per-chunk retry budget. Only a checksum failure shows a peer serves wrong data; a network failure only shows it is unreachable, which failover already handles. The project keeps no persistent peer state. |
| B14 | Reporting bans | The ban is logged once (`Banned peer <host:port> after 3 chunks failed checksum verification`), and an incomplete transfer names the banned peers. | Otherwise a user sees only "chunks missing" and cannot tell a lying peer from an offline one. |
| B12 | Peer promotion | A leecher does not serve chunks while it is downloading; a finished copy can be seeded. | The vision (§1) describes it, but no roadmap phase schedules it and the §13 `leech` command has no port to serve on. The README no longer claims it. |

---

## Roadmap items deliberately not implemented

| Roadmap item | Reason |
|--------------|--------|
| Phase 1 — "`ManifestBuilder` chunks a byte array in memory" | A learning step that §12 already replaces with a file-based builder, and the code with a streaming one. A byte-array entry point now would be an API only tests use. |
| Phase 4 — "`AtomicReference<BitSet>` for piece-map updates" | §20.2 itself says `synchronized` is right here: writes are rare (once per chunk) and nothing reads the map concurrently with downloads. Copy-on-write would only add allocation. The phase's guarantees — bounded in-flight transfers, CAS state transitions, no double scheduling — are verified by tests instead. |

---

## Packaging and process

| ID | Topic | Decision |
|----|-------|----------|
| P1 | Running the CLI | The `application` plugin builds `cli/build/install/swarm-share/bin/swarm-share`. The old README command (`java -cp cli/build/classes/java/main …`) could not work: it left out every dependency. |
| P2 | Native image | GraalVM CE 25 through the official Gradle plugin. Jackson metadata comes from the GraalVM reachability-metadata repository; the manifest DTOs ship their own `reachability-metadata.json`; picocli metadata is generated by `picocli-codegen`. |
| P3 | End-to-end tests | One suite (`cli/src/e2eTest`) launches the packaged executable as real processes. Gradle runs it against the JVM distribution (`e2eTest`, part of `check`) and the native binary (`nativeE2eTest`). |
| P3a | "Multi-machine test on LAN" (phase 5) | `LanSwarmTest` runs a seeder and two leechers in separate containers on a Docker network (Testcontainers), addressed by host name. It needs Docker and is skipped, not failed, without it; CI always has Docker. |
| P4 | Coverage | JaCoCo per module, plus `jacocoAggregatedReport` across modules, because domain code is mostly exercised by other modules' tests. |
| P5 | Versions | Each roadmap milestone is a minor release (`v0.2.0` phases 1–4, `v0.3.0` phase 5, `v0.4.0` phase 6). The release that completes the roadmap and its documentation is `v1.0.0`, matching the spec's "v1" scope. |
| P6 | Mockito | Removed. Its only use was mocking value records, which `AGENTS.md` discourages; real records are simpler. |
