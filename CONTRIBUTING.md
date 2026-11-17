# Contributing to swarm-share-lite

Thank you for helping. This project is a distributed file-sharing tool **and** a
reference implementation of modern Java 25, test-driven development and clean
architecture — code is read as often as it is run, so how a change is made matters
as much as what it does.

## Contents

- [Getting started](#getting-started)
- [Workflow](#workflow)
- [Test-driven development](#test-driven-development)
- [Architecture rules](#architecture-rules)
- [Modern Java standards](#modern-java-standards)
- [Documentation and comments](#documentation-and-comments)
- [Code style](#code-style)
- [Pull request checklist](#pull-request-checklist)

## Getting started

| You need | For |
|----------|-----|
| Java 25 | Building and running (the Gradle wrapper downloads Gradle) |
| Docker | The multi-host `LanSwarmTest` (skipped without Docker; CI always has it) |
| GraalVM 25 with `native-image` | Only to build or test the native executable |

```bash
./gradlew build                     # compile, unit + integration + e2e tests, coverage
./gradlew :cli:installDist          # run it: cli/build/install/swarm-share/bin/swarm-share
./gradlew jacocoAggregatedReport    # coverage → build/reports/jacoco/jacocoAggregatedReport/
./gradlew :cli:nativeE2eTest        # native build + e2e suite against it (needs GraalVM)
```

Read [ARCHITECTURE.md](ARCHITECTURE.md) first — it is the specification. Where it
is silent, check [docs/ASSUMPTIONS.md](docs/ASSUMPTIONS.md) before inventing behaviour.

## Workflow

1. **Understand the problem** and find the section of `ARCHITECTURE.md` it touches.
2. **Write a failing test** that shows the behaviour or reproduces the bug.
3. **Implement the minimum** to make it pass.
4. **Refactor** with the tests green.
5. **Run `./gradlew build`** — every test must pass.
6. **Update the docs** if behaviour, the CLI or the protocol changed.

> **RED → GREEN → REFACTOR.** Pull requests without tests for new behaviour will be
> asked for them.

## Test-driven development

Choose the lowest layer that can show the behaviour:

| Layer | Location | Use it for |
|-------|----------|-----------|
| Unit | `<module>/src/test` | Domain rules, orchestration logic, protocol encoding — with fakes |
| Integration | `networking/src/test` | Real sockets and files working together |
| End-to-end | `cli/src/e2eTest` | User-visible flows of the packaged CLI, as separate processes |

Guidelines:

- **Fakes over mocks.** Use `InMemoryStorage`, `FakePeerConnector` or a scripted
  socket peer. Build real records instead of mocking them. The project has no
  mocking library.
- **Every test asserts something.** "Does not throw" is spelled
  `assertThatCode(...).doesNotThrowAnyException()`.
- **No sleeps for synchronisation.** Wait for a condition (a bound port, a log line).
- **No borrowed ports.** Bind to port `0` and read the real port. A dead peer is
  `127.0.0.1:1`, never a port you just released — a parallel test may take it.
- **Names describe behaviour:** `start_corruptPeerIsBannedAfterThreeChecksumFailures_…`.

Examples of what needs a test first: a new protocol message, a new storage
behaviour, a new CLI option, and every bug fix (reproduce it, then fix it).

## Architecture rules

Dependencies flow inward only:

```
cli ──► transfer ──► manifest ──► core
 └──► networking, storage ──────► core
```

- `core` must not depend on TCP, `FileChannel`, JSON libraries, CLI frameworks or
  any other infrastructure.
- Infrastructure implements the ports in `core`; it is never called directly by the domain.
- New frameworks or libraries need agreement first.

## Modern Java standards

Prefer: records · sealed interfaces · pattern matching and record patterns ·
switch expressions · virtual threads · immutable collections · `Optional` where it
clarifies.

Avoid: legacy patterns with modern replacements · mutable shared state ·
unnecessary inheritance · heavy frameworks · premature optimisation.

Concurrency: never hold a lock across blocking I/O (see `ARCHITECTURE.md` §20.5).
Prefer `ConcurrentHashMap`, atomics, `Semaphore` and `ReentrantLock`.

## Documentation and comments

Document public classes and methods, ports, protocol behaviour, concurrency
assumptions and non-obvious decisions:

```java
/**
 * Reads {@code size} bytes from absolute byte {@code offset}.
 *
 * @return the chunk bytes, or {@link Optional#empty()} if the file is shorter than expected
 * @throws IllegalStateException if this storage has been closed
 */
Optional<byte[]> readChunk(ChunkId id, long offset, int size);
```

Comments explain **why**, not **what**:

```java
// Bad
counter++; // increment counter

// Good
// CAS: only one virtual thread may move a chunk from MISSING to SCHEDULED.
states.replace(id, ChunkState.MISSING, ChunkState.SCHEDULED);
```

## Code style

- Small methods and classes with clear domain names.
- Constructor injection; immutable objects.
- Composition over inheritance; no god classes or utility dumping grounds.
- Readability over cleverness.

## Pull request checklist

- [ ] `./gradlew build` passes locally.
- [ ] New behaviour has tests; bug fixes have a test that failed before the fix.
- [ ] Public APIs are documented.
- [ ] Architectural boundaries are respected.
- [ ] `ARCHITECTURE.md`, `README.md` or `docs/ASSUMPTIONS.md` updated if behaviour changed.
- [ ] No unrelated changes; the history is reasonably clean.

Small, focused pull requests are reviewed fastest.

> When in doubt, choose readability, tests and simplicity.
