# AGENTS.md — Guidelines for AI Coding Agents

These rules apply to every AI agent (Claude, ChatGPT, Copilot, Cursor, Cline,
Windsurf, …) contributing to **swarm-share-lite**. Human contributors follow
[CONTRIBUTING.md](CONTRIBUTING.md); this file adds what an agent needs to work
safely and in step with the project.

The goal is not code that merely works. It is software that is correct, readable,
tested and true to its architecture — a reference implementation of modern
Java 25, TDD, hexagonal architecture and distributed-systems practice.

## 1. Sources of truth

| Question | Answer lives in |
|----------|-----------------|
| What should the system do? | [ARCHITECTURE.md](ARCHITECTURE.md) — the specification |
| What was decided where the spec is silent? | [docs/ASSUMPTIONS.md](docs/ASSUMPTIONS.md) |
| How do I build, test and contribute? | [CONTRIBUTING.md](CONTRIBUTING.md) |

- If code contradicts the spec, fix the code, not the spec.
- If the spec is ambiguous or silent, do not invent behaviour: take the most
  conservative reading and record it in `docs/ASSUMPTIONS.md`.
- Implement only what was asked. No unrequested features, refactors or dependencies.

## 2. Required workflow

1. Read the relevant spec section and the surrounding code.
2. Design the API.
3. Write a failing test.
4. Implement the minimum to pass it.
5. Refactor with the tests green.
6. Run `./gradlew build` and confirm every test passes.
7. Update the documentation if behaviour changed.

**RED → GREEN → REFACTOR.** Do not write production code before its test unless
explicitly told to.

## 3. Verify by running, not by assuming

| To check | Run |
|----------|-----|
| Everything on the JVM | `./gradlew build` |
| Coverage | `./gradlew jacocoAggregatedReport` |
| The native executable | `./gradlew :cli:nativeE2eTest` (needs GraalVM 25, ~1.5 GB free memory) |
| A user flow by hand | `./gradlew :cli:installDist`, then `cli/build/install/swarm-share/bin/swarm-share` |

- Never report a task as done until the relevant command has passed.
- Never skip, disable or weaken a test to make it pass. Fix the cause.
- A flaky test is a bug: make it deterministic (see §7) rather than retrying it.

## 4. Architecture rules

```
cli ──► transfer ──► manifest ──► core
 └──► networking, storage ──────► core
```

- Dependencies flow inward. Infrastructure depends on the domain, never the reverse.
- `core` stays free of networking, storage, serialization, CLI frameworks and
  external services.
- New behaviour in the domain goes behind a port; new infrastructure implements one.
- Never introduce a framework or library without approval.

## 5. Java 25 guidelines

| Prefer | Avoid |
|--------|-------|
| Records, sealed interfaces, pattern matching, switch expressions | Legacy patterns with a modern replacement |
| Virtual threads, immutable collections | Mutable shared state |
| `ConcurrentHashMap`, atomics, `Semaphore`, `ReentrantLock` | Locks held across blocking I/O |
| `Optional` where it improves clarity | Boilerplate, deep inheritance, over-engineering |

## 6. Testing standards

- JUnit Jupiter 6 and AssertJ.
- **Fakes over mocks** — `InMemoryStorage`, `FakePeerConnector`, scripted socket
  peers, real records. Do not add a mocking library.
- Unit tests for logic, integration tests for real sockets and files, end-to-end
  tests (`cli/src/e2eTest`) for user flows; the e2e suite must keep passing
  against both the JVM distribution and the native binary.
- Every test asserts something meaningful.

## 7. Determinism rules for tests

- Bind servers to port `0` and read the real port; never reuse a released port.
- Model a dead peer as `127.0.0.1:1`.
- Wait for a condition (bound port, log line, process exit) — never `sleep` to synchronise.
- Bound every potentially blocking test with `@Timeout`.

## 8. Native image

The CLI ships as a GraalVM native executable. When adding a class that is reached
reflectively (picocli commands, Jackson DTOs):

- picocli commands get metadata from `picocli-codegen` automatically;
- Jackson DTOs need an entry in the owning module's
  `src/main/resources/META-INF/native-image/<group>/<module>/reachability-metadata.json`;
- prove it with `./gradlew :cli:nativeE2eTest`.

## 9. Documentation

Document public APIs, ports, protocol guarantees, concurrency assumptions and
architectural decisions. Explain **why**, not **what**. Keep `README.md`,
`ARCHITECTURE.md` and `docs/ASSUMPTIONS.md` true to the code you leave behind.

## 10. Error handling

- Fail fast; never swallow an exception silently.
- Preserve root causes when wrapping.
- User-facing failures are one clear line (`<command> failed: <reason>`), not a stack trace.

## 11. Definition of done

- [ ] Tests exist and pass (`./gradlew build`)
- [ ] Native e2e still passes if CLI, reflection or serialization changed
- [ ] Architecture respected
- [ ] Public APIs documented
- [ ] Docs updated where behaviour changed
- [ ] No dead code, no unrelated changes

> Choose readability over cleverness, tests over assumptions, architecture over
> shortcuts, and simplicity over complexity.
