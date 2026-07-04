# AGENTS.md

# AI Agent Guidelines

This document defines the engineering standards AI coding agents
(ChatGPT, Claude, GitHub Copilot, Cursor, Cline, Windsurf, etc.) must
follow when contributing to **swarm-share-lite**.

This project is intentionally designed as a production-quality reference
implementation of:

-   Modern Java 25
-   Test-Driven Development (TDD)
-   Hexagonal (Ports & Adapters) Architecture
-   Distributed Systems
-   Production-grade engineering practices

The goal is not simply to make the code work---it is to build software
that is educational, maintainable, extensible, and idiomatic.

------------------------------------------------------------------------

# Primary Objective

Every contribution should improve one or more of the following:

-   Correctness
-   Readability
-   Testability
-   Maintainability
-   Architectural consistency
-   Documentation quality

Never optimize for producing more code.

Optimize for producing better software.

------------------------------------------------------------------------

# Core Principles

Every change must follow these principles:

1.  Test First
2.  Modern Java 25
3.  Clean Architecture
4.  Simplicity
5.  Readability
6.  Small Focused Changes
7.  Extensive Documentation
8.  Production-quality Engineering

If a request conflicts with these principles, prefer the principles.

------------------------------------------------------------------------

# Required Development Workflow

1.  Understand the problem.
2.  Design the API.
3.  Write failing tests.
4.  Implement the minimum solution.
5.  Refactor.
6.  Verify all tests pass.
7.  Improve documentation if needed.

**Always:** RED → GREEN → REFACTOR

Never generate production code before tests unless explicitly
instructed.

------------------------------------------------------------------------

# Architecture Rules

Dependencies always flow inward.

``` text
CLI
 ↓
Transfer
 ↓
Networking / Storage
 ↓
Operating System
```

Infrastructure depends on the domain.

Never the reverse.

The domain must remain independent of networking, storage,
serialization, CLI frameworks, databases, and external services.

------------------------------------------------------------------------

# Java 25 Guidelines

Prefer:

-   Records
-   Sealed interfaces/classes
-   Pattern matching
-   Switch expressions
-   Virtual Threads
-   Immutable collections
-   Structured concurrency when appropriate
-   Optional where it improves clarity

Avoid:

-   Legacy Java patterns
-   Mutable shared state
-   Excessive inheritance
-   Boilerplate
-   Over-engineering

------------------------------------------------------------------------

# Testing Standards

Every production change must include tests.

Prefer:

-   JUnit 5
-   AssertJ
-   Fake implementations over mocks
-   Integration tests where appropriate

Follow TDD.

------------------------------------------------------------------------

# Documentation

Document public APIs, concurrency assumptions, protocol guarantees,
complex algorithms, and architectural decisions.

Explain **why**, not **what**.

------------------------------------------------------------------------

# Code Style

Prefer:

-   Small methods
-   Small classes
-   Constructor injection
-   Immutable objects
-   Clear domain terminology
-   Composition over inheritance

Avoid:

-   God classes
-   Utility dumping grounds
-   Deep inheritance
-   Clever code

------------------------------------------------------------------------

# Concurrency

Prefer Virtual Threads, ConcurrentHashMap, Atomic types, Semaphore, and
structured concurrency.

Avoid unnecessary synchronization and shared mutable state.

------------------------------------------------------------------------

# Error Handling

Fail fast.

Never silently swallow exceptions.

Preserve root causes.

------------------------------------------------------------------------

# Performance

Correctness \> Simplicity \> Readability \> Optimization.

Only optimize with a documented reason.

------------------------------------------------------------------------

# Pull Requests

Before considering work complete:

-   All tests pass
-   New functionality is tested
-   Public APIs are documented
-   Java 25 conventions are followed
-   Architectural boundaries are respected
-   No dead code
-   No unrelated changes

------------------------------------------------------------------------

# AI Agent Expectations

Before writing code:

-   Read surrounding code.
-   Preserve existing architecture.
-   Reuse abstractions.
-   Avoid duplication.
-   Never introduce frameworks without approval.
-   Never bypass architectural boundaries.

------------------------------------------------------------------------

# Definition of Done

-   ✅ Tests exist
-   ✅ Tests pass
-   ✅ Architecture respected
-   ✅ Public APIs documented
-   ✅ Naming is clear
-   ✅ Complexity minimized
-   ✅ Production quality

------------------------------------------------------------------------

# Project Philosophy

Every class should teach.

Every API should communicate intent.

Every test should explain behavior.

Every abstraction should have a purpose.

When in doubt:

> Choose readability over cleverness, tests over assumptions,
> architecture over shortcuts, and simplicity over complexity.
