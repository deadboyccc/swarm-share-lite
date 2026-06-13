// core/domain/TransferResult.java
package io.swarmshare.core.domain;

/**
 * Sealed result type for a single chunk download attempt.
 * The compiler forces exhaustive handling via pattern matching.
 * shouldRetry=true signals the orchestrator to re-queue the chunk.
 */
public sealed interface TransferResult
    permits TransferResult.Success, TransferResult.Failure {

    record Success(ChunkId id, byte[] data) implements TransferResult {}

    record Failure(ChunkId id, String reason, boolean shouldRetry)
        implements TransferResult {}
}