// core/domain/Manifest.java
package io.swarmshare.core.domain;

import java.util.List;

/**
 * The contract of a swarm. Immutable once created by the seeder.
 * Distributed to all peers before any chunk transfer begins.
 * fileHash is SHA-256 of the entire assembled file — final integrity check.
 */
public record Manifest(
    String fileHash,
    String fileName,
    long totalSize,
    int chunkSize,
    List<ChunkDescriptor> chunks
) {
    public Manifest {
        chunks = List.copyOf(chunks); // defensive copy, ensures immutability
    }

    public int totalChunks() { return chunks.size(); }

    /** Convenience: look up a descriptor by chunk index. O(1) by list index. */
    public ChunkDescriptor chunkAt(int index) { return chunks.get(index); }
}