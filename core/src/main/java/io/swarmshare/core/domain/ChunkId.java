// core/domain/ChunkId.java
package io.swarmshare.core.domain;

/**
 * Globally unique identity for a chunk.
 * The manifestHash ties it to a specific file transfer.
 * Two chunks with the same index but different manifests are completely different objects.
 */
public record ChunkId(String manifestHash, int index) {
    // Compact constructor for validation
    public ChunkId {
        if (manifestHash == null || manifestHash.isBlank())
            throw new IllegalArgumentException("manifestHash must not be blank");
        if (index < 0)
            throw new IllegalArgumentException("index must be >= 0");
    }
}