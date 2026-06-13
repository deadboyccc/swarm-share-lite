// core/domain/ChunkDescriptor.java
package io.swarmshare.core.domain;

/**
 * Describes one chunk within a manifest.
 * Immutable — created once by the seeder, consumed by all peers.
 */
public record ChunkDescriptor(
    ChunkId id,
    long offset,   // byte offset in original file
    int size,      // byte count (may be < chunkSize for the last chunk)
    String sha256  // expected SHA-256 hex checksum of this chunk's raw bytes
) {
    public ChunkDescriptor {
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0");
        if (size <= 0)  throw new IllegalArgumentException("size must be > 0");
    }
}