// core/port/StorageProvider.java
package io.swarmshare.core.port;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;

import java.util.BitSet;
import java.util.Optional;

/**
 * Abstracts all persistent storage operations.
 *
 * Implementations:
 *   - FileChannelStorage  (production: direct file I/O)
 *   - InMemoryStorage     (testing: byte arrays, no disk)
 *
 * The domain never calls FileChannel, Path, or Files directly.
 */
public interface StorageProvider {

    /**
     * Pre-allocate the output file to totalSize bytes on disk.
     * This avoids fragmentation and allows out-of-order writes.
     * Called once before any chunks are written.
     */
    void preallocateSpace(long totalSize);

    /**
     * Write raw chunk bytes to the correct byte offset in the output file.
     * Must be safe to call concurrently from multiple virtual threads
     * as long as different chunks are being written simultaneously.
     */
    void writeChunk(ChunkId id, long offset, byte[] data);

    /**
     * Read raw chunk bytes from disk for serving to a remote peer.
     * Returns empty if the chunk has not been written yet.
     */
    Optional<byte[]> readChunk(ChunkId id, long offset, int size);

    /**
     * Scan the output file on startup to determine which chunks
     * are already complete (for resume support).
     * Returns a BitSet where bit[i] = true means chunk i is verified on disk.
     */
    BitSet checkExistingChunks(Manifest manifest);
}