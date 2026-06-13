// core/domain/ChunkState.java
package io.swarmshare.core.domain;

/**
 * The lifecycle state of a single chunk during transfer.
 * States transition monotonically (no going backwards except MISSING on failure).
 */
public enum ChunkState {
    MISSING,     // Not yet downloaded; needs to be fetched
    SCHEDULED,   // Assigned to a virtual thread for download
    IN_FLIGHT,   // Active network I/O in progress
    VERIFYING,   // SHA-256 being computed
    VERIFIED,    // Hash matched; safe to write
    WRITTEN      // Persisted to disk; bit set in BitSet
}