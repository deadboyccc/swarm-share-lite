package io.swarmshare.transfer;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.ChunkState;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe tracker for per-chunk lifecycle state and failure counts.
 *
 * <p>
 * Uses {@link ConcurrentHashMap#replace} for atomic compare-and-swap
 * transitions,
 * preventing two virtual threads from scheduling the same chunk concurrently.
 */
public final class ChunkStateTracker {

    /**
     * Current lifecycle state of each chunk, defaulting to MISSING until initialized.
     */
    private final ConcurrentHashMap<ChunkId, ChunkState> states = new ConcurrentHashMap<>();
    /**
     * Per-chunk count of failed download/verification attempts.
     */
    private final ConcurrentHashMap<ChunkId, AtomicInteger> failureCounts = new ConcurrentHashMap<>();

    /**
     * Registers {@code id} for tracking if it isn't already, starting at
     * {@link ChunkState#MISSING} with a zero failure count. Safe to call
     * more than once for the same id — subsequent calls are no-ops.
     */
    public void initialize(ChunkId id) {
        states.putIfAbsent(id, ChunkState.MISSING);
        failureCounts.putIfAbsent(id, new AtomicInteger(0));
    }

    /**
     * Atomically transitions {@code id} from {@code expected} to {@code next}.
     *
     * @return {@code true} if the transition succeeded
     */
    public boolean transition(ChunkId id, ChunkState expected, ChunkState next) {
        // ConcurrentHashMap.replace acts as a CAS here: only replace when value equals
        // expected
        return states.replace(id, expected, next);
    }

    /**
     * Returns the current state of {@code id}, or {@link ChunkState#MISSING}
     * if it has never been {@link #initialize(ChunkId) initialized}.
     */
    public ChunkState getState(ChunkId id) {
        return states.getOrDefault(id, ChunkState.MISSING);
    }

    /**
     * Records a failed attempt for {@code id} and returns the updated count.
     * Initializes the counter to zero first if this is the first failure seen
     * for this id (e.g. if called before {@link #initialize(ChunkId)}).
     */
    public int incrementFailure(ChunkId id) {
        return failureCounts.computeIfAbsent(id, _ -> new AtomicInteger(0)).incrementAndGet();
    }

    /**
     * Returns the number of recorded failures for {@code id}, or 0 if none have
     * been recorded.
     */
    public int getFailureCount(ChunkId id) {
        return failureCounts.getOrDefault(id, new AtomicInteger(0)).get();
    }
}