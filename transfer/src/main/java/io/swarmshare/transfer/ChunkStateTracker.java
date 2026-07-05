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

    private final ConcurrentHashMap<ChunkId, ChunkState> states = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkId, AtomicInteger> failureCounts = new ConcurrentHashMap<>();

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

    public ChunkState getState(ChunkId id) {
        return states.getOrDefault(id, ChunkState.MISSING);
    }

    public int incrementFailure(ChunkId id) {
        return failureCounts.computeIfAbsent(id, _ -> new AtomicInteger(0)).incrementAndGet();
    }

    public int getFailureCount(ChunkId id) {
        return failureCounts.getOrDefault(id, new AtomicInteger(0)).get();
    }
}
