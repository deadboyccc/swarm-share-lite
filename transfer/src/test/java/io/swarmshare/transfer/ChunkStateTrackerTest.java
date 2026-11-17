package io.swarmshare.transfer;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.ChunkState;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ChunkStateTrackerTest {

    private final ChunkStateTracker tracker = new ChunkStateTracker();
    private final ChunkId id = new ChunkId("abc123", 0);

    @Test
    void unknownChunk_defaultsToMissingWithNoFailures() {
        assertThat(tracker.getState(id)).isEqualTo(ChunkState.MISSING);
        assertThat(tracker.getFailureCount(id)).isZero();
    }

    @Test
    void initialize_setsMissingState() {
        tracker.initialize(id);

        assertThat(tracker.getState(id)).isEqualTo(ChunkState.MISSING);
        assertThat(tracker.getFailureCount(id)).isZero();
    }

    @Test
    void initialize_isIdempotent_andDoesNotResetProgress() {
        tracker.initialize(id);
        tracker.transition(id, ChunkState.MISSING, ChunkState.SCHEDULED);
        tracker.incrementFailure(id);

        tracker.initialize(id);

        assertThat(tracker.getState(id)).isEqualTo(ChunkState.SCHEDULED);
        assertThat(tracker.getFailureCount(id)).isEqualTo(1);
    }

    @Test
    void transition_succeedsWhenExpectedStateMatches() {
        tracker.initialize(id);

        assertThat(tracker.transition(id, ChunkState.MISSING, ChunkState.SCHEDULED)).isTrue();
        assertThat(tracker.getState(id)).isEqualTo(ChunkState.SCHEDULED);
    }

    @Test
    void transition_failsWhenExpectedStateDoesNotMatch() {
        tracker.initialize(id);

        assertThat(tracker.transition(id, ChunkState.SCHEDULED, ChunkState.IN_FLIGHT)).isFalse();
        assertThat(tracker.getState(id)).isEqualTo(ChunkState.MISSING);
    }

    @Test
    void transition_onUninitializedChunk_fails() {
        assertThat(tracker.transition(id, ChunkState.MISSING, ChunkState.SCHEDULED)).isFalse();
    }

    @Test
    void transition_concurrentSchedulers_exactlyOneWins() throws Exception {
        tracker.initialize(id);
        int contenders = 64;
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < contenders; i++) {
                executor.submit(() -> {
                    start.await();
                    if (tracker.transition(id, ChunkState.MISSING, ChunkState.SCHEDULED)) {
                        winners.incrementAndGet();
                    }
                    return null;
                });
            }
            start.countDown();
        }

        assertThat(winners).hasValue(1);
    }

    @Test
    void incrementFailure_countsAcrossVirtualThreadsWithoutLostUpdates() {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 1_000; i++) {
                executor.submit(() -> tracker.incrementFailure(id));
            }
        }

        assertThat(tracker.getFailureCount(id)).isEqualTo(1_000);
    }

    @Test
    void reset_overwritesCurrentState() {
        tracker.initialize(id);
        tracker.transition(id, ChunkState.MISSING, ChunkState.SCHEDULED);

        tracker.reset(id, ChunkState.MISSING);

        assertThat(tracker.getState(id)).isEqualTo(ChunkState.MISSING);
    }
}
