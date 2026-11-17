package io.swarmshare.transfer;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy(5, Duration.ofMillis(100), Duration.ofMillis(800));

    @Test
    void delayFor_doublesEachAttempt_cappedAtMax() {
        assertThat(policy.delayFor(0)).isEqualTo(Duration.ofMillis(100));
        assertThat(policy.delayFor(1)).isEqualTo(Duration.ofMillis(200));
        assertThat(policy.delayFor(2)).isEqualTo(Duration.ofMillis(400));
        assertThat(policy.delayFor(3)).isEqualTo(Duration.ofMillis(800));
        assertThat(policy.delayFor(10)).isEqualTo(Duration.ofMillis(800));
    }

    @Test
    void delayFor_largeAttempt_doesNotOverflow() {
        assertThat(policy.delayFor(62)).isEqualTo(Duration.ofMillis(800));
        assertThat(policy.delayFor(63)).isEqualTo(Duration.ofMillis(800));
        assertThat(policy.delayFor(Integer.MAX_VALUE)).isEqualTo(Duration.ofMillis(800));
    }

    @Test
    void delayFor_multiplicationOverflow_isCapped() {
        RetryPolicy huge = new RetryPolicy(3, Duration.ofMillis(Long.MAX_VALUE / 2), Duration.ofDays(1));

        assertThat(huge.delayFor(4)).isEqualTo(Duration.ofDays(1));
    }

    @Test
    void delayFor_negativeAttempt_isRejected() {
        assertThatThrownBy(() -> policy.delayFor(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRetry_respectsMaxAttempts() {
        assertThat(policy.shouldRetry(0)).isTrue();
        assertThat(policy.shouldRetry(4)).isTrue();
        assertThat(policy.shouldRetry(5)).isFalse();
    }

    @Test
    void sleep_blocksForTheComputedDelay() throws InterruptedException {
        RetryPolicy shortPolicy = new RetryPolicy(2, Duration.ofMillis(30), Duration.ofMillis(30));
        long started = System.nanoTime();

        shortPolicy.sleep(0);

        assertThat(Duration.ofNanos(System.nanoTime() - started)).isGreaterThanOrEqualTo(Duration.ofMillis(30));
    }

    @Test
    void constructor_rejectsInvalidArguments() {
        assertThatThrownBy(() -> new RetryPolicy(0, Duration.ZERO, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, null, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, Duration.ofMillis(-1), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, Duration.ZERO, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
