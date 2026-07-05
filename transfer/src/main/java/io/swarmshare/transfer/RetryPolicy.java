package io.swarmshare.transfer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Exponential backoff for retryable operations.
 *
 * <p>
 * Delay doubles each attempt (100 ms → 200 ms → 400 ms …) capped at
 * {@code maxDelay}.
 */
public final class RetryPolicy {

    private final int maxAttempts;
    private final Duration initialDelay;
    private final Duration maxDelay;

    public RetryPolicy(int maxAttempts, Duration initialDelay, Duration maxDelay) {
        if (maxAttempts < 1)
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        this.maxAttempts = maxAttempts;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
    }

    /**
     * Computes wait duration for attempt {@code attempt} (0-indexed).
     */
    public Duration delayFor(int attempt) {
        // Exponential backoff: initialDelay * 2^attempt, capped at maxDelay
        long ms = initialDelay.toMillis() * (1L << attempt);
        return Duration.ofMillis(Math.min(ms, maxDelay.toMillis()));
    }

    public boolean shouldRetry(int attemptsDone) {
        return attemptsDone < maxAttempts;
    }

    /**
     * Blocks the calling virtual thread for the computed delay.
     */
    public void sleep(int attempt) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(delayFor(attempt).toMillis());
    }
}
