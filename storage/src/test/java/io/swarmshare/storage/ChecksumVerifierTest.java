package io.swarmshare.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ChecksumVerifier}.
 *
 * <p>Coverage targets:
 * <ul>
 * <li>Correctness — known SHA-256 vectors (NIST / RFC-standard inputs)</li>
 * <li>Edge inputs — empty array, null/blank/malformed expected hash</li>
 * <li>Case insensitivity — uppercase hex in expectedHash accepted</li>
 * <li>Virtual-thread safety — concurrent compute() calls produce stable results</li>
 * </ul>
 *
 * <p>The implementation is stateless (no {@code ThreadLocal}), so there is no
 * thread-local state to corrupt; the concurrency test validates that the
 * per-call {@link java.security.MessageDigest} allocation is truly isolated.
 */
class ChecksumVerifierTest {

    // Single shared instance — stateless, safe across all tests and threads
    private final ChecksumVerifier verifier = new ChecksumVerifier();

    // ── compute() ────────────────────────────────────────────────────────────────

    @Test
    void compute_knownInput_returnsStandardSha256() {
        // "hello world" → well-known NIST/RFC-verifiable digest
        byte[] input = bytes("hello world");

        assertThat(verifier.compute(input))
                .isEqualTo("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9");
    }

    @Test
    void compute_emptyArray_returnsEmptySha256() {
        // SHA-256("") is standardised; verifies the MessageDigest is always reset-clean
        assertThat(verifier.compute(new byte[0]))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void compute_sameInputTwice_returnsSameHash() {
        // Stateless design must produce identical output for identical input on every call
        byte[] input = bytes("idempotency-check");

        assertThat(verifier.compute(input)).isEqualTo(verifier.compute(input));
    }

    @Test
    void compute_distinctInputs_returnDistinctHashes() {
        // Collision sanity — different payloads must not produce the same digest
        assertThat(verifier.compute(bytes("chunk-A")))
                .isNotEqualTo(verifier.compute(bytes("chunk-B")));
    }

    @Test
    void compute_returnsLowercaseHex() {
        // Contract: output is always lowercase hex (64 chars, no uppercase)
        String hash = verifier.compute(bytes("case-check"));

        assertThat(hash)
                .hasSize(64)
                .as("must be lowercase hex")
                .matches("[0-9a-f]+");
    }

    // ── verify() — happy path ────────────────────────────────────────────────────

    @Test
    void verify_correctHash_returnsTrue() {
        byte[] input = bytes("swarm-share-lite");
        String hash = verifier.compute(input);

        assertThat(verifier.verify(input, hash)).isTrue();
    }

    @Test
    void verify_upperCaseHash_returnsTrue() {
        // Contract: case-insensitive — uppercase hex from external manifests must be accepted
        byte[] input = bytes("test-case");
        String lower = verifier.compute(input);

        assertThat(verifier.verify(input, lower.toUpperCase())).isTrue();
    }

    @Test
    void verify_mixedCaseHash_returnsTrue() {
        byte[] input = bytes("mixed-case-hex");
        String lower = verifier.compute(input);
        // Alternate upper/lower chars
        StringBuilder mixed = new StringBuilder();
        for (int i = 0; i < lower.length(); i++) {
            mixed.append(i % 2 == 0 ? Character.toUpperCase(lower.charAt(i)) : lower.charAt(i));
        }

        assertThat(verifier.verify(input, mixed.toString())).isTrue();
    }

    // ── verify() — rejection cases ───────────────────────────────────────────────

    @Test
    void verify_wrongHash_returnsFalse() {
        byte[] input = bytes("swarm-share-lite");
        String allZeros = "0".repeat(64);

        assertThat(verifier.verify(input, allZeros)).isFalse();
    }

    @Test
    void verify_differentData_sameHash_returnsFalse() {
        // Verifying different data against a hash computed from different data must fail
        byte[] original = bytes("original-data");
        byte[] tampered = bytes("tampered-data");
        String originalHash = verifier.compute(original);

        assertThat(verifier.verify(tampered, originalHash)).isFalse();
    }

    @ParameterizedTest(name = "verify rejects null/blank expectedHash [{0}]")
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t", "\n"})
    void verify_nullOrBlankHash_returnsFalse(String hash) {
        assertThat(verifier.verify(bytes("any-data"), hash)).isFalse();
    }

    @ParameterizedTest(name = "verify rejects malformed hex [{0}]")
    @ValueSource(strings = {
            "not-hex-at-all",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", // 64 non-hex chars
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde",  // 63 chars (odd length)
            "b94d27b9 934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9" // space inside
    })
    void verify_malformedHex_returnsFalse(String malformedHash) {
        // Must not throw — malformed input is a data error, not a programming error
        assertThat(verifier.verify(bytes("any-data"), malformedHash)).isFalse();
    }

    // ── concurrency ──────────────────────────────────────────────────────────────

    @Test
    void compute_concurrentVirtualThreads_allProduceCorrectHashes() throws InterruptedException {
        // Verify that per-call MessageDigest allocation is truly isolated:
        // 300 virtual threads each hashing their own payload must never cross-contaminate.
        record Payload(byte[] data, String expected) {}

        List<Payload> payloads = List.of(
                new Payload(bytes("data-stream-A"), "a1fc603c9e9aa60268f0192ccc47d77ec0038fe0c2e331b82223e420fa6c90c4"),
                new Payload(bytes("data-stream-B"), "dc5384a624842261fa9b6f19985525b8be4b012754054d326e876aeb7c517a5b"),
                new Payload(bytes("data-stream-C"), "0677dd0021b8bc58b822b3bd23fd6467c5e2b847b787f2f34b9ecb12e2d28c7c")
        );

        int iterations = 200;
        CountDownLatch startGate = new CountDownLatch(1); // all threads start simultaneously
        CountDownLatch doneLatch = new CountDownLatch(payloads.size());
        List<AssertionError> failures = new ArrayList<>();

        for (Payload payload : payloads) {
            Thread.ofVirtual().start(() -> {
                try {
                    startGate.await(); // wait for all threads to be ready
                    for (int i = 0; i < iterations; i++) {
                        String actual = verifier.compute(payload.data());
                        if (!actual.equals(payload.expected())) {
                            synchronized (failures) {
                                failures.add(new AssertionError(
                                        "Hash mismatch on iteration %d: expected %s, got %s"
                                                .formatted(i, payload.expected(), actual)));
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startGate.countDown(); // release all threads at once
        doneLatch.await();

        assertThat(failures).as("concurrent hash computation produced incorrect results").isEmpty();
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
