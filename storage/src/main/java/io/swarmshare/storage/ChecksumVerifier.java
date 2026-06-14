// storage/ChecksumVerifier.java
package io.swarmshare.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Stateless chunk integrity verifier using SHA-256.
 *
 * <h3>Why not ThreadLocal?</h3>
 * {@link MessageDigest} is {@code synchronized} internally on some JDK implementations,
 * which pins virtual threads to their carrier thread for the duration of the lock.
 * Under Project Loom, the preferred pattern is to allocate a fresh {@code MessageDigest}
 * per call: the allocation cost is negligible compared to the hash computation itself,
 * and it eliminates pinning, state-reset bugs, and thread-lifecycle coupling entirely.
 *
 * <h3>Timing safety</h3>
 * Hash comparison uses {@link MessageDigest#isEqual} — a constant-time byte comparison
 * that prevents timing side-channels that {@code String.equals} or {@code equalsIgnoreCase}
 * would expose.
 */
public final class ChecksumVerifier {

    private static final String ALGORITHM = "SHA-256";
    private static final HexFormat HEX = HexFormat.of();

    /**
     * Computes the SHA-256 digest of {@code data} and returns it as a lowercase hex string.
     *
     * @param data raw bytes to hash; must not be {@code null}
     * @return lowercase hex-encoded SHA-256 digest (64 characters)
     */
    public String compute(byte[] data) {
        return HEX.formatHex(digest(data));
    }

    /**
     * Returns {@code true} if the SHA-256 digest of {@code data} matches {@code expectedHex}.
     * Comparison is case-insensitive and constant-time to prevent timing side-channels.
     *
     * @param data        raw bytes to verify
     * @param expectedHex expected SHA-256 hex string; {@code null} or blank always returns false
     * @return {@code true} iff the computed digest matches the expected hash
     */
    public boolean verify(byte[] data, String expectedHex) {
        if (expectedHex == null || expectedHex.isBlank()) return false;

        byte[] expected;
        try {
            expected = HEX.parseHex(expectedHex.toLowerCase());
        } catch (IllegalArgumentException e) {
            // expectedHex is not valid hex — treat as mismatch, not an exception
            return false;
        }

        // Constant-time comparison; prevents timing oracle on partial hash matches
        return MessageDigest.isEqual(digest(data), expected);
    }

    // ── internal ────────────────────────────────────────────────────────────────

    /** Allocates a fresh {@code MessageDigest} per call; safe for virtual threads. */
    private static byte[] digest(byte[] data) {
        try {
            return MessageDigest.getInstance(ALGORITHM).digest(data);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the Java SE spec — this is truly unreachable
            throw new AssertionError("SHA-256 not available on this JVM", e);
        }
    }
}
