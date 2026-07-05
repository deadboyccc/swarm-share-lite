package io.swarmshare.core.crypto;

/**
 * Core port for checksum computation and verification.
 *
 * <p>
 * Implementations must be stateless and thread-safe. Each call to
 * {@link #compute(byte[])} or {@link #verify(byte[], String)} should be
 * independent, with no reliance on prior state.
 *
 * <p>
 * Implementations:
 * - {@link Sha256}: SHA-256 in hex-encoded format
 *
 * <p>
 * Callers rely on this port to ensure data integrity during transfer.
 * Checksum mismatches trigger retry logic; correctness is critical.
 */
public interface HasherPort {

    /**
     * Computes a checksum of the given data.
     *
     * @param data raw bytes to hash; must not be {@code null}
     * @return checksum as a string (typically hex-encoded)
     */
    String compute(byte[] data);

    /**
     * Verifies that the checksum of the given data matches the expected value.
     * Comparison should be constant-time to prevent timing side-channels.
     *
     * @param data        raw bytes to verify
     * @param expectedHex expected checksum value
     * @return {@code true} iff the computed checksum matches the expected value
     */
    boolean verify(byte[] data, String expectedHex);
}
