package io.swarmshare.core.crypto;

/**
 * Core port for checksum computation and verification.
 * Implementations live in infrastructure modules (e.g. storage).
 */
public interface HasherPort {

    String compute(byte[] data);

    boolean verify(byte[] data, String expectedHex);
}
