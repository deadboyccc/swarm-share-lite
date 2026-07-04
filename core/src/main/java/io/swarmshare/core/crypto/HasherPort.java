package io.swarmshare.core.crypto;

/**
 * Core port for checksum computation and verification.
 */
public interface HasherPort {

    String compute(byte[] data);

    boolean verify(byte[] data, String expectedHex);
}
