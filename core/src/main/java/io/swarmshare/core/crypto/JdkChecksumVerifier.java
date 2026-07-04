package io.swarmshare.core.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Simple JDK-backed implementation of the ChecksumVerifierPort.
 * Placed in core so higher-level modules can use it as a sensible default
 * without pulling infrastructure dependencies.
 */
public final class JdkChecksumVerifier implements ChecksumVerifierPort {

    private static final String ALGORITHM = "SHA-256";
    private static final HexFormat HEX = HexFormat.of();

    private static byte[] digest(byte[] data) {
        try {
            return MessageDigest.getInstance(ALGORITHM).digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 not available on this JVM", e);
        }
    }

    @Override
    public String compute(byte[] data) {
        return HEX.formatHex(digest(data));
    }

    @Override
    public boolean verify(byte[] data, String expectedHex) {
        if (expectedHex == null || expectedHex.isBlank()) return false;
        byte[] expected;
        try {
            expected = HEX.parseHex(expectedHex.toLowerCase());
        } catch (IllegalArgumentException e) {
            return false;
        }
        return MessageDigest.isEqual(digest(data), expected);
    }
}
