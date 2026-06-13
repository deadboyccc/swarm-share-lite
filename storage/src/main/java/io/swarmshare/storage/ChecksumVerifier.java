// storage/ChecksumVerifier.java
package io.swarmshare.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 verification.
 * <p>
 * MessageDigest is NOT thread-safe — never share an instance across threads.
 * Options:
 * 1. Create per call (simple, slight GC pressure — fine for chunk-sized work)
 * 2. ThreadLocal<MessageDigest> (reuse, zero contention)
 * <p>
 * We use option 2 for production path:
 */
public class ChecksumVerifier {

    // ThreadLocal: each virtual thread gets its own MessageDigest instance
    // TODO check if using virtual threads here will create alot of MessageDigest objects
    private static final ThreadLocal<MessageDigest> DIGEST =
            ThreadLocal.withInitial(() -> {
                try {
                    return MessageDigest.getInstance("SHA-256");
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            });

    // hashes the input byte[] into a hexFormat md5 hash
    public String compute(byte[] data) {
        MessageDigest md = DIGEST.get();
        md.reset(); // must reset before reuse
        return HexFormat.of().formatHex(md.digest(data));
    }

    // given byte[] and  hash - verify mdsHash(bytes[]) == given hash
    public boolean verify(byte[] data, String expectedHash) {
        if (expectedHash == null) {
            return false;
        }
        String computed = compute(data);
        return computed.equalsIgnoreCase(expectedHash);
    }
}