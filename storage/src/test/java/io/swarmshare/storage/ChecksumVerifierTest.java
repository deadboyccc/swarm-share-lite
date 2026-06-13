package io.swarmshare.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChecksumVerifierTest {

    // one instance of checkSum to check ThreadLocal Behavior
    private final ChecksumVerifier checksumVerifier = new ChecksumVerifier();

    @BeforeEach
    void setUp() {
    }

    @Test
    void compute_WithKnownInput_ReturnsCorrectSha256Hex() {
        // Arrange: "hello world" has a globally known standard SHA-256 hash
        byte[] input = "hello world".getBytes(StandardCharsets.UTF_8);
        String expectedHash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

        // Act
        String actualHash = checksumVerifier.compute(input);

        // Assert
        assertThat(actualHash).isEqualTo(expectedHash);
    }

    @Test
    void compute_WithEmptyInput_ReturnsCorrectSha256Hex() {
        // Arrange: Empty byte array hash standard
        byte[] input = new byte[0];
        String expectedHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        // Act
        String actualHash = checksumVerifier.compute(input);

        // Assert
        assertThat(actualHash).isEqualTo(expectedHash);
    }

    @Test
    void verify_WithMatchingHash_ReturnsTrue() {
        // Arrange
        byte[] input = "swarm-share-lite".getBytes(StandardCharsets.UTF_8);
        String correctHash = checksumVerifier.compute(input);

        // Act & Assert
        assertTrue(checksumVerifier.verify(input, correctHash),
                "Verification should pass when hashes match exactly.");
    }

    @Test
    void verify_WithMismatchedHash_ReturnsFalse() {
        // Arrange
        byte[] input = "swarm-share-lite".getBytes(StandardCharsets.UTF_8);
        String wrongHash = "0000000000000000000000000000000000000000000000000000000000000000";

        // Act & Assert
        assertFalse(checksumVerifier.verify(input, wrongHash),
                "Verification should fail when a corrupted hash is provided.");
    }

    @Test
    void verify_IsCaseInsensitiveForHexStrings() {
        // Arrange
        byte[] input = "test-case".getBytes(StandardCharsets.UTF_8);
        String lowerCaseHash = checksumVerifier.compute(input);
        String upperCaseHash = lowerCaseHash.toUpperCase();

        // Act & Assert
        assertTrue(checksumVerifier.verify(input, upperCaseHash),
                "Verification should handle upper-case hex characters gracefully.");
    }

    @Test
    void compute_ConcurrentlyOnMultipleThreads_MaintainsThreadSafety() throws InterruptedException {
        // Arrange: Test JEP 444 behavior—ensure ThreadLocal handles multiple concurrent workers cleanly
        byte[] inputA = "data-stream-A".getBytes(StandardCharsets.UTF_8);
        byte[] inputB = "data-stream-B".getBytes(StandardCharsets.UTF_8);
        byte[] inputC = "data-stream-C".getBytes(StandardCharsets.UTF_8);

        String expectedHashA = checksumVerifier.compute(inputA);
        String expectedHashB = checksumVerifier.compute(inputB);
        String expectedHashC = checksumVerifier.compute(inputC);

        // Act & Assert running tasks simultaneously on individual Virtual Threads
        Thread vt1 = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < 500; i++) {
                assertThat(checksumVerifier.compute(inputA)).isEqualTo(expectedHashA);
            }
        });

        Thread vt2 = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < 50; i++) {
                assertThat(checksumVerifier.compute(inputB)).isEqualTo(expectedHashB);
            }
        });

        Thread vt3 = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < 50; i++) {
                assertThat(checksumVerifier.compute(inputC)).isEqualTo(expectedHashC);
            }
        });


        vt1.join();
        vt2.join();
        vt3.join();
    }
}
