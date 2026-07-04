// manifest/ManifestValidator.java
package io.swarmshare.manifest;

import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.Manifest;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates the internal consistency of a {@link Manifest}.
 *
 * <h3>Why a dedicated validator?</h3>
 * Validation concerns are kept out of both {@link ManifestBuilder} (build-time)
 * and {@link ManifestSerializer} (I/O-time) to preserve single-responsibility.
 * A peer receiving a manifest over the network should validate it before trusting
 * any of its fields — this class provides that capability independently of how
 * the manifest was obtained.
 *
 * <h3>What is validated?</h3>
 * <ul>
 *   <li>No null or blank required string fields</li>
 *   <li>{@code totalSize} matches the sum of all chunk sizes</li>
 *   <li>Each chunk's {@code offset} equals the sum of preceding chunk sizes</li>
 *   <li>Chunk indices are contiguous from 0</li>
 *   <li>No chunk has a zero or negative size</li>
 *   <li>Each chunk's {@link io.swarmshare.core.domain.ChunkId#manifestHash()} matches
 *       the manifest's {@code fileHash}</li>
 * </ul>
 *
 * <h3>Result type</h3>
 * Returns a {@link ValidationResult} — a sealed type that forces callers to handle
 * both the success and failure cases explicitly, avoiding silent ignoring of errors.
 */
public final class ManifestValidator {

    private ManifestValidator() {
    } // utility class — no instances

    // ── public API ────────────────────────────────────────────────────────────────

    /**
     * Validates the manifest and returns a {@link ValidationResult}.
     *
     * @param manifest the manifest to validate; must not be {@code null}
     * @return {@link ValidationResult.Valid} if consistent,
     * {@link ValidationResult.Invalid} listing all discovered violations
     */
    public static ValidationResult validate(Manifest manifest) {
        List<String> violations = new ArrayList<>();

        checkRequiredStrings(manifest, violations);
        checkChunkList(manifest, violations);
        checkChunkConsistency(manifest, violations);

        return violations.isEmpty()
                ? new ValidationResult.Valid(manifest)
                : new ValidationResult.Invalid(violations);
    }

    // ── checks ───────────────────────────────────────────────────────────────────

    private static void checkRequiredStrings(Manifest manifest, List<String> out) {
        if (isBlank(manifest.fileHash())) out.add("fileHash must not be blank");
        if (isBlank(manifest.fileName())) out.add("fileName must not be blank");
        if (manifest.totalSize() <= 0) out.add("totalSize must be positive, got: " + manifest.totalSize());
        if (manifest.chunkSize() <= 0) out.add("chunkSize must be positive, got: " + manifest.chunkSize());
    }

    private static void checkChunkList(Manifest manifest, List<String> out) {
        if (manifest.chunks() == null || manifest.chunks().isEmpty()) {
            out.add("manifest must contain at least one chunk");
        }
    }

    private static void checkChunkConsistency(Manifest manifest, List<String> out) {
        if (manifest.chunks() == null || manifest.chunks().isEmpty()) return;

        long expectedOffset = 0;
        long observedTotalSize = 0;

        for (int i = 0; i < manifest.chunks().size(); i++) {
            ChunkDescriptor desc = manifest.chunks().get(i);

            // Index contiguity
            if (desc.id().index() != i) {
                out.add("chunk %d has id.index()=%d (expected %d)".formatted(i, desc.id().index(), i));
            }

            // ChunkId must reference this manifest; skip check if manifest.fileHash is blank
            if (!isBlank(manifest.fileHash()) && !manifest.fileHash().equals(desc.id().manifestHash())) {
                out.add("chunk %d has manifestHash='%s' but manifest.fileHash='%s'"
                        .formatted(i, desc.id().manifestHash(), manifest.fileHash()));
            }

            // Positive size
            if (desc.size() <= 0) {
                out.add("chunk %d has non-positive size: %d".formatted(i, desc.size()));
            }

            // Offset correctness
            if (desc.offset() != expectedOffset) {
                out.add("chunk %d has offset=%d but expected %d"
                        .formatted(i, desc.offset(), expectedOffset));
            }

            // SHA-256 field present
            if (isBlank(desc.sha256())) {
                out.add("chunk %d has blank sha256".formatted(i));
            }

            expectedOffset += Math.max(desc.size(), 0);
            observedTotalSize += Math.max(desc.size(), 0);
        }

        // Only check totalSize consistency if chunk sizes themselves were all valid
        if (out.stream().noneMatch(v -> v.contains("non-positive size"))) {
            if (observedTotalSize != manifest.totalSize()) {
                out.add("sum of chunk sizes (%d) does not match totalSize (%d)"
                        .formatted(observedTotalSize, manifest.totalSize()));
            }
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    // ── result type ──────────────────────────────────────────────────────────────

    /**
     * Sealed result of manifest validation.
     * The compiler enforces exhaustive handling in switch expressions.
     *
     * <pre>{@code
     * switch (ManifestValidator.validate(manifest)) {
     *     case ValidationResult.Valid   v -> proceed(v.manifest());
     *     case ValidationResult.Invalid i -> log.error(i.summary());
     * }
     * }</pre>
     */
    public sealed interface ValidationResult
            permits ValidationResult.Valid, ValidationResult.Invalid {

        /**
         * The manifest passed all consistency checks.
         */
        record Valid(Manifest manifest) implements ValidationResult {
        }

        /**
         * One or more consistency violations were found.
         *
         * @param violations an unmodifiable list of human-readable violation messages
         */
        record Invalid(List<String> violations) implements ValidationResult {

            public Invalid {
                violations = List.copyOf(violations); // defensive copy
            }

            /**
             * All violations joined by newlines — convenient for logging.
             */
            public String summary() {
                return String.join(System.lineSeparator(), violations);
            }
        }
    }
}