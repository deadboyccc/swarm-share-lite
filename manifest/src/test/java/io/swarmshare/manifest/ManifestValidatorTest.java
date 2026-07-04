// manifest/ManifestValidatorTest.java
package io.swarmshare.manifest;

import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.manifest.ManifestValidator.ValidationResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ManifestValidator}.
 *
 * <p>The sealed {@link ValidationResult} type forces exhaustive handling; these tests
 * also verify that callers can use pattern matching on both the {@code Valid} and
 * {@code Invalid} variants without needing a default branch.
 *
 * <p>Coverage targets:
 * <ul>
 *   <li>Valid manifests — single chunk and multi-chunk pass without violations</li>
 *   <li>Blank/null required string fields</li>
 *   <li>Non-positive totalSize and chunkSize</li>
 *   <li>Empty chunk list</li>
 *   <li>Non-contiguous chunk indices</li>
 *   <li>Wrong chunk offsets</li>
 *   <li>totalSize mismatch with sum of chunk sizes</li>
 *   <li>ChunkId.manifestHash mismatch with manifest.fileHash</li>
 *   <li>Blank chunk sha256</li>
 *   <li>Multiple simultaneous violations all reported</li>
 *   <li>Pattern matching ergonomics on ValidationResult</li>
 * </ul>
 */
class ManifestValidatorTest {

    // ── factory helpers ───────────────────────────────────────────────────────────

    private static final String VALID_HASH = "a".repeat(64);

    private static ChunkDescriptor chunk(String manifestHash, int index, long offset, int size) {
        return new ChunkDescriptor(new ChunkId(manifestHash, index), offset, size,
                "b".repeat(64));
    }

    private static Manifest validSingleChunk() {
        return new Manifest(VALID_HASH, "file.bin", 100L, 100,
                List.of(chunk(VALID_HASH, 0, 0L, 100)));
    }

    private static Manifest validTwoChunks() {
        return new Manifest(VALID_HASH, "file.bin", 200L, 100,
                List.of(
                        chunk(VALID_HASH, 0, 0L, 100),
                        chunk(VALID_HASH, 1, 100L, 100)));
    }

    // ── valid manifests ───────────────────────────────────────────────────────────

    @Test
    void validate_validSingleChunkManifest_returnsValid() {
        ValidationResult result = ManifestValidator.validate(validSingleChunk());

        assertThat(result).isInstanceOf(ValidationResult.Valid.class);
    }

    @Test
    void validate_validTwoChunkManifest_returnsValid() {
        ValidationResult result = ManifestValidator.validate(validTwoChunks());

        assertThat(result).isInstanceOf(ValidationResult.Valid.class);
    }

    @Test
    void validate_valid_resultContainsOriginalManifest() {
        Manifest manifest = validSingleChunk();

        ValidationResult result = ManifestValidator.validate(manifest);

        // Pattern matching — no cast needed
        assertThat(result).isInstanceOf(ValidationResult.Valid.class);
        ValidationResult.Valid valid = (ValidationResult.Valid) result;
        assertThat(valid.manifest()).isSameAs(manifest);
    }

    // ── pattern matching ergonomics ───────────────────────────────────────────────

    @Test
    void validate_patternMatchSwitch_compilesExhaustivelyWithNoDefault() {
        // This test verifies that switch on the sealed type compiles without 'default'
        // and routes to the correct branch — a compile-time guarantee that future
        // permits additions won't silently fall through.
        Manifest manifest = validSingleChunk();
        ValidationResult result = ManifestValidator.validate(manifest);

        String outcome = switch (result) {
            case ValidationResult.Valid v -> "valid";
            case ValidationResult.Invalid i -> "invalid: " + i.summary();
        };

        assertThat(outcome).isEqualTo("valid");
    }

    // ── blank/null required strings ───────────────────────────────────────────────

    @ParameterizedTest(name = "fileHash [{0}] rejected")
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void validate_blankFileHash_returnsInvalidWithViolation(String bad) {
        Manifest manifest = new Manifest(bad, "f.bin", 1L, 1,
                List.of(chunk("x", 0, 0L, 1)));

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        ValidationResult.Invalid invalid = (ValidationResult.Invalid) result;
        assertThat(invalid.violations()).anyMatch(v -> v.contains("fileHash"));
    }

    @ParameterizedTest(name = "fileName [{0}] rejected")
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    void validate_blankFileName_returnsInvalidWithViolation(String bad) {
        Manifest manifest = new Manifest(VALID_HASH, bad, 1L, 1,
                List.of(chunk(VALID_HASH, 0, 0L, 1)));

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        assertThat(((ValidationResult.Invalid) result).violations())
                .anyMatch(v -> v.contains("fileName"));
    }

    // ── non-positive numeric fields ───────────────────────────────────────────────

    @ParameterizedTest(name = "totalSize {0} is invalid")
    @ValueSource(longs = {0L, -1L, Long.MIN_VALUE})
    void validate_nonPositiveTotalSize_returnsInvalid(long bad) {
        Manifest manifest = new Manifest(VALID_HASH, "f.bin", bad, 1,
                List.of(chunk(VALID_HASH, 0, 0L, 1)));

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        assertThat(((ValidationResult.Invalid) result).violations())
                .anyMatch(v -> v.contains("totalSize"));
    }

    @ParameterizedTest(name = "chunkSize {0} is invalid")
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void validate_nonPositiveChunkSize_returnsInvalid(int bad) {
        Manifest manifest = new Manifest(VALID_HASH, "f.bin", 1L, bad,
                List.of(chunk(VALID_HASH, 0, 0L, 1)));

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        assertThat(((ValidationResult.Invalid) result).violations())
                .anyMatch(v -> v.contains("chunkSize"));
    }

    // ── chunk list problems ───────────────────────────────────────────────────────

    @Test
    void validate_emptyChunkList_returnsInvalid() {
        Manifest manifest = new Manifest(VALID_HASH, "f.bin", 100L, 100, List.of());

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        assertThat(((ValidationResult.Invalid) result).violations())
                .anyMatch(v -> v.contains("at least one chunk"));
    }

    @Test
    void validate_nonContiguousChunkIndices_returnsInvalid() {
        // Chunk 0 at index 0, then chunk 1 claims index 2 (skipping 1)
        List<ChunkDescriptor> chunks = List.of(
                chunk(VALID_HASH, 0, 0L, 100),
                new ChunkDescriptor(new ChunkId(VALID_HASH, 2), 100L, 100, "b".repeat(64)));
        Manifest manifest = new Manifest(VALID_HASH, "f.bin", 200L, 100, chunks);

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        assertThat(((ValidationResult.Invalid) result).violations())
                .anyMatch(v -> v.contains("index"));
    }

    @Test
    void validate_wrongChunkOffset_returnsInvalid() {
        // Chunk 1 claims offset=50, but it should be 100 (after a 100-byte chunk 0)
        List<ChunkDescriptor> chunks = List.of(
                chunk(VALID_HASH, 0, 0L, 100),
                chunk(VALID_HASH, 1, 50L, 100)); // wrong: should be 100
        Manifest manifest = new Manifest(VALID_HASH, "f.bin", 200L, 100, chunks);

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        assertThat(((ValidationResult.Invalid) result).violations())
                .anyMatch(v -> v.contains("offset"));
    }

    @Test
    void validate_totalSizeMismatch_returnsInvalid() {
        // chunks sum to 100 bytes, but totalSize claims 999
        List<ChunkDescriptor> chunks = List.of(chunk(VALID_HASH, 0, 0L, 100));
        Manifest manifest = new Manifest(VALID_HASH, "f.bin", 999L, 100, chunks);

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        assertThat(((ValidationResult.Invalid) result).violations())
                .anyMatch(v -> v.contains("totalSize"));
    }

    @Test
    void validate_chunkManifestHashMismatch_returnsInvalid() {
        // Chunk's ChunkId references "wrong-hash", not the manifest's fileHash
        String wrongHash = "c".repeat(64);
        List<ChunkDescriptor> chunks = List.of(
                new ChunkDescriptor(new ChunkId(wrongHash, 0), 0L, 100, "b".repeat(64)));
        Manifest manifest = new Manifest(VALID_HASH, "f.bin", 100L, 100, chunks);

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        assertThat(((ValidationResult.Invalid) result).violations())
                .anyMatch(v -> v.contains("manifestHash"));
    }

    @ParameterizedTest(name = "blank chunk sha256 [{0}]")
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    void validate_blankChunkSha256_returnsInvalid(String bad) {
        List<ChunkDescriptor> chunks = List.of(
                new ChunkDescriptor(new ChunkId(VALID_HASH, 0), 0L, 100, bad));
        Manifest manifest = new Manifest(VALID_HASH, "f.bin", 100L, 100, chunks);

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        assertThat(((ValidationResult.Invalid) result).violations())
                .anyMatch(v -> v.contains("sha256"));
    }

    // ── multiple violations ───────────────────────────────────────────────────────

    @Test
    void validate_multipleViolations_allReportedTogether() {
        // Both fileHash and fileName are blank — both must appear in the violation list
        Manifest manifest = new Manifest("", "", -1L, -1, List.of());

        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result).isInstanceOf(ValidationResult.Invalid.class);
        ValidationResult.Invalid invalid = (ValidationResult.Invalid) result;
        assertThat(invalid.violations().size()).isGreaterThan(1);
        assertThat(invalid.violations()).anyMatch(v -> v.contains("fileHash"));
        assertThat(invalid.violations()).anyMatch(v -> v.contains("fileName"));
        assertThat(invalid.violations()).anyMatch(v -> v.contains("totalSize"));
        assertThat(invalid.violations()).anyMatch(v -> v.contains("chunkSize"));
    }

    // ── Invalid.summary() ────────────────────────────────────────────────────────

    @Test
    void invalid_summary_containsAllViolationMessages() {
        Manifest manifest = new Manifest("", "", -1L, -1, List.of());
        ValidationResult.Invalid invalid =
                (ValidationResult.Invalid) ManifestValidator.validate(manifest);

        String summary = invalid.summary();
        invalid.violations().forEach(v -> assertThat(summary).contains(v));
    }

    // ── violations list is immutable ──────────────────────────────────────────────

    @Test
    void invalid_violations_listIsUnmodifiable() {
        Manifest manifest = new Manifest("", "f.bin", 1L, 1, List.of());
        ValidationResult.Invalid invalid =
                (ValidationResult.Invalid) ManifestValidator.validate(manifest);

        assertThat(invalid.violations())
                .isUnmodifiable();
    }

    // ── builder-generated manifest always validates ───────────────────────────────

    @Test
    void validate_manifestFromBuilder_isAlwaysValid(@org.junit.jupiter.api.io.TempDir
                                                    java.nio.file.Path tempDir)
            throws java.io.IOException {
        byte[] content = new byte[300];
        new java.util.Random(42).nextBytes(content);
        java.nio.file.Path file = tempDir.resolve("test.bin");
        java.nio.file.Files.write(file, content);

        Manifest manifest = new ManifestBuilder(64).build(file);
        ValidationResult result = ManifestValidator.validate(manifest);

        assertThat(result)
                .as("ManifestBuilder should always produce a valid manifest")
                .isInstanceOf(ValidationResult.Valid.class);
    }
}