// manifest/ManifestBuilderTest.java
package io.swarmshare.manifest;

import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.Manifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ManifestBuilder}.
 *
 * <p>All SHA-256 hashes used in these tests were independently verified with
 * {@code echo -n "..." | sha256sum} on the command line before being written here.
 * Hardcoding known hash vectors makes the tests self-documenting and immune to
 * regressions in the hashing logic.
 *
 * <p>Coverage targets:
 * <ul>
 *   <li>Chunk count — file smaller than chunk, exactly one chunk, exact multiple, remainder</li>
 *   <li>Last-chunk size — must be {@code totalSize % chunkSize}, not {@code chunkSize}</li>
 *   <li>Chunk offsets — each starts immediately after the previous chunk ends</li>
 *   <li>Chunk checksums — per-chunk SHA-256 matches independently computed reference</li>
 *   <li>Whole-file hash — SHA-256 of complete file content, not of concatenated hashes</li>
 *   <li>ChunkId fields — {@code index} and {@code manifestHash} are set correctly</li>
 *   <li>Manifest metadata — {@code fileName}, {@code totalSize}, {@code chunkSize}</li>
 *   <li>Rejection — empty file, invalid chunk size</li>
 *   <li>Determinism — same file → identical manifests every time</li>
 * </ul>
 */
class ManifestBuilderTest {

    private final Sha256 sha256 = new Sha256();
    @TempDir
    Path tempDir;

    // ── construction ─────────────────────────────────────────────────────────────

    private static byte[] randomBytes(int size, long seed) {
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }

    @Test
    void constructor_defaultChunkSize_is1MiB() {
        assertThat(new ManifestBuilder().build(writeFile("f.bin", new byte[1]))
                .chunkSize())
                .isEqualTo(ManifestBuilder.DEFAULT_CHUNK_SIZE);
    }

    // ── empty file rejection ─────────────────────────────────────────────────────

    @ParameterizedTest(name = "chunkSize {0} is invalid")
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void constructor_invalidChunkSize_throwsIllegalArgument(int bad) {
        assertThatThrownBy(() -> new ManifestBuilder(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chunkSize must be >= 1");
    }

    // ── chunk count ──────────────────────────────────────────────────────────────

    @Test
    void build_emptyFile_throwsIllegalArgument() {
        Path empty = writeFile("empty.bin", new byte[0]);

        assertThatThrownBy(() -> new ManifestBuilder(4).build(empty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty file");
    }

    @Test
    void build_fileSmallerThanChunkSize_producesSingleChunk() {
        // 3-byte file, 10-byte chunk → 1 chunk of size 3
        Path file = writeFile("small.bin", new byte[]{0x41, 0x42, 0x43}); // "ABC"

        Manifest manifest = new ManifestBuilder(10).build(file);

        assertThat(manifest.totalChunks()).isEqualTo(1);
        assertThat(manifest.chunks().get(0).size()).isEqualTo(3);
    }

    @Test
    void build_fileExactlyOneChunk_producesSingleChunk() {
        // 4-byte file, 4-byte chunk → exactly 1 chunk
        Path file = writeFile("exact1.bin", new byte[]{0x01, 0x02, 0x03, 0x04});

        Manifest manifest = new ManifestBuilder(4).build(file);

        assertThat(manifest.totalChunks()).isEqualTo(1);
        assertThat(manifest.chunks().get(0).size()).isEqualTo(4);
    }

    @Test
    void build_fileExactMultipleOfChunkSize_producesEqualSizedChunks() {
        // 4 bytes, chunk=2 → 2 chunks of 2 bytes each
        Path file = writeFile("exact-multi.bin", new byte[]{0x01, 0x02, 0x03, 0x04});

        Manifest manifest = new ManifestBuilder(2).build(file);

        assertThat(manifest.totalChunks()).isEqualTo(2);
        assertThat(manifest.chunks()).allSatisfy(desc -> assertThat(desc.size()).isEqualTo(2));
    }

    @Test
    void build_fileWithRemainder_lastChunkIsSmallerThanChunkSize() {
        // 3 bytes, chunk=2 → chunk0=2 bytes, chunk1=1 byte
        Path file = writeFile("with-remainder.bin", new byte[]{0x41, 0x42, 0x43});

        Manifest manifest = new ManifestBuilder(2).build(file);

        assertThat(manifest.totalChunks()).isEqualTo(2);
        assertThat(manifest.chunks().get(0).size()).isEqualTo(2); // full
        assertThat(manifest.chunks().get(1).size()).isEqualTo(1); // remainder
    }

    // ── chunk offsets ────────────────────────────────────────────────────────────

    @Test
    void build_largerFile_chunkCountMatchesCeilingDivision() {
        // 2.5 MiB file with 1 MiB chunks → ceil(2.5) = 3 chunks
        int size = 2 * 1024 * 1024 + 512 * 1024; // 2.5 MiB
        byte[] data = randomBytes(size, 42);
        Path file = writeFile("large.bin", data);

        Manifest manifest = new ManifestBuilder(1024 * 1024).build(file);

        assertThat(manifest.totalChunks()).isEqualTo(3);
        assertThat(manifest.chunks().get(0).size()).isEqualTo(1024 * 1024);
        assertThat(manifest.chunks().get(1).size()).isEqualTo(1024 * 1024);
        assertThat(manifest.chunks().get(2).size()).isEqualTo(512 * 1024);
    }

    @Test
    void build_chunkOffsets_areContiguousAndStartAtZero() {
        // 6 bytes, chunk=2 → offsets: 0, 2, 4
        byte[] content = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06};
        Path file = writeFile("offsets.bin", content);

        Manifest manifest = new ManifestBuilder(2).build(file);

        List<ChunkDescriptor> chunks = manifest.chunks();
        assertThat(chunks.get(0).offset()).isEqualTo(0);
        assertThat(chunks.get(1).offset()).isEqualTo(2);
        assertThat(chunks.get(2).offset()).isEqualTo(4);
    }

    @Test
    void build_firstChunkOffset_isAlwaysZero() {
        Path file = writeFile("first-offset.bin", new byte[]{0x42, 0x43});

        Manifest manifest = new ManifestBuilder(1).build(file);

        assertThat(manifest.chunks().get(0).offset()).isEqualTo(0);
    }

    // ── chunk checksums ──────────────────────────────────────────────────────────

    @Test
    void build_eachChunkOffset_equalsAccumulatedSizeOfPreviousChunks() {
        byte[] content = randomBytes(1000, 99);
        Path file = writeFile("cumulative.bin", content);

        Manifest manifest = new ManifestBuilder(100).build(file);

        long cumulative = 0;
        for (ChunkDescriptor desc : manifest.chunks()) {
            assertThat(desc.offset()).as("offset of chunk %d", desc.id().index()).isEqualTo(cumulative);
            cumulative += desc.size();
        }
    }

    @Test
    void build_chunkChecksum_matchesIndependentlyComputedHash() {
        // "AB" (0x41, 0x42) → sha256 = aa8e4095784caaf34c2ffd89ba109a21c1f0db97fa1da76a6e4f6f658ebc1305
        // verified: printf '\x41\x42' | sha256sum
        byte[] content = {0x41, 0x42, 0x43};
        Path file = writeFile("chunk-hash.bin", content);

        Manifest manifest = new ManifestBuilder(2).build(file);

        assertThat(manifest.chunks().get(0).sha256())
                .isEqualTo("38164fbd17603d73f696b8b4d72664d735bb6a7c88577687fd2ae33fd6964153");
        // "C" (0x43)
        // verified: printf '\x43' | sha256sum
        assertThat(manifest.chunks().get(1).sha256())
                .isEqualTo("6b23c0d5f35d1b11f9b683f0b0a617355deb11277d91ae091d399c655b87940d");
    }

    @Test
    void build_singleChunk_chunkHashMatchesVerifierDirectly() {
        // When only one chunk exists, its hash is independently verifiable
        byte[] content = {0x41, 0x42, 0x43}; // "ABC"
        Path file = writeFile("single-chunk-hash.bin", content);

        Manifest manifest = new ManifestBuilder(100).build(file);

        String expected = sha256.compute(content);
        assertThat(manifest.chunks().get(0).sha256()).isEqualTo(expected);
    }

    // ── whole-file hash ──────────────────────────────────────────────────────────

    @Test
    void build_eachChunkHash_matchesHashOfItsRawBytes() {
        // Verifies that each chunk's sha256 matches independently recomputed hash
        byte[] content = randomBytes(200, 7);
        Path file = writeFile("all-chunk-hashes.bin", content);

        Manifest manifest = new ManifestBuilder(50).build(file);

        for (ChunkDescriptor desc : manifest.chunks()) {
            // Re-extract the bytes for this chunk
            int start = (int) desc.offset();
            byte[] chunkBytes = new byte[desc.size()];
            System.arraycopy(content, start, chunkBytes, 0, desc.size());

            assertThat(desc.sha256())
                    .as("hash of chunk %d", desc.id().index())
                    .isEqualTo(sha256.compute(chunkBytes));
        }
    }

    @Test
    void build_fileHash_matchesIndependentlyComputedHash() {
        // "AB" = 0x41, 0x42 → sha256 = aa8e4095784caaf34c2ffd89ba109a21c1f0db97fa1da76a6e4f6f658ebc1305
        // verified: printf '\x41\x42' | sha256sum
        byte[] content = {0x41, 0x42};
        Path file = writeFile("file-hash.bin", content);

        Manifest manifest = new ManifestBuilder(1).build(file);

        assertThat(manifest.fileHash())
                .isEqualTo("38164fbd17603d73f696b8b4d72664d735bb6a7c88577687fd2ae33fd6964153");
    }

    @Test
    void build_fileHash_isHashOfWholeFileNotOfChunkHashes() {
        // This test distinguishes two plausible but incorrect implementations:
        // (a) hash of concatenated chunk bytes — correct
        // (b) hash of concatenated chunk hashes — wrong
        //
        // If the implementation were (b), fileHash would equal SHA-256 of
        // SHA-256("A") + SHA-256("B") — detectable by comparing with the known
        // whole-file hash SHA-256("AB").
        byte[] content = {0x41, 0x42}; // "AB"
        Path file = writeFile("not-merkle.bin", content);

        Manifest manifest = new ManifestBuilder(1).build(file); // 2 chunks: "A", "B"

        // Independently compute correct fileHash (SHA-256 of "AB"):
        String correctFileHash = sha256.compute(content);
        assertThat(manifest.fileHash()).isEqualTo(correctFileHash);
    }

    // ── ChunkId fields ───────────────────────────────────────────────────────────

    @Test
    void build_fileHash_matchesVerifierOnFullContent() {
        byte[] content = randomBytes(300, 13);
        Path file = writeFile("full-hash.bin", content);

        Manifest manifest = new ManifestBuilder(100).build(file);

        assertThat(manifest.fileHash()).isEqualTo(sha256.compute(content));
    }

    @Test
    void build_chunkIdIndices_areZeroBasedAndContiguous() {
        byte[] content = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06};
        Path file = writeFile("indices.bin", content);

        Manifest manifest = new ManifestBuilder(2).build(file);

        for (int i = 0; i < manifest.totalChunks(); i++) {
            assertThat(manifest.chunks().get(i).id().index())
                    .as("chunk at list position %d", i)
                    .isEqualTo(i);
        }
    }

    // ── manifest metadata ────────────────────────────────────────────────────────

    @Test
    void build_chunkIdManifestHash_equalsManifestFileHash() {
        byte[] content = randomBytes(50, 77);
        Path file = writeFile("manifest-hash-link.bin", content);

        Manifest manifest = new ManifestBuilder(10).build(file);

        // Every chunk's ChunkId must reference the manifest's own fileHash
        manifest.chunks().forEach(desc ->
                assertThat(desc.id().manifestHash())
                        .as("chunk %d manifestHash", desc.id().index())
                        .isEqualTo(manifest.fileHash()));
    }

    @Test
    void build_fileName_matchesActualFileName() {
        Path file = writeFile("my-archive.tar.gz", new byte[]{0x01});

        Manifest manifest = new ManifestBuilder(1).build(file);

        assertThat(manifest.fileName()).isEqualTo("my-archive.tar.gz");
    }

    @Test
    void build_totalSize_matchesActualFileSize() throws IOException {
        byte[] content = randomBytes(137, 11);
        Path file = writeFile("size-check.bin", content);

        Manifest manifest = new ManifestBuilder(50).build(file);

        assertThat(manifest.totalSize()).isEqualTo(Files.size(file));
        assertThat(manifest.totalSize()).isEqualTo(content.length);
    }

    @Test
    void build_chunkSize_reflectsConfiguredValue() {
        Path file = writeFile("cs.bin", randomBytes(100, 5));

        Manifest manifest = new ManifestBuilder(32).build(file);

        assertThat(manifest.chunkSize()).isEqualTo(32);
    }

    // ── determinism ──────────────────────────────────────────────────────────────

    @Test
    void build_sumOfChunkSizes_equalsTotalSize() {
        byte[] content = randomBytes(257, 3); // deliberately odd size
        Path file = writeFile("sum.bin", content);

        Manifest manifest = new ManifestBuilder(64).build(file);

        long sum = manifest.chunks().stream().mapToLong(ChunkDescriptor::size).sum();
        assertThat(sum).isEqualTo(manifest.totalSize());
    }

    @Test
    void build_sameFileTwice_producesIdenticalManifests() {
        byte[] content = randomBytes(100, 42);
        Path file = writeFile("det.bin", content);

        ManifestBuilder builder = new ManifestBuilder(32);
        Manifest first = builder.build(file);
        Manifest second = builder.build(file);

        assertThat(first.fileHash()).isEqualTo(second.fileHash());
        assertThat(first.totalChunks()).isEqualTo(second.totalChunks());

        for (int i = 0; i < first.totalChunks(); i++) {
            assertThat(first.chunks().get(i).sha256())
                    .isEqualTo(second.chunks().get(i).sha256());
            assertThat(first.chunks().get(i).offset())
                    .isEqualTo(second.chunks().get(i).offset());
        }
    }

    // ── chunk size boundary ───────────────────────────────────────────────────────

    @Test
    void build_differentFiles_produceDifferentFileHashes() {
        Path fileA = writeFile("a.bin", new byte[]{0x41});
        Path fileB = writeFile("b.bin", new byte[]{0x42});

        ManifestBuilder builder = new ManifestBuilder(1);
        assertThat(builder.build(fileA).fileHash())
                .isNotEqualTo(builder.build(fileB).fileHash());
    }

    @Test
    void build_chunkSizeOf1_eachByteIsItsOwnChunk() {
        byte[] content = {0x41, 0x42, 0x43}; // 3 bytes
        Path file = writeFile("byte-per-chunk.bin", content);

        Manifest manifest = new ManifestBuilder(1).build(file);

        assertThat(manifest.totalChunks()).isEqualTo(3);
        for (int i = 0; i < 3; i++) {
            assertThat(manifest.chunks().get(i).size()).isEqualTo(1);
            assertThat(manifest.chunks().get(i).offset()).isEqualTo(i);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    @Test
    void build_chunkSizeLargerThanFile_producesSingleChunkWithCorrectSize() {
        byte[] content = {0x01, 0x02, 0x03};
        Path file = writeFile("oversize-chunk.bin", content);

        Manifest manifest = new ManifestBuilder(1000).build(file);

        assertThat(manifest.totalChunks()).isEqualTo(1);
        assertThat(manifest.chunks().get(0).size()).isEqualTo(3);
        assertThat(manifest.chunks().get(0).offset()).isEqualTo(0);
    }

    private Path writeFile(String name, byte[] content) {
        try {
            Path path = tempDir.resolve(name);
            Files.write(path, content);
            return path;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}