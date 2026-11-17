package io.swarmshare.storage;

import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link FileChannelStorage} against the real filesystem.
 *
 * <p>
 * Nothing is mocked: the domain records are cheap to build and the point of this
 * adapter is the interaction with {@code FileChannel}. {@link TempDir} gives every
 * test a fresh directory that is deleted afterwards.
 */
class FileChannelStorageTest {

    private static final String MANIFEST_HASH = "a".repeat(64);
    private static final Sha256 SHA = new Sha256();

    @TempDir
    Path tempDir;

    private Path testFile;
    private FileChannelStorage storage;

    @BeforeEach
    void setUp() {
        testFile = tempDir.resolve("storage-test.bin");
        storage = new FileChannelStorage(testFile);
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    // ── preallocateSpace ─────────────────────────────────────────────────────────

    @Test
    void preallocateSpace_createsFileWithExactSize() throws IOException {
        storage.preallocateSpace(1024);

        assertThat(Files.size(testFile)).isEqualTo(1024);
    }

    @Test
    void preallocateSpace_singleByte_createsMinimalFile() throws IOException {
        storage.preallocateSpace(1);

        assertThat(Files.size(testFile)).isEqualTo(1);
    }

    @Test
    void preallocateSpace_createsMissingParentDirectories() throws IOException {
        Path nested = tempDir.resolve("a/b/out.bin");
        try (var nestedStorage = new FileChannelStorage(nested)) {
            nestedStorage.preallocateSpace(8);
        }

        assertThat(Files.size(nested)).isEqualTo(8);
    }

    @Test
    void preallocateSpace_zeroSize_throwsIllegalArgument() {
        assertThatThrownBy(() -> storage.preallocateSpace(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("totalSize must be positive");
    }

    @Test
    void preallocateSpace_negativeSize_throwsIllegalArgument() {
        assertThatThrownBy(() -> storage.preallocateSpace(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void preallocateSpace_existingSameSizeFile_preservesContentsForResume() throws IOException {
        byte[] existing = "already-downloaded-bytes".getBytes();
        Files.write(testFile, existing);

        storage.preallocateSpace(existing.length);

        assertThat(Files.readAllBytes(testFile)).isEqualTo(existing);
    }

    @Test
    void preallocateSpace_largerExistingFile_isTruncatedToManifestSize() throws IOException {
        Files.write(testFile, new byte[100]);

        storage.preallocateSpace(40);

        assertThat(Files.size(testFile)).isEqualTo(40);
    }

    @Test
    void preallocateSpace_shorterExistingFile_isExtendedAndKeepsPrefix() throws IOException {
        Files.write(testFile, "prefix".getBytes());

        storage.preallocateSpace(32);

        byte[] content = Files.readAllBytes(testFile);
        assertThat(content).hasSize(32);
        assertThat(new String(content, 0, 6)).isEqualTo("prefix");
    }

    // ── writeChunk / readChunk ───────────────────────────────────────────────────

    @Test
    void writeChunk_thenReadChunk_returnsIdenticalBytes() {
        byte[] payload = "swarm-chunk-payload".getBytes();
        storage.preallocateSpace(payload.length);

        storage.writeChunk(chunkId(0), 0, payload);

        assertThat(storage.readChunk(chunkId(0), 0, payload.length)).contains(payload);
    }

    @Test
    void writeChunk_atNonZeroOffset_writesOnlyToTargetRange() {
        byte[] payload = "HELLO".getBytes();
        long offset = 16;
        storage.preallocateSpace(64);

        storage.writeChunk(chunkId(0), offset, payload);

        assertThat(storage.readChunk(chunkId(0), 0, (int) offset))
                .hasValueSatisfying(prefix -> assertThat(prefix).containsOnly((byte) 0));
        assertThat(storage.readChunk(chunkId(0), offset, payload.length)).contains(payload);
    }

    @Test
    void writeChunk_adjacentChunks_doNotBleed() {
        byte[] chunkA = "AAAA".getBytes();
        byte[] chunkB = "BBBB".getBytes();
        storage.preallocateSpace(8);

        storage.writeChunk(chunkId(1), 4, chunkB);
        storage.writeChunk(chunkId(0), 0, chunkA);

        assertThat(storage.readChunk(chunkId(0), 0, 4)).contains(chunkA);
        assertThat(storage.readChunk(chunkId(1), 4, 4)).contains(chunkB);
    }

    @Test
    void writeChunk_concurrentVirtualThreadsAtDisjointOffsets_assembleExactFile() throws Exception {
        int chunkSize = 4096;
        int chunks = 64;
        byte[] expected = new byte[chunkSize * chunks];
        new Random(7).nextBytes(expected);
        storage.preallocateSpace(expected.length);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> writes = new ArrayList<>();
            for (int i = chunks - 1; i >= 0; i--) { // reverse order: out-of-order arrival
                int index = i;
                writes.add(executor.submit(() -> {
                    byte[] slice = new byte[chunkSize];
                    System.arraycopy(expected, index * chunkSize, slice, 0, chunkSize);
                    storage.writeChunk(chunkId(index), (long) index * chunkSize, slice);
                }));
            }
            for (Future<?> write : writes) {
                write.get();
            }
        }
        storage.flush();

        assertThat(Files.readAllBytes(testFile)).isEqualTo(expected);
    }

    @Test
    void writeChunk_beforePreallocate_isRejected() {
        assertThatThrownBy(() -> storage.writeChunk(chunkId(0), 0, new byte[]{1}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("preallocateSpace");
    }

    @Test
    void writeChunk_afterReadOnlyLazyOpen_isRejectedWithClearMessage() throws IOException {
        Files.write(testFile, "seed-file".getBytes());
        assertThat(storage.readChunk(chunkId(0), 0, 4)).isPresent(); // opens read-only

        assertThatThrownBy(() -> storage.writeChunk(chunkId(0), 0, new byte[]{1}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("preallocateSpace");
    }

    @Test
    void readChunk_beyondEOF_returnsEmpty() {
        storage.preallocateSpace(8);

        assertThat(storage.readChunk(chunkId(0), 0, 1024)).isEmpty();
    }

    @Test
    void readChunk_exactlyAtEndOfFile_returnsEmpty() {
        storage.preallocateSpace(16);

        assertThat(storage.readChunk(chunkId(0), 16, 1)).isEmpty();
    }

    @Test
    void readChunk_negativeSize_isRejected() {
        storage.preallocateSpace(16);

        assertThatThrownBy(() -> storage.readChunk(chunkId(0), 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readChunk_existingFileWithoutPreallocate_readsFileContents() throws IOException {
        byte[] payload = "existing-file-content".getBytes();
        Files.write(testFile, payload);

        assertThat(storage.readChunk(chunkId(0), 0L, payload.length)).contains(payload);
    }

    @Test
    void readChunk_missingFileWithoutPreallocate_returnsEmpty() {
        assertThat(storage.readChunk(chunkId(0), 0L, 4)).isEmpty();
    }

    // ── checkExistingChunks ──────────────────────────────────────────────────────

    @Test
    void checkExistingChunks_allValidChunks_allBitsSet() {
        byte[] data0 = "chunk-zero".getBytes();
        byte[] data1 = "chunk-one!".getBytes();
        storage.preallocateSpace(20);
        storage.writeChunk(chunkId(0), 0, data0);
        storage.writeChunk(chunkId(1), 10, data1);

        BitSet result = storage.checkExistingChunks(manifestOf(10, data0, data1));

        assertThat(result.cardinality()).isEqualTo(2);
    }

    @Test
    void checkExistingChunks_corruptedChunk_bitNotSet() {
        byte[] good = "good-data-123456".getBytes();
        byte[] expectedSecond = "second-chunk-abc".getBytes();
        storage.preallocateSpace(32);
        storage.writeChunk(chunkId(0), 0, good);
        storage.writeChunk(chunkId(1), 16, "xxxx-corrupted!!".getBytes());

        BitSet result = storage.checkExistingChunks(manifestOf(16, good, expectedSecond));

        assertThat(result.get(0)).as("valid chunk 0").isTrue();
        assertThat(result.get(1)).as("corrupted chunk 1").isFalse();
    }

    @Test
    void checkExistingChunks_tornWriteOfLastChunk_isNotTrusted() throws IOException {
        byte[] data0 = "complete".getBytes();
        byte[] data1 = "torn-end".getBytes();
        Files.write(testFile, (new String(data0) + "torn").getBytes()); // crash mid-write

        BitSet result = storage.checkExistingChunks(manifestOf(8, data0, data1));

        assertThat(result.get(0)).isTrue();
        assertThat(result.get(1)).isFalse();
    }

    @Test
    void preallocateSpace_onCompleteFile_doesNotInvalidateExistingChunks() {
        byte[] data0 = "resume-chunk-0".getBytes();
        byte[] data1 = "resume-chunk-1".getBytes();
        storage.preallocateSpace(28);
        storage.writeChunk(chunkId(0), 0, data0);
        storage.writeChunk(chunkId(1), 14, data1);
        storage.close();

        try (var resumed = new FileChannelStorage(testFile)) {
            resumed.preallocateSpace(28);

            assertThat(resumed.checkExistingChunks(manifestOf(14, data0, data1)).cardinality()).isEqualTo(2);
        }
    }

    // ── flush / close ────────────────────────────────────────────────────────────

    @Test
    void flush_afterWrites_keepsDataReadable() {
        storage.preallocateSpace(16);
        storage.writeChunk(chunkId(0), 0L, "durable".getBytes());

        storage.flush();

        assertThat(storage.readChunk(chunkId(0), 0, 7)).contains("durable".getBytes());
    }

    @Test
    void flush_beforePreallocate_isNoOp() {
        assertThatCode(storage::flush).doesNotThrowAnyException();
        assertThat(testFile).doesNotExist();
    }

    @Test
    void close_isIdempotent() {
        storage.preallocateSpace(64);

        assertThatCode(() -> {
            storage.close();
            storage.close();
        }).doesNotThrowAnyException();
    }

    @Test
    void close_isTerminal_readsAndWritesAreRejected() {
        storage.preallocateSpace(8);
        storage.close();

        assertThatThrownBy(() -> storage.readChunk(chunkId(0), 0, 4))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> storage.writeChunk(chunkId(0), 0, new byte[]{1}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> storage.preallocateSpace(8))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void close_releasesFileSoItCanBeDeleted() throws IOException {
        storage.preallocateSpace(32);
        storage.close();

        Files.delete(testFile);

        assertThat(testFile).doesNotExist();
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static ChunkId chunkId(int index) {
        return new ChunkId(MANIFEST_HASH, index);
    }

    /** Manifest whose chunks are laid out back to back with the hashes of {@code contents}. */
    private static Manifest manifestOf(int chunkSize, byte[]... contents) {
        List<ChunkDescriptor> descriptors = new ArrayList<>();
        long offset = 0;
        for (int i = 0; i < contents.length; i++) {
            descriptors.add(new ChunkDescriptor(chunkId(i), offset, contents[i].length, SHA.compute(contents[i])));
            offset += contents[i].length;
        }
        return new Manifest(MANIFEST_HASH, "test.bin", offset, chunkSize, descriptors);
    }
}
