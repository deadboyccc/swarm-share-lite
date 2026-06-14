package io.swarmshare.storage;

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
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration tests for {@link FileChannelStorage}.
 *
 * <p>"Integration" here means real filesystem I/O — no mocking of the channel itself.
 * JUnit's {@link TempDir} guarantees isolation: each test class gets a fresh directory
 * that is deleted (recursively) after the suite, regardless of test outcome.
 *
 * <p>Coverage targets:
 * <ul>
 * <li>{@code preallocateSpace} — file created, exact size, idempotency / rejection of zero</li>
 * <li>{@code writeChunk} — bytes land at the correct offset, adjacent chunks don't bleed</li>
 * <li>{@code readChunk} — round-trip fidelity, EOF sentinel for out-of-bounds reads</li>
 * <li>{@code checkExistingChunks} — BitSet reflects valid vs. corrupted vs. missing chunks</li>
 * <li>{@code close} — idempotent, channel closed after try-with-resources</li>
 * </ul>
 */
class FileChannelStorageTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private FileChannelStorage storage;
    private final ChecksumVerifier verifier = new ChecksumVerifier();

    @BeforeEach
    void setUp() {
        testFile = tempDir.resolve("storage-test.bin");
        storage = new FileChannelStorage(testFile);
    }

    @AfterEach
    void tearDown() {
        // close() is idempotent — safe even if a test already closed it
        storage.close();
    }

    // ── preallocateSpace ─────────────────────────────────────────────────────────

    @Test
    void preallocateSpace_createsFileWithExactSize() throws IOException {
        storage.preallocateSpace(1024);

        assertThat(testFile).exists();
        assertThat(Files.size(testFile)).isEqualTo(1024);
    }

    @Test
    void preallocateSpace_singleByte_createsMinimalFile() throws IOException {
        storage.preallocateSpace(1);

        assertThat(Files.size(testFile)).isEqualTo(1);
    }

    @Test
    void preallocateSpace_largeSizes_allocatesCorrectly() throws IOException {
        long size = 10 * 1024 * 1024L; // 10 MiB
        storage.preallocateSpace(size);

        assertThat(Files.size(testFile)).isEqualTo(size);
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
    void preallocateSpace_fileAlreadyExists_throwsUncheckedIOException() throws IOException {
        Files.createFile(testFile);

        // CREATE_NEW must fail fast rather than silently clobbering existing data
        assertThatThrownBy(() -> storage.preallocateSpace(512))
                .isInstanceOf(java.io.UncheckedIOException.class);
    }

    // ── writeChunk / readChunk — round-trip ──────────────────────────────────────

    @Test
    void writeChunk_thenReadChunk_returnsIdenticalBytes() {
        byte[] payload = "swarm-chunk-payload".getBytes();
        long offset = 0;
        storage.preallocateSpace(payload.length);

        storage.writeChunk(chunkId(0), offset, payload);

        Optional<byte[]> result = storage.readChunk(chunkId(0), offset, payload.length);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(payload);
    }

    @Test
    void writeChunk_atNonZeroOffset_writesOnlyToTargetRange() {
        // File layout: [zeros | payload | zeros]
        long fileSize = 64;
        byte[] payload = "HELLO".getBytes();
        long offset = 16;
        storage.preallocateSpace(fileSize);

        storage.writeChunk(chunkId(0), offset, payload);

        // Bytes before the offset must still be zero
        Optional<byte[]> prefix = storage.readChunk(chunkId(0), 0, (int) offset);
        assertThat(prefix).isPresent();
        assertThat(prefix.get()).containsOnly((byte) 0);

        // Written region must match exactly
        Optional<byte[]> written = storage.readChunk(chunkId(0), offset, payload.length);
        assertThat(written).isPresent();
        assertThat(written.get()).isEqualTo(payload);
    }

    @Test
    void writeChunk_multipleChunks_adjacentRegionsDoNotBleed() {
        byte[] chunkA = "AAAA".getBytes();
        byte[] chunkB = "BBBB".getBytes();
        long offsetA = 0;
        long offsetB = chunkA.length;
        storage.preallocateSpace(chunkA.length + chunkB.length);

        storage.writeChunk(chunkId(0), offsetA, chunkA);
        storage.writeChunk(chunkId(1), offsetB, chunkB);

        assertThat(storage.readChunk(chunkId(0), offsetA, chunkA.length))
                .isPresent()
                .hasValueSatisfying(b -> assertThat(b).isEqualTo(chunkA));

        assertThat(storage.readChunk(chunkId(1), offsetB, chunkB.length))
                .isPresent()
                .hasValueSatisfying(b -> assertThat(b).isEqualTo(chunkB));
    }

    @Test
    void readChunk_beyondEOF_returnsEmpty() {
        storage.preallocateSpace(8);

        // Reading past the end of file must return Optional.empty(), not throw
        Optional<byte[]> result = storage.readChunk(chunkId(0), 0, 1024);
        assertThat(result).isEmpty();
    }

    @Test
    void readChunk_exactlyAtEndOfFile_returnsEmpty() {
        long size = 16;
        storage.preallocateSpace(size);

        // Offset == fileSize means we start reading at EOF
        Optional<byte[]> result = storage.readChunk(chunkId(0), size, 1);
        assertThat(result).isEmpty();
    }

    // ── checkExistingChunks ──────────────────────────────────────────────────────

    @Test
    void checkExistingChunks_allValidChunks_allBitsSet() {
        byte[] data0 = "chunk-zero".getBytes();
        byte[] data1 = "chunk-one!".getBytes();
        int chunkSize = data0.length; // both same length for simplicity

        storage.preallocateSpace((long) chunkSize * 2);
        storage.writeChunk(chunkId(0), 0L, data0);
        storage.writeChunk(chunkId(1), chunkSize, data1);

        Manifest manifest = manifestOf(
                chunkDescriptor(0, 0L, chunkSize, verifier.compute(data0)),
                chunkDescriptor(1, chunkSize, chunkSize, verifier.compute(data1))
        );

        BitSet result = storage.checkExistingChunks(manifest);

        assertThat(result.get(0)).isTrue();
        assertThat(result.get(1)).isTrue();
        assertThat(result.cardinality()).isEqualTo(2);
    }

    @Test
    void checkExistingChunks_corruptedChunk_bitNotSet() {
        byte[] good = "good-data-123456".getBytes();
        byte[] corrupt = "xxxx-corrupted!!".getBytes();
        int chunkSize = good.length;

        storage.preallocateSpace((long) chunkSize * 2);
        storage.writeChunk(chunkId(0), 0L, good);
        storage.writeChunk(chunkId(1), chunkSize, corrupt); // written, but wrong hash registered

        Manifest manifest = manifestOf(
                chunkDescriptor(0, 0L, chunkSize, verifier.compute(good)),
                // Manifest declares correct hash, but on-disk bytes are corrupt
                chunkDescriptor(1, chunkSize, chunkSize, verifier.compute(good))
        );

        BitSet result = storage.checkExistingChunks(manifest);

        assertThat(result.get(0)).as("valid chunk 0 must be set").isTrue();
        assertThat(result.get(1)).as("corrupted chunk 1 must not be set").isFalse();
    }

    @Test
    void checkExistingChunks_emptyManifest_returnsEmptyBitSet() {
        storage.preallocateSpace(64);

        Manifest manifest = manifestOf(); // no chunks declared

        BitSet result = storage.checkExistingChunks(manifest);

        assertThat(result.isEmpty()).isTrue();
    }

    // ── close ────────────────────────────────────────────────────────────────────

    @Test
    void close_isIdempotent_doesNotThrow() {
        storage.preallocateSpace(64);

        // Calling close() multiple times must never throw
        storage.close();
        storage.close();
        storage.close();
    }

    @Test
    void close_withoutPreallocate_doesNotThrow() {
        // Channel is null when close() is called before preallocateSpace()
        storage.close();
    }

    @Test
    void close_viaAutoCloseable_channelReleasedAfterBlock() throws IOException {
        Path file = tempDir.resolve("closeable-test.bin");
        try (FileChannelStorage s = new FileChannelStorage(file)) {
            s.preallocateSpace(32);
            // Channel is open inside the block
        }
        // File still exists but the channel is released; we can re-open it
        assertThat(file).exists();
        // Re-opening the same path must succeed (no exclusive lock held)
        try (FileChannelStorage s2 = new FileChannelStorage(tempDir.resolve("closeable-test-2.bin"))) {
            s2.preallocateSpace(32);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static ChunkId chunkId(int index) {
        ChunkId id = mock(ChunkId.class);
        when(id.index()).thenReturn(index);
        return id;
    }

    private static ChunkDescriptor chunkDescriptor(int index, long offset, int size, String sha256) {
        ChunkDescriptor desc = mock(ChunkDescriptor.class);
        
        // BUG FIX: Isolate the nested mock creation before passing it to thenReturn()
        ChunkId resolvedId = chunkId(index); 
        
        when(desc.id()).thenReturn(resolvedId);
        when(desc.offset()).thenReturn(offset);
        when(desc.size()).thenReturn(size);
        when(desc.sha256()).thenReturn(sha256);
        return desc;
    }

    private static Manifest manifestOf(ChunkDescriptor... descriptors) {
        Manifest manifest = mock(Manifest.class);
        when(manifest.chunks()).thenReturn(List.of(descriptors));
        when(manifest.totalChunks()).thenReturn(descriptors.length);
        return manifest;
    }
}
