package io.swarmshare.transfer;

import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.*;
import io.swarmshare.core.port.StorageProvider;
import io.swarmshare.manifest.ManifestBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkStateTrackerTest {

    private final ChunkStateTracker tracker = new ChunkStateTracker();
    private final ChunkId id = new ChunkId("abc123", 0);

    @Test
    void initialize_setsMissingState() {
        tracker.initialize(id);
        assertThat(tracker.getState(id)).isEqualTo(ChunkState.MISSING);
        assertThat(tracker.getFailureCount(id)).isZero();
    }

    @Test
    void transition_succeedsWhenExpectedStateMatches() {
        tracker.initialize(id);
        assertThat(tracker.transition(id, ChunkState.MISSING, ChunkState.SCHEDULED)).isTrue();
        assertThat(tracker.getState(id)).isEqualTo(ChunkState.SCHEDULED);
    }

    @Test
    void transition_failsWhenExpectedStateDoesNotMatch() {
        tracker.initialize(id);
        assertThat(tracker.transition(id, ChunkState.SCHEDULED, ChunkState.IN_FLIGHT)).isFalse();
        assertThat(tracker.getState(id)).isEqualTo(ChunkState.MISSING);
    }

    @Test
    void incrementFailure_isAtomic() {
        tracker.initialize(id);
        assertThat(tracker.incrementFailure(id)).isEqualTo(1);
        assertThat(tracker.incrementFailure(id)).isEqualTo(2);
        assertThat(tracker.getFailureCount(id)).isEqualTo(2);
    }

    @Test
    void reset_overwritesCurrentState() {
        tracker.initialize(id);
        tracker.transition(id, ChunkState.MISSING, ChunkState.VERIFYING);
        tracker.reset(id, ChunkState.SCHEDULED);
        assertThat(tracker.getState(id)).isEqualTo(ChunkState.SCHEDULED);
    }
}

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy(5, java.time.Duration.ofMillis(100),
            java.time.Duration.ofMillis(800));

    @Test
    void delayFor_doublesEachAttempt_cappedAtMax() {
        assertThat(policy.delayFor(0).toMillis()).isEqualTo(100);
        assertThat(policy.delayFor(1).toMillis()).isEqualTo(200);
        assertThat(policy.delayFor(2).toMillis()).isEqualTo(400);
        assertThat(policy.delayFor(3).toMillis()).isEqualTo(800);
        assertThat(policy.delayFor(10).toMillis()).isEqualTo(800);
    }

    @Test
    void delayFor_largeAttempt_doesNotOverflowAndStaysCapped() {
        assertThat(policy.delayFor(63).toMillis()).isEqualTo(800);
        assertThat(policy.delayFor(100).toMillis()).isEqualTo(800);
    }

    @Test
    void shouldRetry_respectsMaxAttempts() {
        assertThat(policy.shouldRetry(0)).isTrue();
        assertThat(policy.shouldRetry(4)).isTrue();
        assertThat(policy.shouldRetry(5)).isFalse();
    }
}

class TransferManagerTest {

    private static final RetryPolicy NO_WAIT =
            new RetryPolicy(3, Duration.ZERO, Duration.ZERO);

    @TempDir
    Path tempDir;

    private static Manifest buildManifest(byte[] fileBytes, int chunkSize) throws Exception {
        Path file = Files.createTempFile("transfer-test-", ".bin");
        Files.write(file, fileBytes);
        return new ManifestBuilder(chunkSize).build(file);
    }

    private static TransferManager manager(Manifest manifest, StorageProvider storage,
                                            FakePeerConnector connector, PeerInfo peer) {
        return new TransferManager(manifest, List.of(peer), storage, connector, new Sha256(), NO_WAIT);
    }

    private static PeerInfo testPeer() {
        return new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", 9999));
    }

    @Test
    void start_downloadsAllChunksFromPeer() throws Exception {
        byte[] fileBytes = "hello-swarm-share-lite-test-data".getBytes();
        Manifest manifest = buildManifest(fileBytes, 10);

        InMemoryStorage storage = new InMemoryStorage();
        FakePeerConnector connector = new FakePeerConnector(
                manifest, FakePeerConnector.chunksFromManifest(manifest, fileBytes));
        PeerInfo peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", 9999));

        var manager = manager(manifest, storage, connector, peer);
        manager.start();

        BitSet held = storage.checkExistingChunks(manifest);
        assertThat(held.cardinality()).isEqualTo(manifest.totalChunks());
    }

    @Test
    void start_retries_onChecksumMismatch() throws Exception {
        byte[] fileBytes = "retry-checksum-mismatch-test".getBytes();
        Manifest manifest = buildManifest(fileBytes, 8);

        InMemoryStorage storage = new InMemoryStorage();
        FakePeerConnector connector = new FakePeerConnector(
                manifest, FakePeerConnector.chunksFromManifest(manifest, fileBytes))
                .corruptOnce(0);
        PeerInfo peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", 9999));

        var manager = manager(manifest, storage, connector, peer);
        manager.start();

        assertThat(storage.checkExistingChunks(manifest).cardinality())
                .isEqualTo(manifest.totalChunks());
        assertThat(manager.heldChunks().get(0)).isTrue();
    }

    @Test
    void start_skipsAlreadyHeldChunks() throws Exception {
        byte[] fileBytes = "resume-existing-chunks".getBytes();
        Manifest manifest = buildManifest(fileBytes, 12);

        InMemoryStorage storage = new InMemoryStorage();
        for (ChunkDescriptor desc : manifest.chunks()) {
            byte[] chunk = new byte[desc.size()];
            System.arraycopy(fileBytes, (int) desc.offset(), chunk, 0, desc.size());
            storage.writeChunk(desc.id(), desc.offset(), chunk);
        }

        FakePeerConnector connector = new FakePeerConnector(
                manifest, FakePeerConnector.chunksFromManifest(manifest, fileBytes));
        PeerInfo peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", 9999));

        var manager = manager(manifest, storage, connector, peer);
        manager.start();

        assertThat(manager.heldChunks().cardinality()).isEqualTo(manifest.totalChunks());
    }

    @Test
    void start_retries_whenWriteFailsAfterVerify() throws Exception {
        byte[] fileBytes = "retry-after-write-failure".getBytes();
        Manifest manifest = buildManifest(fileBytes, 12);

        AtomicInteger writes = new AtomicInteger();
        InMemoryStorage delegate = new InMemoryStorage();
        StorageProvider storage = new StorageProvider() {
            @Override
            public void preallocateSpace(long totalSize) {
                delegate.preallocateSpace(totalSize);
            }

            @Override
            public void writeChunk(ChunkId id, long offset, byte[] data) {
                if (writes.getAndIncrement() == 0) {
                    throw new UncheckedIOException("simulated disk full", new IOException("disk full"));
                }
                delegate.writeChunk(id, offset, data);
            }

            @Override
            public java.util.Optional<byte[]> readChunk(ChunkId id, long offset, int size) {
                return delegate.readChunk(id, offset, size);
            }

            @Override
            public BitSet checkExistingChunks(Manifest manifest) {
                return delegate.checkExistingChunks(manifest);
            }
        };
        FakePeerConnector connector = new FakePeerConnector(
                manifest, FakePeerConnector.chunksFromManifest(manifest, fileBytes));
        PeerInfo peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", 9999));

        var manager = manager(manifest, storage, connector, peer);
        manager.start();

        assertThat(storage.checkExistingChunks(manifest).cardinality())
                .isEqualTo(manifest.totalChunks());
    }

    @Test
    void start_rejectsInconsistentManifest() {
        Manifest empty = new Manifest("a".repeat(64), "empty.bin", 100, 100, List.of());
        InMemoryStorage storage = new InMemoryStorage();
        FakePeerConnector connector = new FakePeerConnector(empty, java.util.Map.of());

        assertThatThrownBy(() -> manager(empty, storage, connector, testPeer()).start())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid manifest");
    }

    @Test
    void start_rejectsWhenAssembledFileHashMismatches() throws Exception {
        byte[] fileBytes = "file-hash-mismatch".getBytes();
        Manifest manifest = buildManifest(fileBytes, 8);

        InMemoryStorage delegate = new InMemoryStorage();
        StorageProvider storage = new StorageProvider() {
            @Override
            public void preallocateSpace(long totalSize) {
                delegate.preallocateSpace(totalSize);
            }

            @Override
            public void writeChunk(ChunkId id, long offset, byte[] data) {
                byte[] corrupted = data.clone();
                corrupted[0] ^= 0x01;
                delegate.writeChunk(id, offset, corrupted);
            }

            @Override
            public java.util.Optional<byte[]> readChunk(ChunkId id, long offset, int size) {
                return delegate.readChunk(id, offset, size);
            }

            @Override
            public BitSet checkExistingChunks(Manifest manifest) {
                return new BitSet();
            }
        };
        FakePeerConnector connector = new FakePeerConnector(
                manifest, FakePeerConnector.chunksFromManifest(manifest, fileBytes));

        assertThatThrownBy(() -> manager(manifest, storage, connector, testPeer()).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("file hash mismatch");
    }
}
