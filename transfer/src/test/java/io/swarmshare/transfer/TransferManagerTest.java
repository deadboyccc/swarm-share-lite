package io.swarmshare.transfer;

import io.swarmshare.core.domain.*;
import io.swarmshare.manifest.ManifestBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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
    void shouldRetry_respectsMaxAttempts() {
        assertThat(policy.shouldRetry(0)).isTrue();
        assertThat(policy.shouldRetry(4)).isTrue();
        assertThat(policy.shouldRetry(5)).isFalse();
    }
}

class TransferManagerTest {

    @TempDir
    Path tempDir;

    private static Manifest buildManifest(byte[] fileBytes, int chunkSize) throws Exception {
        Path file = Files.createTempFile("transfer-test-", ".bin");
        Files.write(file, fileBytes);
        return new ManifestBuilder(chunkSize).build(file);
    }

    @Test
    void start_downloadsAllChunksFromPeer() throws Exception {
        byte[] fileBytes = "hello-swarm-share-lite-test-data".getBytes();
        Manifest manifest = buildManifest(fileBytes, 10);

        InMemoryStorage storage = new InMemoryStorage();
        FakePeerConnector connector = new FakePeerConnector(
                manifest, FakePeerConnector.chunksFromManifest(manifest, fileBytes));
        PeerInfo peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", 9999));

        var manager = new TransferManager(manifest, List.of(peer), storage, connector);
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

        var manager = new TransferManager(manifest, List.of(peer), storage, connector);
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

        var manager = new TransferManager(manifest, List.of(peer), storage, connector);
        manager.start();

        assertThat(manager.heldChunks().cardinality()).isEqualTo(manifest.totalChunks());
    }
}
