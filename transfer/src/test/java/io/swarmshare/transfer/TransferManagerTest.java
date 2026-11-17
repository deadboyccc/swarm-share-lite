package io.swarmshare.transfer;

import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.core.port.StorageProvider;
import io.swarmshare.manifest.ManifestBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Orchestration tests with no disk and no network: {@link InMemoryStorage} and a
 * scriptable {@link FakePeerConnector} stand in for the adapters.
 */
@Timeout(30)
class TransferManagerTest {

    private static final RetryPolicy NO_WAIT = new RetryPolicy(3, Duration.ZERO, Duration.ZERO);

    @TempDir
    Path tempDir;

    // ── happy path ───────────────────────────────────────────────────────────────

    @Test
    void start_downloadsAllChunksFromPeer() throws Exception {
        Fixture f = fixture(32, 10);
        PeerInfo peer = peer();

        manager(f, List.of(peer), f.storage(), f.connector()).start();

        assertStorageHoldsFile(f);
    }

    @Test
    void start_healthySwarm_fetchesEveryChunkExactlyOnce() throws Exception {
        Fixture f = fixture(1_000, 10);

        manager(f, List.of(peer()), f.storage(), f.connector()).start();

        assertThat(f.connector().totalFetches()).isEqualTo(f.manifest().totalChunks());
    }

    @Test
    void start_chunksSpreadAcrossPeers_eachFetchedFromAHolder() throws Exception {
        Fixture f = fixture(40, 10); // chunks 0..3
        PeerInfo evens = peer();
        PeerInfo odds = peer();
        f.connector().holds(evens, 0, 2).holds(odds, 1, 3);

        manager(f, List.of(evens, odds), f.storage(), f.connector()).start();

        assertStorageHoldsFile(f);
        assertThat(f.connector().fetchCount(evens, 1)).isZero();
        assertThat(f.connector().fetchCount(odds, 0)).isZero();
    }

    // ── concurrency & backpressure (roadmap phase 4) ─────────────────────────────

    @Test
    void start_neverExceedsInflightLimit_butDoesRunInParallel() throws Exception {
        Fixture f = fixture(160 * 8, 8); // 160 chunks
        f.connector().withFetchDelay(Duration.ofMillis(15));

        manager(f, List.of(peer()), f.storage(), f.connector()).start();

        assertThat(f.connector().maxConcurrentFetches())
                .isLessThanOrEqualTo(TransferManager.MAX_INFLIGHT)
                .isGreaterThan(1);
        assertStorageHoldsFile(f);
    }

    // ── resume ───────────────────────────────────────────────────────────────────

    @Test
    void start_allChunksAlreadyOnDisk_fetchesNothingAndNeedsNoPeer() throws Exception {
        Fixture f = fixture(36, 12);
        writeChunks(f, 0, 1, 2);
        PeerInfo down = peer();
        f.connector().unreachable(down);

        TransferManager manager = manager(f, List.of(down), f.storage(), f.connector());
        manager.start();

        assertThat(f.connector().totalFetches()).isZero();
        assertThat(manager.heldChunks().cardinality()).isEqualTo(3);
    }

    @Test
    void start_partialDownload_fetchesOnlyMissingChunks() throws Exception {
        Fixture f = fixture(40, 10);
        writeChunks(f, 0, 2);
        PeerInfo peer = peer();

        manager(f, List.of(peer), f.storage(), f.connector()).start();

        assertStorageHoldsFile(f);
        assertThat(f.connector().fetchCount(peer, 0)).isZero();
        assertThat(f.connector().fetchCount(peer, 1)).isEqualTo(1);
        assertThat(f.connector().fetchCount(peer, 2)).isZero();
        assertThat(f.connector().fetchCount(peer, 3)).isEqualTo(1);
    }

    @Test
    void start_corruptChunkOnDisk_isRedownloaded() throws Exception {
        Fixture f = fixture(20, 10);
        ChunkDescriptor first = f.manifest().chunkAt(0);
        f.storage().writeChunk(first.id(), first.offset(), new byte[first.size()]); // zeros, wrong hash
        PeerInfo peer = peer();

        manager(f, List.of(peer), f.storage(), f.connector()).start();

        assertStorageHoldsFile(f);
        assertThat(f.connector().fetchCount(peer, 0)).isEqualTo(1);
    }

    // ── failures & retries ───────────────────────────────────────────────────────

    @Test
    void start_retries_onChecksumMismatch() throws Exception {
        Fixture f = fixture(28, 8);
        PeerInfo peer = peer();
        f.connector().corruptOnce(0);

        TransferManager manager = manager(f, List.of(peer), f.storage(), f.connector());
        manager.start();

        assertStorageHoldsFile(f);
        assertThat(f.connector().fetchCount(peer, 0)).isEqualTo(2);
        assertThat(manager.heldChunks().get(0)).isTrue();
    }

    @Test
    void start_unreachablePeerIsIgnored_whenAnotherPeerHasTheData() throws Exception {
        Fixture f = fixture(30, 10);
        PeerInfo down = peer();
        PeerInfo up = peer();
        f.connector().unreachable(down);

        manager(f, List.of(down, up), f.storage(), f.connector()).start();

        assertStorageHoldsFile(f);
        assertThat(f.connector().fetchCount(down, 0)).isZero();
    }

    @Test
    void start_peerDroppingConnections_fallsBackToAlternatePeer() throws Exception {
        Fixture f = fixture(30, 10);
        PeerInfo flaky = peer();
        PeerInfo solid = peer();
        f.connector().dropsConnections(flaky).holds(flaky, 0, 1, 2).holds(solid, 0, 1, 2);

        manager(f, List.of(flaky, solid), f.storage(), f.connector()).start();

        assertStorageHoldsFile(f);
        for (int chunk = 0; chunk < 3; chunk++) {
            assertThat(f.connector().fetchCount(flaky, chunk)).as("first choice tried").isEqualTo(1);
            assertThat(f.connector().fetchCount(solid, chunk)).as("fallback used").isEqualTo(1);
        }
    }

    @Test
    void start_peerSelection_followsConfiguredOrder() throws Exception {
        Fixture f = fixture(30, 10);
        PeerInfo preferred = peer();
        PeerInfo backup = peer();

        manager(f, List.of(preferred, backup), f.storage(), f.connector()).start();

        assertThat(f.connector().fetchCount(backup, 0) + f.connector().fetchCount(backup, 1)
                + f.connector().fetchCount(backup, 2)).isZero();
    }

    @Test
    void start_retriesExhausted_failsWithIncompleteTransfer() throws Exception {
        Fixture f = fixture(20, 10);
        PeerInfo flaky = peer();
        f.connector().dropsConnections(flaky);

        assertThatThrownBy(() -> manager(f, List.of(flaky), f.storage(), f.connector()).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Transfer incomplete: 2 of 2 chunks missing");
        assertThat(f.connector().fetchCount(flaky, 0)).as("max attempts per chunk").isEqualTo(3);
        assertThat(f.connector().fetchCount(flaky, 1)).isEqualTo(3);
    }

    @Test
    void start_backsOffExponentiallyBetweenAttempts() throws Exception {
        Fixture f = fixture(10, 10); // one chunk
        PeerInfo flaky = peer();
        f.connector().dropsConnections(flaky);
        RetryPolicy backoff = new RetryPolicy(3, Duration.ofMillis(60), Duration.ofSeconds(1));
        long started = System.nanoTime();

        assertThatThrownBy(() -> new TransferManager(f.manifest(), List.of(flaky), f.storage(), f.connector(),
                new Sha256(), backoff).start()).isInstanceOf(IllegalStateException.class);

        // 3 attempts → 2 waits: 60 ms, then 120 ms
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isGreaterThanOrEqualTo(Duration.ofMillis(180));
        assertThat(f.connector().fetchCount(flaky, 0)).isEqualTo(3);
    }

    // ── corrupt-peer detection (roadmap phase 6) ─────────────────────────────────

    @Test
    void start_corruptPeerIsBannedAfterThreeChecksumFailures_honestPeerServesTheRest() throws Exception {
        Fixture f = fixture(100, 10); // 10 chunks
        PeerInfo liar = peer();
        PeerInfo honest = peer();
        f.connector().alwaysCorrupt(liar);
        PeerBanList bans = new PeerBanList();

        sequential(f, List.of(liar, honest), bans).start();

        assertStorageHoldsFile(f);
        assertThat(bans.isBanned(liar)).isTrue();
        assertThat(bans.isBanned(honest)).isFalse();
        assertThat(f.connector().fetchesFrom(liar)).as("no requests after the ban").isEqualTo(3);
    }

    @Test
    void start_soleCorruptPeer_isBannedAndTransferFailsFastNamingIt() throws Exception {
        Fixture f = fixture(100, 10);
        PeerInfo liar = peer();
        f.connector().alwaysCorrupt(liar);
        PeerBanList bans = new PeerBanList();

        assertThatThrownBy(() -> sequential(f, List.of(liar), bans).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Transfer incomplete: 10 of 10 chunks missing")
                .hasMessageContaining("banned for corrupt data: " + liar.displayAddress());
        assertThat(f.connector().fetchesFrom(liar)).as("3 strikes, not 3 per chunk").isEqualTo(3);
    }

    @Test
    void start_connectionFailures_neverBanAPeer() throws Exception {
        Fixture f = fixture(50, 10);
        PeerInfo flaky = peer();
        PeerInfo solid = peer();
        f.connector().dropsConnections(flaky);
        PeerBanList bans = new PeerBanList();

        sequential(f, List.of(flaky, solid), bans).start();

        assertStorageHoldsFile(f);
        assertThat(f.connector().fetchesFrom(flaky)).isEqualTo(5);
        assertThat(bans.banned()).isEmpty();
    }

    @Test
    void start_occasionalCorruption_belowThreshold_keepsThePeer() throws Exception {
        Fixture f = fixture(50, 10);
        PeerInfo peer = peer();
        f.connector().corruptOnce(1).corruptOnce(3);
        PeerBanList bans = new PeerBanList();

        sequential(f, List.of(peer), bans).start();

        assertStorageHoldsFile(f);
        assertThat(bans.checksumFailures(peer)).isEqualTo(2);
        assertThat(bans.isBanned(peer)).isFalse();
    }

    @Test
    void constructor_rejectsNonPositiveInflightLimit() throws Exception {
        Fixture f = fixture(10, 10);

        assertThatThrownBy(() -> new TransferManager(f.manifest(), List.of(peer()), f.storage(), f.connector(),
                new Sha256(), NO_WAIT, 0, new PeerBanList()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void start_noReachablePeers_failsWithIncompleteTransfer() throws Exception {
        Fixture f = fixture(30, 10);
        PeerInfo down = peer();
        f.connector().unreachable(down);

        assertThatThrownBy(() -> manager(f, List.of(down), f.storage(), f.connector()).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Transfer incomplete: 3 of 3 chunks missing");
    }

    @Test
    void start_chunkAdvertisedByNoPeer_failsAfterDownloadingTheRest() throws Exception {
        Fixture f = fixture(30, 10);
        PeerInfo partial = peer();
        f.connector().holds(partial, 0, 1);

        assertThatThrownBy(() -> manager(f, List.of(partial), f.storage(), f.connector()).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1 of 3 chunks missing");
        assertThat(f.storage().readChunk(f.manifest().chunkAt(0).id(), 0, 10)).isPresent();
    }

    @Test
    void start_retries_whenWriteFailsAfterVerify() throws Exception {
        Fixture f = fixture(25, 12);
        AtomicInteger writes = new AtomicInteger();
        StorageProvider failingOnce = new DelegatingStorage(f.storage()) {
            @Override
            public void writeChunk(ChunkId id, long offset, byte[] data) {
                if (writes.getAndIncrement() == 0) {
                    throw new UncheckedIOException("simulated disk full", new IOException("disk full"));
                }
                super.writeChunk(id, offset, data);
            }
        };

        manager(f, List.of(peer()), failingOnce, f.connector()).start();

        assertStorageHoldsFile(f);
    }

    @Test
    void start_rejectsInconsistentManifest() {
        Manifest empty = new Manifest("a".repeat(64), "empty.bin", 100, 100, List.of());

        assertThatThrownBy(() -> new TransferManager(empty, List.of(peer()), new InMemoryStorage(),
                new FakePeerConnector(Map.of()), new Sha256(), NO_WAIT).start())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid manifest");
    }

    @Test
    void start_rejectsWhenAssembledFileHashMismatches() throws Exception {
        Fixture f = fixture(18, 8);
        // Storage that flips a bit on write but reads back without re-verifying:
        // only the whole-file hash can catch this.
        StorageProvider bitFlipping = new DelegatingStorage(f.storage()) {
            @Override
            public void writeChunk(ChunkId id, long offset, byte[] data) {
                byte[] corrupted = data.clone();
                corrupted[0] ^= 0x01;
                super.writeChunk(id, offset, corrupted);
            }
        };

        assertThatThrownBy(() -> manager(f, List.of(peer()), bitFlipping, f.connector()).start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("file hash mismatch");
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private record Fixture(byte[] fileBytes, Manifest manifest, InMemoryStorage storage,
                           FakePeerConnector connector) {
    }

    private Fixture fixture(int fileSize, int chunkSize) throws IOException {
        byte[] bytes = new byte[fileSize];
        new Random(fileSize).nextBytes(bytes);
        Path file = Files.write(tempDir.resolve("source-" + fileSize + ".bin"), bytes);
        Manifest manifest = new ManifestBuilder(chunkSize).build(file);
        return new Fixture(bytes, manifest, new InMemoryStorage(),
                new FakePeerConnector(FakePeerConnector.chunksFromManifest(manifest, bytes)));
    }

    private static TransferManager manager(Fixture f, List<PeerInfo> peers, StorageProvider storage,
                                           FakePeerConnector connector) {
        return new TransferManager(f.manifest(), peers, storage, connector, new Sha256(), NO_WAIT);
    }

    /** One download at a time, so the order of failures and bans is deterministic. */
    private static TransferManager sequential(Fixture f, List<PeerInfo> peers, PeerBanList bans) {
        return new TransferManager(f.manifest(), peers, f.storage(), f.connector(), new Sha256(), NO_WAIT, 1, bans);
    }

    private static PeerInfo peer() {
        return new PeerInfo(UUID.randomUUID(), InetSocketAddress.createUnresolved("peer", 7070));
    }

    private static void writeChunks(Fixture f, int... indices) {
        for (int index : indices) {
            ChunkDescriptor desc = f.manifest().chunkAt(index);
            byte[] chunk = new byte[desc.size()];
            System.arraycopy(f.fileBytes(), (int) desc.offset(), chunk, 0, desc.size());
            f.storage().writeChunk(desc.id(), desc.offset(), chunk);
        }
    }

    private static void assertStorageHoldsFile(Fixture f) {
        for (ChunkDescriptor desc : f.manifest().chunks()) {
            byte[] expected = new byte[desc.size()];
            System.arraycopy(f.fileBytes(), (int) desc.offset(), expected, 0, desc.size());
            assertThat(f.storage().readChunk(desc.id(), desc.offset(), desc.size()))
                    .as("chunk %d", desc.id().index())
                    .contains(expected);
        }
    }

    /** Forwards everything to an {@link InMemoryStorage}; tests override single methods. */
    private static class DelegatingStorage implements StorageProvider {
        private final InMemoryStorage delegate;

        DelegatingStorage(InMemoryStorage delegate) {
            this.delegate = delegate;
        }

        @Override
        public void preallocateSpace(long totalSize) {
            delegate.preallocateSpace(totalSize);
        }

        @Override
        public void writeChunk(ChunkId id, long offset, byte[] data) {
            delegate.writeChunk(id, offset, data);
        }

        @Override
        public Optional<byte[]> readChunk(ChunkId id, long offset, int size) {
            return delegate.readChunk(id, offset, size);
        }

        @Override
        public BitSet checkExistingChunks(Manifest manifest) {
            return delegate.checkExistingChunks(manifest);
        }
    }
}
