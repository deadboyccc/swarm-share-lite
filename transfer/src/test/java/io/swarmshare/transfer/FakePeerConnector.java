package io.swarmshare.transfer;

import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.core.port.PeerConnector;

import java.time.Duration;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link PeerConnector} whose peers can be scripted to misbehave.
 *
 * <p>
 * Every peer serves the same chunk bytes; per-peer settings decide whether it
 * advertises a chunk, answers at all, drops connections, or sends corrupt data.
 * Fetches run on virtual threads, like {@code TcpPeerConnector}, so concurrency
 * limits in the orchestrator are observable rather than hidden by a small pool.
 */
final class FakePeerConnector implements PeerConnector {

    private final Map<Integer, byte[]> chunks;
    private final Map<PeerInfo, BitSet> holdings = new ConcurrentHashMap<>();
    private final Set<PeerInfo> unreachable = ConcurrentHashMap.newKeySet();
    private final Set<PeerInfo> droppingConnections = ConcurrentHashMap.newKeySet();
    private final Set<PeerInfo> alwaysCorrupt = ConcurrentHashMap.newKeySet();
    private final Set<Integer> corruptOnce = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> fetchCounts = new ConcurrentHashMap<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private volatile Duration fetchDelay = Duration.ZERO;

    FakePeerConnector(Map<Integer, byte[]> chunks) {
        this.chunks = Map.copyOf(chunks);
    }

    static Map<Integer, byte[]> chunksFromManifest(Manifest manifest, byte[] fileBytes) {
        Map<Integer, byte[]> map = new ConcurrentHashMap<>();
        for (ChunkDescriptor desc : manifest.chunks()) {
            byte[] chunk = new byte[desc.size()];
            System.arraycopy(fileBytes, (int) desc.offset(), chunk, 0, desc.size());
            map.put(desc.id().index(), chunk);
        }
        return map;
    }

    // ── scripting ────────────────────────────────────────────────────────────────

    /** The next fetch of {@code chunkIndex} (from any peer) returns corrupt bytes. */
    FakePeerConnector corruptOnce(int chunkIndex) {
        corruptOnce.add(chunkIndex);
        return this;
    }

    /** {@code peer} always serves corrupt bytes. */
    FakePeerConnector alwaysCorrupt(PeerInfo peer) {
        alwaysCorrupt.add(peer);
        return this;
    }

    /** {@code peer} fails both piece-map and chunk requests, like a host that is down. */
    FakePeerConnector unreachable(PeerInfo peer) {
        unreachable.add(peer);
        return this;
    }

    /** {@code peer} answers piece-map requests but every chunk transfer dies mid-way. */
    FakePeerConnector dropsConnections(PeerInfo peer) {
        droppingConnections.add(peer);
        return this;
    }

    /** {@code peer} advertises (and serves) only {@code indices}; by default it holds everything. */
    FakePeerConnector holds(PeerInfo peer, int... indices) {
        BitSet held = new BitSet();
        for (int index : indices) {
            held.set(index);
        }
        holdings.put(peer, held);
        return this;
    }

    FakePeerConnector withFetchDelay(Duration delay) {
        this.fetchDelay = delay;
        return this;
    }

    // ── observations ─────────────────────────────────────────────────────────────

    int fetchCount(PeerInfo peer, int chunkIndex) {
        AtomicInteger count = fetchCounts.get(key(peer, chunkIndex));
        return count == null ? 0 : count.get();
    }

    int totalFetches() {
        return fetchCounts.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    int maxConcurrentFetches() {
        return maxInFlight.get();
    }

    // ── PeerConnector ────────────────────────────────────────────────────────────

    @Override
    public CompletableFuture<byte[]> fetchChunkAsync(PeerInfo peer, ChunkId id, int size) {
        return CompletableFuture.supplyAsync(() -> {
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            try {
                fetchCounts.computeIfAbsent(key(peer, id.index()), _ -> new AtomicInteger()).incrementAndGet();
                pause();
                if (unreachable.contains(peer)) {
                    throw new IllegalStateException("connection refused: " + peer.displayAddress());
                }
                if (droppingConnections.contains(peer)) {
                    throw new IllegalStateException("connection reset mid-transfer: " + peer.displayAddress());
                }
                byte[] data = chunks.get(id.index());
                if (data == null || !holdingsOf(peer).get(id.index())) {
                    throw new IllegalStateException("NOT_FOUND: chunk " + id.index());
                }
                if (alwaysCorrupt.contains(peer) || corruptOnce.remove(id.index())) {
                    byte[] bad = data.clone();
                    bad[0] ^= (byte) 0xFF;
                    return bad;
                }
                return data.clone();
            } finally {
                inFlight.decrementAndGet();
            }
        }, executor);
    }

    @Override
    public CompletableFuture<BitSet> fetchPieceMapAsync(PeerInfo peer, String manifestHash) {
        return CompletableFuture.supplyAsync(() -> {
            if (unreachable.contains(peer)) {
                throw new IllegalStateException("connection refused: " + peer.displayAddress());
            }
            return (BitSet) holdingsOf(peer).clone();
        }, executor);
    }

    private BitSet holdingsOf(PeerInfo peer) {
        return holdings.computeIfAbsent(peer, _ -> {
            BitSet all = new BitSet();
            chunks.keySet().forEach(all::set);
            return all;
        });
    }

    private void pause() {
        if (!fetchDelay.isZero()) {
            try {
                Thread.sleep(fetchDelay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted", e);
            }
        }
    }

    private static String key(PeerInfo peer, int chunkIndex) {
        return peer.id() + "#" + chunkIndex;
    }
}
