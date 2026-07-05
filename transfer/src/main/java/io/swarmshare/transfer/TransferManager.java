package io.swarmshare.transfer;

import io.swarmshare.core.crypto.HasherPort;
import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.*;
import io.swarmshare.core.port.PeerConnector;
import io.swarmshare.core.port.StorageProvider;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Central orchestrator for the swarm download pipeline:
 * resume → collect piece maps → parallel download → verify → persist.
 */
public final class TransferManager {

    private static final Logger LOG = System.getLogger(TransferManager.class.getName());

    private static final int MAX_INFLIGHT = 32;
    private static final int MAX_RETRIES = 3;

    private final Manifest manifest;
    private final List<PeerInfo> peers;
    private final StorageProvider storage;
    private final PeerConnector connector;
    private final HasherPort verifier;
    private final ChunkStateTracker stateTracker;
    private final Semaphore inflightLimit = new Semaphore(MAX_INFLIGHT);
    private final BitSet heldChunks;

    public TransferManager(Manifest manifest, List<PeerInfo> peers,
                           StorageProvider storage, PeerConnector connector) {
        this(manifest, peers, storage, connector, new Sha256());
    }

    TransferManager(Manifest manifest, List<PeerInfo> peers,
                    StorageProvider storage, PeerConnector connector,
                    HasherPort verifier) {
        this.manifest = manifest;
        this.peers = List.copyOf(peers);
        this.storage = storage;
        this.connector = connector;
        this.verifier = verifier;
        this.stateTracker = new ChunkStateTracker();
        this.heldChunks = new BitSet(manifest.totalChunks());
    }

    public void start() {
        storage.preallocateSpace(manifest.totalSize());

        BitSet existing = storage.checkExistingChunks(manifest);
        existing.stream().forEach(i -> {
            heldChunks.set(i);
            ChunkId id = manifest.chunkAt(i).id();
            stateTracker.initialize(id);
            stateTracker.transition(id, ChunkState.MISSING, ChunkState.WRITTEN);
        });

        Map<PeerInfo, BitSet> peerPieceMaps = collectPieceMaps();

        List<ChunkDescriptor> missing = manifest.chunks().stream()
                .filter(desc -> !heldChunks.get(desc.id().index()))
                .toList();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (ChunkDescriptor chunk : missing) {
                stateTracker.initialize(chunk.id());
                PeerInfo source = selectPeer(chunk.id().index(), peerPieceMaps);
                if (source == null) {
                    LOG.log(Level.WARNING, "No peer has chunk {0}", chunk.id().index());
                    continue;
                }
                stateTracker.transition(chunk.id(), ChunkState.MISSING, ChunkState.SCHEDULED);
                executor.submit(() -> downloadWithRetry(chunk, source, peerPieceMaps));
            }
        }

        verifyCompleteFile();
    }

    private void downloadWithRetry(ChunkDescriptor desc, PeerInfo initial,
                                   Map<PeerInfo, BitSet> pieceMaps) {
        int attempts = 0;
        PeerInfo source = initial;

        while (attempts < MAX_RETRIES) {
            inflightLimit.acquireUninterruptibly();
            try {
                stateTracker.transition(desc.id(), ChunkState.SCHEDULED, ChunkState.IN_FLIGHT);
                byte[] data = connector.fetchChunkAsync(source, desc.id(), desc.size()).join();

                stateTracker.transition(desc.id(), ChunkState.IN_FLIGHT, ChunkState.VERIFYING);

                if (verifier.verify(data, desc.sha256())) {
                    storage.writeChunk(desc.id(), desc.offset(), data);
                    stateTracker.transition(desc.id(), ChunkState.VERIFYING, ChunkState.WRITTEN);
                    synchronized (heldChunks) {
                        heldChunks.set(desc.id().index());
                    }
                    return;
                }

                stateTracker.transition(desc.id(), ChunkState.VERIFYING, ChunkState.MISSING);
                stateTracker.incrementFailure(desc.id());
                source = nextPeerAfterFailure(desc.id().index(), source, pieceMaps);
                if (source == null) break;
                stateTracker.transition(desc.id(), ChunkState.MISSING, ChunkState.SCHEDULED);

            } catch (Exception e) {
                LOG.log(Level.DEBUG, "Chunk {0} attempt {1} failed: {2}",
                        desc.id().index(), attempts, e.getMessage());
                stateTracker.transition(desc.id(), ChunkState.IN_FLIGHT, ChunkState.MISSING);
                stateTracker.incrementFailure(desc.id());
                source = nextPeerAfterFailure(desc.id().index(), source, pieceMaps);
                if (source == null) break;
                stateTracker.transition(desc.id(), ChunkState.MISSING, ChunkState.SCHEDULED);
            } finally {
                inflightLimit.release();
            }
            attempts++;
        }

        LOG.log(Level.ERROR, "FAILED: chunk {0} after {1} attempts", desc.id().index(), attempts);
    }

    private Map<PeerInfo, BitSet> collectPieceMaps() {
        Map<PeerInfo, BitSet> result = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = peers.stream()
                .map(peer -> connector.fetchPieceMapAsync(peer, manifest.fileHash())
                        .thenAccept(bitset -> result.put(peer, bitset))
                        .exceptionally(_ -> null))
                .toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return result;
    }

    private PeerInfo selectPeer(int chunkIndex, Map<PeerInfo, BitSet> pieceMaps) {
        return pieceMaps.entrySet().stream()
                .filter(e -> e.getValue().get(chunkIndex))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    private PeerInfo selectAlternativePeer(int chunkIndex, PeerInfo exclude,
                                           Map<PeerInfo, BitSet> pieceMaps) {
        return pieceMaps.entrySet().stream()
                .filter(e -> !e.getKey().equals(exclude))
                .filter(e -> e.getValue().get(chunkIndex))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    /**
     * Chooses where to source a retry after a failed attempt. Prefers a peer
     * other than the one that just failed; if none exists but the same peer
     * still advertises the chunk, retries against it rather than giving up —
     * a checksum mismatch or I/O error may be transient rather than a sign
     * the peer's copy is bad.
     */
    private PeerInfo nextPeerAfterFailure(int chunkIndex, PeerInfo current,
                                          Map<PeerInfo, BitSet> pieceMaps) {
        PeerInfo alternative = selectAlternativePeer(chunkIndex, current, pieceMaps);
        if (alternative != null) {
            return alternative;
        }
        BitSet currentMap = pieceMaps.get(current);
        return (currentMap != null && currentMap.get(chunkIndex)) ? current : null;
    }

    private void verifyCompleteFile() {
        long missing = manifest.chunks().stream()
                .filter(desc -> !heldChunks.get(desc.id().index()))
                .count();
        if (missing > 0) {
            throw new IllegalStateException(
                    "Transfer incomplete: " + missing + " of " + manifest.totalChunks() + " chunks missing");
        }
        LOG.log(Level.INFO, "Transfer complete. All {0} chunks verified.", manifest.totalChunks());
    }

    /**
     * Exposed for tests.
     */
    BitSet heldChunks() {
        synchronized (heldChunks) {
            return (BitSet) heldChunks.clone();
        }
    }
}