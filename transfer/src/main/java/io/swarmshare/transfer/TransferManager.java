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

    /**
     * Maximum number of chunk downloads allowed in flight at once, across all peers.
     */
    private static final int MAX_INFLIGHT = 32;
    /**
     * Maximum download attempts per chunk before giving up on it entirely.
     */
    private static final int MAX_RETRIES = 3;

    private final Manifest manifest;
    private final List<PeerInfo> peers;
    private final StorageProvider storage;
    private final PeerConnector connector;
    private final HasherPort verifier;
    private final ChunkStateTracker stateTracker;
    /**
     * Bounds concurrent downloads to {@link #MAX_INFLIGHT} to avoid saturating network buffers.
     */
    private final Semaphore inflightLimit = new Semaphore(MAX_INFLIGHT);
    /**
     * Bitmap of chunk indices already verified and written to disk.
     */
    private final BitSet heldChunks;

    /**
     * Creates a manager using the default SHA-256 verifier.
     */
    public TransferManager(Manifest manifest, List<PeerInfo> peers,
                           StorageProvider storage, PeerConnector connector) {
        this(manifest, peers, storage, connector, new Sha256());
    }

    /**
     * Package-private constructor allowing tests to inject a fake {@link HasherPort}
     * instead of real SHA-256 verification.
     */
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

    /**
     * Runs the full transfer pipeline synchronously: pre-allocates disk space,
     * resumes any chunks already present and valid on disk, fetches peer piece
     * maps, then downloads every remaining chunk in parallel (one virtual
     * thread per chunk) before verifying the file is complete.
     *
     * @throws IllegalStateException if any chunk is still missing once all
     *                               downloads have finished or been abandoned
     */
    public void start() {
        storage.preallocateSpace(manifest.totalSize());

        // Resume support: chunks already correct on disk are marked WRITTEN
        // immediately so they're skipped during download, without re-fetching them.
        BitSet existing = storage.checkExistingChunks(manifest);
        existing.stream().forEach(i -> {
            heldChunks.set(i);
            ChunkId id = manifest.chunkAt(i).id();
            stateTracker.initialize(id);
            stateTracker.transition(id, ChunkState.MISSING, ChunkState.WRITTEN);
        });

        Map<PeerInfo, BitSet> peerPieceMaps = collectPieceMaps();

        // Determine which chunks are still missing on disk and need download
        List<ChunkDescriptor> missing = manifest.chunks().stream()
                .filter(desc -> !heldChunks.get(desc.id().index()))
                .toList();

        // try-with-resources on the executor blocks here until every submitted
        // download task completes (or is interrupted), since AutoCloseable
        // executors await task completion on close().
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (ChunkDescriptor chunk : missing) {
                // Prepare per-chunk state and select a peer that advertises it
                stateTracker.initialize(chunk.id());
                PeerInfo source = selectPeer(chunk.id().index(), peerPieceMaps);
                if (source == null) {
                    LOG.log(Level.WARNING, "No peer has chunk {0}", chunk.id().index());
                    continue;
                }
                stateTracker.transition(chunk.id(), ChunkState.MISSING, ChunkState.SCHEDULED);
                // Download on its own virtual thread
                executor.submit(() -> downloadWithRetry(chunk, source, peerPieceMaps));
            }
        }

        verifyCompleteFile();
    }

    /**
     * Downloads a single chunk, retrying up to {@link #MAX_RETRIES} times on
     * either a checksum mismatch or a network/IO exception. Each retry may
     * switch to a different peer via {@link #nextPeerAfterFailure}. Runs the
     * full attempt loop on whichever virtual thread called it, acquiring
     * {@link #inflightLimit} for the duration of each network round-trip.
     */
    private void downloadWithRetry(ChunkDescriptor desc, PeerInfo initial,
                                   Map<PeerInfo, BitSet> pieceMaps) {
        int attempts = 0;
        PeerInfo source = initial;

        while (attempts < MAX_RETRIES) {
            // Block (parking the virtual thread cheaply) until a download slot frees up.
            inflightLimit.acquireUninterruptibly();
            try {
                stateTracker.transition(desc.id(), ChunkState.SCHEDULED, ChunkState.IN_FLIGHT);
                byte[] data = connector.fetchChunkAsync(source, desc.id(), desc.size()).join();

                stateTracker.transition(desc.id(), ChunkState.IN_FLIGHT, ChunkState.VERIFYING);

                if (verifier.verify(data, desc.sha256())) {
                    storage.writeChunk(desc.id(), desc.offset(), data);
                    stateTracker.transition(desc.id(), ChunkState.VERIFYING, ChunkState.WRITTEN);
                    // heldChunks is shared across all downloader threads; synchronize
                    // the mutation so verifyCompleteFile() and heldChunks() see a
                    // consistent snapshot.
                    synchronized (heldChunks) {
                        heldChunks.set(desc.id().index());
                    }
                    return;
                }

                // Verification failed: mark missing and pick an alternative peer
                stateTracker.transition(desc.id(), ChunkState.VERIFYING, ChunkState.MISSING);
                stateTracker.incrementFailure(desc.id());
                source = nextPeerAfterFailure(desc.id().index(), source, pieceMaps);
                if (source == null)
                    break;
                stateTracker.transition(desc.id(), ChunkState.MISSING, ChunkState.SCHEDULED);

            } catch (Exception e) {
                // Covers network failures, timeouts, and any unchecked exception from
                // fetchChunkAsync/join — treated the same as a bad chunk: retry with
                // another peer if one is available.
                LOG.log(Level.DEBUG, "Chunk {0} attempt {1} failed: {2}",
                        desc.id().index(), attempts, e.getMessage());
                stateTracker.transition(desc.id(), ChunkState.IN_FLIGHT, ChunkState.MISSING);
                stateTracker.incrementFailure(desc.id());
                source = nextPeerAfterFailure(desc.id().index(), source, pieceMaps);
                if (source == null)
                    break;
                stateTracker.transition(desc.id(), ChunkState.MISSING, ChunkState.SCHEDULED);
            } finally {
                // Always release, whether the attempt succeeded, failed, or threw.
                inflightLimit.release();
            }
            attempts++;
        }

        LOG.log(Level.ERROR, "FAILED: chunk {0} after {1} attempts", desc.id().index(), attempts);
    }

    /**
     * Queries every peer's piece map concurrently and collects the results.
     * A peer that fails to respond (timeout, connection refused, etc.) is
     * simply omitted from the result rather than failing the whole transfer.
     */
    private Map<PeerInfo, BitSet> collectPieceMaps() {
        Map<PeerInfo, BitSet> result = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = peers.stream()
                .map(peer -> connector.fetchPieceMapAsync(peer, manifest.fileHash())
                        .thenAccept(bitset -> result.put(peer, bitset))
                        // Swallow individual peer failures here; a peer that never
                        // answers just won't be considered a source for any chunk.
                        .exceptionally(_ -> null))
                .toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return result;
    }

    /**
     * Picks the first peer (in iteration order) known to advertise the given
     * chunk index, or {@code null} if none do.
     */
    private PeerInfo selectPeer(int chunkIndex, Map<PeerInfo, BitSet> pieceMaps) {
        return pieceMaps.entrySet().stream()
                .filter(e -> e.getValue().get(chunkIndex))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    /**
     * Like {@link #selectPeer}, but excludes a specific peer — used to avoid
     * immediately retrying against the peer that just failed.
     */
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

    /**
     * Confirms every chunk in the manifest ended up in {@link #heldChunks}.
     * Called once after all download tasks have finished (successfully or not).
     *
     * @throws IllegalStateException if one or more chunks never got downloaded/verified
     */
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
        // Return a defensive copy so tests can't mutate internal state, taken
        // under the same lock used by downloadWithRetry for a consistent snapshot.
        synchronized (heldChunks) {
            return (BitSet) heldChunks.clone();
        }
    }
}