package io.swarmshare.transfer;

import io.swarmshare.core.crypto.HasherPort;
import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.*;
import io.swarmshare.core.port.PeerConnector;
import io.swarmshare.core.port.StorageProvider;
import io.swarmshare.manifest.ManifestValidator;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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

    static final RetryPolicy DEFAULT_RETRY_POLICY =
            new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(2));

    private final Manifest manifest;
    private final List<PeerInfo> peers;
    private final StorageProvider storage;
    private final PeerConnector connector;
    private final HasherPort verifier;
    private final Sha256 fileHasher = new Sha256();
    private final RetryPolicy retryPolicy;
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
     * Creates a manager using the default SHA-256 verifier and retry policy.
     */
    public TransferManager(Manifest manifest, List<PeerInfo> peers,
                           StorageProvider storage, PeerConnector connector) {
        this(manifest, peers, storage, connector, new Sha256(), DEFAULT_RETRY_POLICY);
    }

    /**
     * Package-private constructor allowing tests to inject a fake {@link HasherPort}
     * and a {@link RetryPolicy} with zero delay.
     */
    TransferManager(Manifest manifest, List<PeerInfo> peers,
                    StorageProvider storage, PeerConnector connector,
                    HasherPort verifier) {
        this(manifest, peers, storage, connector, verifier, DEFAULT_RETRY_POLICY);
    }

    TransferManager(Manifest manifest, List<PeerInfo> peers,
                    StorageProvider storage, PeerConnector connector,
                    HasherPort verifier, RetryPolicy retryPolicy) {
        this.manifest = manifest;
        this.peers = List.copyOf(peers);
        this.storage = storage;
        this.connector = connector;
        this.verifier = verifier;
        this.retryPolicy = retryPolicy;
        this.stateTracker = new ChunkStateTracker();
        this.heldChunks = new BitSet(manifest.totalChunks());
    }

    /**
     * Runs the full transfer pipeline synchronously: pre-allocates disk space,
     * resumes any chunks already present and valid on disk, fetches peer piece
     * maps, then downloads every remaining chunk in parallel (one virtual
     * thread per chunk) before verifying the file is complete.
     *
     * @throws IllegalArgumentException if the manifest is internally inconsistent
     * @throws IllegalStateException    if any chunk is still missing once all
     *                                 downloads have finished or been abandoned,
     *                                 or if the assembled file hash does not match
     */
    public void start() {
        switch (ManifestValidator.validate(manifest)) {
            case ManifestValidator.ValidationResult.Valid ignored -> {
            }
            case ManifestValidator.ValidationResult.Invalid invalid ->
                    throw new IllegalArgumentException("Invalid manifest:\n" + invalid.summary());
        }

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
     * Downloads a single chunk, retrying according to {@link #retryPolicy} on
     * either a checksum mismatch or a network/IO exception. Each retry may
     * switch to a different peer via {@link #nextPeerAfterFailure}. Runs the
     * full attempt loop on whichever virtual thread called it, acquiring
     * {@link #inflightLimit} for the duration of each network round-trip.
     */
    private void downloadWithRetry(ChunkDescriptor desc, PeerInfo initial,
                                   Map<PeerInfo, BitSet> pieceMaps) {
        int attempts = 0;
        PeerInfo source = initial;

        while (retryPolicy.shouldRetry(attempts)) {
            attempts++;
            inflightLimit.acquireUninterruptibly();
            try {
                TransferResult result = fetchAndVerify(desc, source);
                switch (result) {
                    case TransferResult.Success(_, byte[] data) -> {
                        storage.writeChunk(desc.id(), desc.offset(), data);
                        stateTracker.transition(desc.id(), ChunkState.VERIFIED, ChunkState.WRITTEN);
                        synchronized (heldChunks) {
                            heldChunks.set(desc.id().index());
                        }
                        return;
                    }
                    case TransferResult.Failure(_, String reason, boolean shouldRetry) -> {
                        LOG.log(Level.DEBUG, "Chunk {0} attempt {1} failed: {2}",
                                desc.id().index(), attempts, reason);
                        if (!shouldRetry) {
                            stateTracker.reset(desc.id(), ChunkState.MISSING);
                            LOG.log(Level.ERROR, "FAILED: chunk {0} after {1} attempts",
                                    desc.id().index(), attempts);
                            return;
                        }
                        source = scheduleRetry(desc, source, pieceMaps);
                        if (source == null) {
                            stateTracker.reset(desc.id(), ChunkState.MISSING);
                            LOG.log(Level.ERROR, "FAILED: chunk {0} after {1} attempts",
                                    desc.id().index(), attempts);
                            return;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.log(Level.DEBUG, "Chunk {0} attempt {1} failed: {2}",
                        desc.id().index(), attempts, e.getMessage());
                source = scheduleRetry(desc, source, pieceMaps);
                if (source == null) {
                    stateTracker.reset(desc.id(), ChunkState.MISSING);
                    LOG.log(Level.ERROR, "FAILED: chunk {0} after {1} attempts",
                            desc.id().index(), attempts);
                    return;
                }
            } finally {
                inflightLimit.release();
            }

            if (retryPolicy.shouldRetry(attempts)) {
                try {
                    retryPolicy.sleep(attempts - 1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stateTracker.reset(desc.id(), ChunkState.MISSING);
                    return;
                }
            }
        }

        stateTracker.reset(desc.id(), ChunkState.MISSING);
        LOG.log(Level.ERROR, "FAILED: chunk {0} after {1} attempts", desc.id().index(), attempts);
    }

    /**
     * Fetches one chunk and checksums it. Write failures are handled by the caller
     * so a disk error after {@link ChunkState#VERIFIED} can still be retried.
     */
    private TransferResult fetchAndVerify(ChunkDescriptor desc, PeerInfo source) {
        try {
            stateTracker.transition(desc.id(), ChunkState.SCHEDULED, ChunkState.IN_FLIGHT);
            byte[] data = connector.fetchChunkAsync(source, desc.id(), desc.size()).join();
            stateTracker.transition(desc.id(), ChunkState.IN_FLIGHT, ChunkState.VERIFYING);
            if (!verifier.verify(data, desc.sha256())) {
                return new TransferResult.Failure(desc.id(), "checksum mismatch", true);
            }
            stateTracker.transition(desc.id(), ChunkState.VERIFYING, ChunkState.VERIFIED);
            return new TransferResult.Success(desc.id(), data);
        } catch (Exception e) {
            return new TransferResult.Failure(desc.id(), String.valueOf(e.getMessage()), true);
        }
    }

    /**
     * Resets the chunk to {@link ChunkState#SCHEDULED} after a failed attempt so
     * the next loop iteration can transition SCHEDULED → IN_FLIGHT. Uses
     * {@link ChunkStateTracker#reset} because the current state may be IN_FLIGHT
     * (network error) or VERIFYING/VERIFIED (hash/write error).
     *
     * @return the peer to use for the next attempt, or {@code null} if none remains
     */
    private PeerInfo scheduleRetry(ChunkDescriptor desc, PeerInfo source,
                                   Map<PeerInfo, BitSet> pieceMaps) {
        stateTracker.incrementFailure(desc.id());
        PeerInfo next = nextPeerAfterFailure(desc.id().index(), source, pieceMaps);
        if (next == null) {
            return null;
        }
        stateTracker.reset(desc.id(), ChunkState.SCHEDULED);
        return next;
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
     * Confirms every chunk is held, flushes storage, then re-hashes the assembled
     * file against {@link Manifest#fileHash()}.
     */
    private void verifyCompleteFile() {
        long missing;
        synchronized (heldChunks) {
            missing = manifest.chunks().stream()
                    .filter(desc -> !heldChunks.get(desc.id().index()))
                    .count();
        }
        if (missing > 0) {
            throw new IllegalStateException(
                    "Transfer incomplete: " + missing + " of " + manifest.totalChunks() + " chunks missing");
        }

        storage.flush();
        String actual = fileHasher.compute(chunkBytesInOrder());
        if (!fileHasher.hashesMatch(actual, manifest.fileHash())) {
            throw new IllegalStateException(
                    "Transfer complete but file hash mismatch: expected "
                            + manifest.fileHash() + " got " + actual);
        }
        LOG.log(Level.INFO, "Transfer complete. All {0} chunks verified. fileHash={1}",
                manifest.totalChunks(), manifest.fileHash());
    }

    private Iterator<byte[]> chunkBytesInOrder() {
        return new Iterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < manifest.totalChunks();
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                ChunkDescriptor desc = manifest.chunkAt(index++);
                return storage.readChunk(desc.id(), desc.offset(), desc.size())
                        .orElseThrow(() -> new IllegalStateException(
                                "Chunk " + desc.id().index() + " missing during file-hash verification"));
            }
        };
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
