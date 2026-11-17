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
import java.util.stream.Collectors;

/**
 * Central orchestrator for the swarm download pipeline:
 * resume → collect piece maps → parallel download → verify → persist.
 *
 * <p>
 * Resilience (roadmap phase 6): each chunk is retried with exponential backoff,
 * moving to another peer that advertises it after a failure; peers that keep
 * serving data which fails verification are banned for the rest of the transfer
 * (see {@link PeerBanList}).
 */
public final class TransferManager {

    private static final Logger LOG = System.getLogger(TransferManager.class.getName());

    /**
     * Maximum number of chunk downloads allowed in flight at once, across all peers.
     */
    static final int MAX_INFLIGHT = 32;

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
     * Bounds concurrent downloads (default {@link #MAX_INFLIGHT}) to avoid saturating network buffers.
     */
    private final Semaphore inflightLimit;
    /**
     * Peers banned for serving corrupt chunks during this transfer.
     */
    private final PeerBanList banList;
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
        this(manifest, peers, storage, connector, verifier, retryPolicy, MAX_INFLIGHT, new PeerBanList());
    }

    /**
     * Test seam: a small in-flight limit makes download order observable, and an
     * injected {@link PeerBanList} lets tests inspect bans.
     */
    TransferManager(Manifest manifest, List<PeerInfo> peers,
                    StorageProvider storage, PeerConnector connector,
                    HasherPort verifier, RetryPolicy retryPolicy,
                    int maxInflight, PeerBanList banList) {
        if (maxInflight < 1) {
            throw new IllegalArgumentException("maxInflight must be >= 1, got: " + maxInflight);
        }
        this.manifest = manifest;
        this.peers = List.copyOf(peers);
        this.storage = storage;
        this.connector = connector;
        this.verifier = verifier;
        this.retryPolicy = retryPolicy;
        this.inflightLimit = new Semaphore(maxInflight);
        this.banList = banList;
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
        if (!existing.isEmpty()) {
            LOG.log(Level.INFO, "Resuming: {0} of {1} chunks already on disk",
                    existing.cardinality(), manifest.totalChunks());
        }
        existing.stream().forEach(i -> {
            heldChunks.set(i);
            ChunkId id = manifest.chunkAt(i).id();
            stateTracker.initialize(id);
            stateTracker.transition(id, ChunkState.MISSING, ChunkState.WRITTEN);
        });

        // Determine which chunks are still missing on disk and need download
        List<ChunkDescriptor> missing = manifest.chunks().stream()
                .filter(desc -> !heldChunks.get(desc.id().index()))
                .toList();
        if (!missing.isEmpty()) {
            downloadMissing(missing, collectPieceMaps());
        }

        verifyCompleteFile();
    }

    /**
     * Schedules one virtual thread per missing chunk. Chunks that no reachable peer
     * advertises are reported once, in aggregate, instead of once per chunk — a
     * swarm with every peer down would otherwise print thousands of identical lines.
     */
    private void downloadMissing(List<ChunkDescriptor> missing, Map<PeerInfo, BitSet> peerPieceMaps) {
        if (peerPieceMaps.isEmpty()) {
            LOG.log(Level.WARNING, "None of the {0} configured peer(s) answered the piece-map request",
                    peers.size());
        }
        int unavailable = 0;
        // try-with-resources on the executor blocks here until every submitted
        // download task completes (or is interrupted), since AutoCloseable
        // executors await task completion on close().
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (ChunkDescriptor chunk : missing) {
                // Prepare per-chunk state and select a peer that advertises it
                stateTracker.initialize(chunk.id());
                PeerInfo source = selectPeer(chunk.id().index(), peerPieceMaps);
                if (source == null) {
                    unavailable++;
                    continue;
                }
                stateTracker.transition(chunk.id(), ChunkState.MISSING, ChunkState.SCHEDULED);
                // Download on its own virtual thread
                executor.submit(() -> downloadWithRetry(chunk, source, peerPieceMaps));
            }
        }
        if (unavailable > 0) {
            LOG.log(Level.WARNING, "No reachable peer advertises {0} of {1} chunks",
                    unavailable, manifest.totalChunks());
        }
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
                // The source was chosen when the chunk was scheduled; the peer may have
                // been banned while this thread waited for an in-flight slot.
                if (banList.isBanned(source)) {
                    source = selectPeer(desc.id().index(), pieceMaps);
                    if (source == null) {
                        stateTracker.reset(desc.id(), ChunkState.MISSING);
                        LOG.log(Level.ERROR, "FAILED: chunk {0}: every peer that has it is banned",
                                desc.id().index());
                        return;
                    }
                }
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
                if (banList.recordChecksumFailure(source)) {
                    // Report the threshold, not the live count: other download threads may
                    // record further failures for this peer before this line is formatted.
                    LOG.log(Level.WARNING, "Banned peer {0} after {1} chunks failed checksum verification",
                            source.displayAddress(), String.valueOf(banList.maxChecksumFailures()));
                }
                return new TransferResult.Failure(desc.id(),
                        "checksum mismatch from " + source.displayAddress(), true);
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

    /**
     * Naive selection: the first peer, in the order the caller configured them, that
     * advertises the chunk and is not banned. Walking {@link #peers} rather than the
     * concurrent map keeps the choice deterministic, so a user's peer list doubles
     * as a priority list.
     */
    private PeerInfo selectPeer(int chunkIndex, Map<PeerInfo, BitSet> pieceMaps) {
        return peers.stream()
                .filter(peer -> usable(pieceMaps, peer, chunkIndex))
                .findFirst()
                .orElse(null);
    }

    private PeerInfo selectAlternativePeer(int chunkIndex, PeerInfo exclude,
                                           Map<PeerInfo, BitSet> pieceMaps) {
        return peers.stream()
                .filter(peer -> !peer.equals(exclude))
                .filter(peer -> usable(pieceMaps, peer, chunkIndex))
                .findFirst()
                .orElse(null);
    }

    /** Advertises the chunk in its piece map and has not been banned. */
    private boolean usable(Map<PeerInfo, BitSet> pieceMaps, PeerInfo peer, int chunkIndex) {
        BitSet pieceMap = pieceMaps.get(peer);
        return pieceMap != null && pieceMap.get(chunkIndex) && !banList.isBanned(peer);
    }

    private PeerInfo nextPeerAfterFailure(int chunkIndex, PeerInfo current,
                                          Map<PeerInfo, BitSet> pieceMaps) {
        PeerInfo alternative = selectAlternativePeer(chunkIndex, current, pieceMaps);
        if (alternative != null) {
            return alternative;
        }
        return usable(pieceMaps, current, chunkIndex) ? current : null;
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
            String bans = banList.banned().isEmpty() ? "" : " (banned for corrupt data: "
                    + banList.banned().stream().map(PeerInfo::displayAddress).sorted()
                    .collect(Collectors.joining(", ")) + ")";
            throw new IllegalStateException(
                    "Transfer incomplete: " + missing + " of " + manifest.totalChunks() + " chunks missing" + bans);
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
