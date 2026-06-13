// core/port/PeerConnector.java
package io.swarmshare.core.port;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.PeerInfo;

import java.util.BitSet;
import java.util.concurrent.CompletableFuture;

/**
 * Abstracts all outgoing peer communication.
 *
 * Implementations:
 *   - TcpPeerConnector    (production: raw TCP binary framing)
 *   - FakePeerConnector   (testing: in-memory byte arrays)
 *
 * All methods return CompletableFuture to allow the orchestrator
 * to submit many concurrent requests without blocking the caller.
 * Each future runs on a virtual thread inside the implementation.
 */
public interface PeerConnector {

    /**
     * Request a specific chunk from a remote peer.
     * Returns the raw chunk bytes on success.
     * Exceptionally completes on timeout, connection refused, or NOT_FOUND.
     */
    CompletableFuture<byte[]> fetchChunkAsync(PeerInfo peer, ChunkId id, int size);

    /**
     * Request the remote peer's BitSet of held chunks for a given manifest.
     * Used to plan which peer to request each chunk from.
     */
    CompletableFuture<BitSet> fetchPieceMapAsync(PeerInfo peer, String manifestHash);
}