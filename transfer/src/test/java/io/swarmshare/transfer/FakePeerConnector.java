package io.swarmshare.transfer;

import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.core.port.PeerConnector;

import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test double that serves chunk bytes from an in-memory map.
 */
final class FakePeerConnector implements PeerConnector {

    private final String manifestHash;
    private final Map<Integer, byte[]> chunks;
    private final Set<Integer> corruptOnce = ConcurrentHashMap.newKeySet();

    FakePeerConnector(Manifest manifest, Map<Integer, byte[]> chunks) {
        this.manifestHash = manifest.fileHash();
        this.chunks = chunks;
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

    /**
     * Returns corrupt data on the first fetch for {@code chunkIndex}, then correct data.
     */
    FakePeerConnector corruptOnce(int chunkIndex) {
        corruptOnce.add(chunkIndex);
        return this;
    }

    @Override
    public CompletableFuture<byte[]> fetchChunkAsync(PeerInfo peer, ChunkId id, int size) {
        return CompletableFuture.supplyAsync(() -> {
            byte[] data = chunks.get(id.index());
            if (data == null) {
                throw new RuntimeException("Chunk not found: " + id.index());
            }
            if (corruptOnce.remove(id.index())) {
                byte[] bad = data.clone();
                bad[0] ^= 0xFF;
                return bad;
            }
            return data.clone();
        });
    }

    @Override
    public CompletableFuture<BitSet> fetchPieceMapAsync(PeerInfo peer, String hash) {
        return CompletableFuture.supplyAsync(() -> {
            BitSet held = new BitSet();
            chunks.keySet().forEach(held::set);
            return held;
        });
    }
}
