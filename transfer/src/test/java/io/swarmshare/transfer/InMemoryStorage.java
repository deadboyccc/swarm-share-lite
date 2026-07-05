package io.swarmshare.transfer;

import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.port.StorageProvider;

import java.util.BitSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link StorageProvider} for unit tests — no disk, no network.
 */
final class InMemoryStorage implements StorageProvider {

    private final ConcurrentHashMap<Integer, byte[]> chunks = new ConcurrentHashMap<>();
    private final Sha256 verifier = new Sha256();

    @Override
    public void preallocateSpace(long totalSize) {
        // no-op for in-memory
    }

    @Override
    public void writeChunk(ChunkId id, long offset, byte[] data) {
        chunks.put(id.index(), data.clone());
    }

    @Override
    public Optional<byte[]> readChunk(ChunkId id, long offset, int size) {
        byte[] data = chunks.get(id.index());
        if (data == null) return Optional.empty();
        return Optional.of(data.clone());
    }

    @Override
    public BitSet checkExistingChunks(Manifest manifest) {
        BitSet existing = new BitSet(manifest.totalChunks());
        for (ChunkDescriptor desc : manifest.chunks()) {
            readChunk(desc.id(), desc.offset(), desc.size())
                    .filter(data -> verifier.verify(data, desc.sha256()))
                    .ifPresent(_ -> existing.set(desc.id().index()));
        }
        return existing;
    }
}
