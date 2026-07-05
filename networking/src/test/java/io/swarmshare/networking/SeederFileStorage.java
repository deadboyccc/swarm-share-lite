package io.swarmshare.networking;

import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.port.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.Optional;

/**
 * Read-only {@link StorageProvider} backed by an existing file on disk.
 * Used in integration tests where the seeder serves a pre-written source file.
 */
final class SeederFileStorage implements StorageProvider, Closeable {

    private final FileChannel channel;
    private final Sha256 verifier = new Sha256();

    SeederFileStorage(Path existingFile) throws IOException {
        channel = FileChannel.open(existingFile, StandardOpenOption.READ);
    }

    @Override
    public void preallocateSpace(long totalSize) {
        // source file already exists
    }

    @Override
    public void writeChunk(ChunkId id, long offset, byte[] data) {
        throw new UnsupportedOperationException("Seeder storage is read-only");
    }

    @Override
    public Optional<byte[]> readChunk(ChunkId id, long offset, int size) {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        try {
            while (buffer.hasRemaining()) {
                int n = channel.read(buffer, offset + buffer.position());
                if (n == -1) return Optional.empty();
            }
            return Optional.of(buffer.array());
        } catch (IOException e) {
            return Optional.empty();
        }
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

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
