// storage/FileChannelStorage.java
package io.swarmshare.storage;

import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.port.StorageProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.Optional;

/**
 * Writes chunks directly to their byte offset in a pre-allocated output file.
 * Uses FileChannel for position-aware reads and writes — no sequential constraint.
 * <p>
 * Why FileChannel over FileOutputStream?
 * - FileChannel.write(buffer, position) writes at an explicit offset without seeking
 * - Multiple virtual threads can write to different offsets simultaneously
 * - FileOutputStream is sequential; concurrent seeks would require external locking
 */
public class FileChannelStorage implements StorageProvider {

    private final Path outputPath;
    private final ChecksumVerifier verifier;

    // FileChannel is thread-safe for positional reads/writes at different offsets
    private FileChannel channel;

    public FileChannelStorage(Path outputPath) {
        this.outputPath = outputPath;
        this.verifier = new ChecksumVerifier();
    }

    @Override
    public void preallocateSpace(long totalSize) {
        try {
            // Open or create the file, then truncate/extend to exact size
            channel = FileChannel.open(outputPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            channel.truncate(totalSize); // pre-allocates exactly totalSize bytes
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to preallocate output file", e);
        }
    }

    @Override
    public void writeChunk(ChunkId id, long offset, byte[] data) {
        try {
            // ByteBuffer.wrap avoids copying: wraps the existing array
            // channel.write(buf, position) is atomic for concurrent calls at different positions
            ByteBuffer buffer = ByteBuffer.wrap(data);
            while (buffer.hasRemaining()) {
                // Loop handles partial writes (rare but contractually required by FileChannel)
                channel.write(buffer, offset + buffer.position());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write chunk " + id.index(), e);
        }
    }

    @Override
    public Optional<byte[]> readChunk(ChunkId id, long offset, int size) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            int bytesRead = 0;
            while (bytesRead < size) {
                int n = channel.read(buffer, offset + bytesRead);
                if (n == -1) return Optional.empty(); // file shorter than expected
                bytesRead += n;
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

    public void close() {
        try {
            if (channel != null) channel.close();
        } catch (IOException e) { /* log and swallow on close */ }
    }
}