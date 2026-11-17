// storage/FileChannelStorage.java
package io.swarmshare.storage;

import io.swarmshare.core.crypto.HasherPort;
import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.port.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Writes and reads chunks at explicit byte offsets in a pre-allocated output
 * file.
 *
 * <h3>Why {@link FileChannel}?</h3>
 * <ul>
 * <li>{@code FileChannel.write(buffer, position)} writes at an explicit offset
 * atomically
 * without moving a shared file-pointer — no external locking needed for
 * disjoint ranges.</li>
 * <li>{@code FileOutputStream} exposes only a sequential cursor; concurrent
 * seeks require
 * a mutex that serialises all writes.</li>
 * <li>Virtual threads can block on I/O without pinning their carrier thread
 * when using NIO
 * channels (JDK 21+).</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 * {@link FileChannel} guarantees atomicity for positional reads and writes at
 * <em>distinct</em>
 * offsets. Callers must ensure no two calls share an overlapping byte range.
 */
public final class FileChannelStorage implements StorageProvider, Closeable {

    private static final Logger LOG = System.getLogger(FileChannelStorage.class.getName());

    private final Path outputPath;
    private final HasherPort verifier;

    /**
     * Visible to concurrent virtual threads; volatile ensures safe publication
     * after
     * {@link #preallocateSpace} without a full synchronised block on the happy
     * path.
     */
    private volatile FileChannel channel;

    /**
     * True only once {@link #preallocateSpace} has installed a read-write channel.
     * A channel opened lazily by {@link #readChunk} (e.g. by a seeder) is read-only,
     * so writes must be refused with a clear message instead of failing deep inside NIO.
     */
    private volatile boolean writable;

    /**
     * Terminal: once closed, the storage never reopens the file. Without this a late
     * reader (for example a server handler racing shutdown) would silently reopen and
     * leak a channel.
     */
    private volatile boolean closed;

    /**
     * Guards swapping, lazily opening, and closing {@link #channel}. A
     * {@link ReentrantLock} rather than {@code synchronized} because opening the
     * file is blocking I/O (ARCHITECTURE.md §20.5). Positional reads and writes do
     * not take it.
     */
    private final ReentrantLock lifecycle = new ReentrantLock();

    public FileChannelStorage(Path outputPath) {
        this.outputPath = outputPath;
        this.verifier = new Sha256();
    }

    // ── StorageProvider ──────────────────────────────────────────────────────────

    /**
     * Creates and pre-allocates the output file to exactly {@code totalSize} bytes.
     *
     * <p>
     * If the file already exists at {@code totalSize}, contents are left intact so
     * {@link #checkExistingChunks} can resume a partial download. A dummy last-byte
     * write is used only when extending a new or shorter file — never on an already
     * correctly sized file, which would corrupt the last chunk.
     *
     * @param totalSize exact byte size the file must reach before any chunk is
     *                  written
     * @throws IllegalArgumentException if {@code totalSize} is non-positive
     * @throws UncheckedIOException     if the file cannot be created or
     *                                  pre-allocated
     */
    @Override
    public void preallocateSpace(long totalSize) {
        if (totalSize <= 0)
            throw new IllegalArgumentException("totalSize must be positive, got: " + totalSize);
        ensureNotClosed();

        try {
            Path parent = outputPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            FileChannel opened = FileChannel.open(
                    outputPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);

            try {
                long currentSize = opened.size();
                if (currentSize != totalSize) {
                    opened.truncate(totalSize);

                    if (currentSize < totalSize) {
                        // Materialise the allocation by writing one byte at the last position.
                        // Skip this when shrinking or when the file already had the right size.
                        opened.write(ByteBuffer.allocate(1), totalSize - 1);
                    }

                    opened.force(true);
                }

                FileChannel previous;
                lifecycle.lock();
                try {
                    if (closed) {
                        throw new IllegalStateException("Storage for %s is closed".formatted(outputPath));
                    }
                    previous = channel;
                    channel = opened;
                    writable = true;
                } finally {
                    lifecycle.unlock();
                }
                closeQuietly(previous);

                if (currentSize == totalSize) {
                    LOG.log(Level.INFO, "Reusing existing {0}-byte file at {1}", totalSize, outputPath);
                } else {
                    LOG.log(Level.INFO, "Pre-allocated {0} bytes at {1}", totalSize, outputPath);
                }
            } catch (IOException | RuntimeException e) {
                try {
                    opened.close();
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to pre-allocate %d bytes at %s".formatted(totalSize, outputPath), e);
        }
    }

    /**
     * Writes {@code data} at the given absolute byte {@code offset} in the file.
     *
     * <p>
     * Loops until all bytes are written — {@link FileChannel#write} may perform
     * partial writes on some OS/kernel configurations even for NIO channels.
     *
     * @param id     chunk identifier (used only for error reporting)
     * @param offset absolute byte offset into the output file
     * @param data   raw chunk bytes; must not be {@code null} or empty
     * @throws UncheckedIOException if any write attempt fails
     */
    @Override
    public void writeChunk(ChunkId id, long offset, byte[] data) {
        ensureNotClosed();
        FileChannel ch = channel;
        if (!writable || ch == null || !ch.isOpen()) {
            throw new IllegalStateException(
                    "Storage is not initialized for %s. Call preallocateSpace() before writing.".formatted(outputPath));
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        try {
            while (buffer.hasRemaining()) {
                int written = ch.write(buffer, offset + buffer.position());
                if (written <= 0) {
                    throw new IOException("Zero-length write at offset " + (offset + buffer.position()));
                }
            }
            ch.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to write chunk %d (%d bytes) at offset %d".formatted(id.index(), data.length, offset), e);
        }
    }

    /**
     * Reads {@code size} bytes from absolute byte {@code offset}.
     *
     * @param id     chunk identifier (used only for error reporting)
     * @param offset absolute byte offset into the output file
     * @param size   number of bytes to read
     * @return the chunk bytes, or {@link Optional#empty()} if the file is shorter
     *         than expected
     * @throws IllegalStateException if this storage has been closed
     */
    @Override
    public Optional<byte[]> readChunk(ChunkId id, long offset, int size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be >= 0, got: " + size);
        }

        ensureNotClosed();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        try {
            FileChannel ch = ensureReadableChannel();
            while (buffer.hasRemaining()) {
                int n = ch.read(buffer, offset + buffer.position());
                if (n <= 0)
                    return Optional.empty(); // EOF or no progress before we filled the buffer
            }
            return Optional.of(buffer.array());
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to read chunk {0} at offset {1}: {2}",
                    id.index(), offset, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Validates every chunk declared in {@code manifest}.
     *
     * <p>
     * For each chunk: reads the bytes at its declared offset, then verifies the
     * SHA-256 digest against the manifest's expected hash. Sets the corresponding
     * bit
     * in the returned {@link BitSet} only when <em>both</em> checks pass.
     *
     * @param manifest source of truth for chunk offsets, sizes, and expected hashes
     * @return a {@link BitSet} of length {@code manifest.totalChunks()} where bit
     *         {@code i}
     *         is {@code 1} iff chunk {@code i} exists on disk and its digest
     *         matches
     */
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
    public void flush() {
        FileChannel ch = channel;
        if (ch == null || !ch.isOpen()) {
            return;
        }
        try {
            ch.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to flush " + outputPath, e);
        }
    }

    // ── Closeable ────────────────────────────────────────────────────────────────

    /**
     * Returns the current channel, lazily opening the file read-only when no channel
     * exists yet. This is the seeder path: it serves an existing file and never calls
     * {@link #preallocateSpace}, so it must not get write access.
     */
    private FileChannel ensureReadableChannel() throws IOException {
        FileChannel ch = channel;
        if (ch != null && ch.isOpen()) {
            return ch;
        }
        lifecycle.lock();
        try {
            ensureNotClosed();
            ch = channel;
            if (ch != null && ch.isOpen()) {
                return ch;
            }
            if (!Files.exists(outputPath)) {
                throw new IOException("Storage file does not exist: " + outputPath);
            }
            ch = FileChannel.open(outputPath, StandardOpenOption.READ);
            channel = ch;
            writable = false;
            return ch;
        } finally {
            lifecycle.unlock();
        }
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("Storage for %s is closed".formatted(outputPath));
        }
    }

    /**
     * Closes the underlying {@link FileChannel}. Closing is terminal: later reads and
     * writes throw {@link IllegalStateException} rather than reopening the file.
     *
     * <p>
     * Idempotent — safe to call more than once. Exceptions during close are logged
     * but not propagated; the channel is unusable regardless of whether
     * {@code close()} itself throws.
     */
    @Override
    public void close() {
        FileChannel ch;
        lifecycle.lock();
        try {
            closed = true;
            writable = false;
            ch = channel;
            channel = null;
        } finally {
            lifecycle.unlock();
        }
        closeQuietly(ch);
    }

    private void closeQuietly(FileChannel ch) {
        if (ch == null || !ch.isOpen())
            return;
        try {
            ch.close();
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to close FileChannel for {0}: {1}", outputPath, e.getMessage());
        }
    }
}
