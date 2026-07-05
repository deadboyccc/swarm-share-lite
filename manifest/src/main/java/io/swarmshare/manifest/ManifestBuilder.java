// manifest/ManifestBuilder.java
package io.swarmshare.manifest;

import io.swarmshare.core.crypto.HasherPort;
import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Builds a {@link Manifest} by splitting a file into fixed-size chunks and computing
 * SHA-256 checksums for each chunk and for the complete file.
 *
 * <h3>Design: streaming over loading</h3>
 * The design doc's original sketch used {@code Files.readAllBytes()}, which loads the
 * entire file into the heap. For a 10 GB ISO that would require 10 GB of heap — impractical
 * in production. This implementation reads the file once via {@link FileChannel}, processing
 * one chunk at a time, so heap usage is bounded by {@code chunkSize} regardless of total
 * file size.
 *
 * <h3>Single-pass whole-file hash</h3>
 * A second read pass to compute the whole-file hash is avoided by feeding chunk bytes
 * into a {@link MessageDigest} incrementally during the same loop that computes per-chunk
 * hashes. The whole-file hash is finalised only after all chunks are read.
 *
 * <h3>Two-phase ChunkId construction</h3>
 * {@link ChunkId} requires the whole-file hash, which is not known until all chunks have
 * been processed. Chunk metadata is therefore accumulated as {@link PendingChunkMeta}
 * (index, offset, size, sha256) and promoted to {@link ChunkDescriptor} after the loop.
 *
 * <h3>Chunk size constraints</h3>
 * {@code chunkSize} must be {@code >= 1}. Values above {@link Integer#MAX_VALUE} are not
 * representable as a Java {@code ByteBuffer} capacity and are therefore rejected.
 */
public final class ManifestBuilder {

    /**
     * Default: 1 MiB — balances network round-trips with per-chunk overhead.
     */
    public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

    private static final Logger LOG = System.getLogger(ManifestBuilder.class.getName());
    private static final HexFormat HEX = HexFormat.of();

    /**
     * Number of bytes per chunk, applied to every chunk except possibly the last.
     */
    private final int chunkSize;
    /**
     * Hashing strategy used to compute each chunk's checksum.
     */
    private final HasherPort verifier;

    /**
     * Creates a builder that splits files into chunks of {@code chunkSize} bytes.
     *
     * @param chunkSize number of bytes per chunk; must be {@code >= 1}
     * @throws IllegalArgumentException if {@code chunkSize < 1}
     */
    public ManifestBuilder(int chunkSize, HasherPort verifier) {
        if (chunkSize < 1) throw new IllegalArgumentException(
                "chunkSize must be >= 1, got: " + chunkSize);
        this.chunkSize = chunkSize;
        this.verifier = verifier;
    }

    /**
     * Creates a builder that splits files into chunks of {@code chunkSize} bytes,
     * using the default SHA-256 {@link HasherPort} implementation.
     *
     * @param chunkSize number of bytes per chunk; must be {@code >= 1}
     * @throws IllegalArgumentException if {@code chunkSize < 1}
     */
    public ManifestBuilder(int chunkSize) {
        this(chunkSize, new Sha256());
    }

    /**
     * Creates a builder with the {@link #DEFAULT_CHUNK_SIZE} of 1 MiB.
     */
    public ManifestBuilder() {
        this(DEFAULT_CHUNK_SIZE);
    }

    // ── public API ────────────────────────────────────────────────────────────────

    /**
     * Creates a fresh {@link MessageDigest} for SHA-256. Wraps the checked
     * {@link NoSuchAlgorithmException} since SHA-256 is guaranteed to be present
     * on any standard-compliant JVM, making the checked exception unreachable
     * in practice.
     */
    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the Java SE specification — truly unreachable
            throw new AssertionError("SHA-256 not available on this JVM", e);
        }
    }

    // ── internal ─────────────────────────────────────────────────────────────────

    /**
     * Reads {@code filePath} once, computes per-chunk and whole-file SHA-256 checksums,
     * and returns the fully populated {@link Manifest}.
     *
     * <p>The file is opened read-only and closed before this method returns.
     * Peak heap usage is bounded by {@code chunkSize}, not by file size.
     *
     * @param filePath path to the file to chunk; must exist and be readable
     * @return a fully populated, immutable {@link Manifest}
     * @throws IllegalArgumentException if the file is empty (no chunks can be produced)
     * @throws UncheckedIOException     if the file cannot be opened or read
     */
    public Manifest build(Path filePath) {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long totalSize = channel.size();

            if (totalSize == 0) throw new IllegalArgumentException(
                    "Cannot build a manifest for an empty file: " + filePath);

            String fileName = filePath.getFileName().toString();
            LOG.log(Level.INFO, "Building manifest for ''{0}'' ({1} bytes, chunkSize={2})",
                    fileName, totalSize, chunkSize);

            return buildFromChannel(channel, fileName, totalSize);

        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read file for manifest building: " + filePath, e);
        }
    }

    /**
     * Single-pass implementation: reads the file sequentially one chunk at a time,
     * accumulating per-chunk metadata and the whole-file digest simultaneously.
     *
     * @param channel   open, readable channel positioned at the start of the file
     * @param fileName  display name recorded in the resulting manifest
     * @param totalSize total file size in bytes, as reported by the channel
     * @return the fully assembled manifest, with {@code fileHash} computed from
     * every byte read during this pass
     * @throws IOException if a read from {@code channel} fails
     */
    private Manifest buildFromChannel(FileChannel channel, String fileName, long totalSize)
            throws IOException {

        // Whole-file digest fed incrementally — avoids re-reading the file
        MessageDigest fileDigest = newSha256();

        // Chunk metadata accumulator — only primitives + hash strings, not raw bytes
        List<PendingChunkMeta> pending = new ArrayList<>();

        // Single reusable buffer — avoids re-allocation per chunk
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        long fileOffset = 0;
        int index = 0;

        while (fileOffset < totalSize) {
            buffer.clear();

            // Read up to chunkSize bytes from the current file offset.
            // FileChannel.read(buf, pos) may return fewer bytes than remaining capacity
            // (partial read) — loop until either the buffer is full or we reach EOF.
            while (buffer.hasRemaining()) {
                int n = channel.read(buffer, fileOffset + buffer.position());
                if (n == -1) break; // EOF
            }

            buffer.flip(); // switch to read mode: limit = bytes read, position = 0
            int bytesRead = buffer.limit();
            if (bytesRead == 0) break; // defensive guard against an infinite loop at EOF

            // Copy buffer content to a byte array for hashing.
            // This is unavoidable: MessageDigest.update(byte[]) needs a concrete array.
            byte[] chunkBytes = new byte[bytesRead];
            buffer.get(chunkBytes);

            // Feed into whole-file digest incrementally
            fileDigest.update(chunkBytes);

            // Per-chunk hash (stateless per-call allocation — see ChecksumVerifier)
            String chunkHash = verifier.compute(chunkBytes);

            pending.add(new PendingChunkMeta(index, fileOffset, bytesRead, chunkHash));

            fileOffset += bytesRead;
            index++;
        }

        // Finalize whole-file hash now that all bytes have been fed
        String fileHash = HEX.formatHex(fileDigest.digest());

        // Promote PendingChunkMeta → ChunkDescriptor now that fileHash is known
        List<ChunkDescriptor> descriptors = pending.stream()
                .map(meta -> meta.toDescriptor(fileHash))
                .toList();

        LOG.log(Level.INFO, "Manifest built: {0} chunks, fileHash={1}", index, fileHash);

        return new Manifest(fileHash, fileName, totalSize, chunkSize, descriptors);
    }

    // ── internal value type ───────────────────────────────────────────────────────

    /**
     * Holds the chunk metadata collected during the read loop, before the whole-file
     * hash is available. Using a private record here keeps the two-phase logic explicit
     * rather than scattering nulls or sentinel values through the main code.
     */
    private record PendingChunkMeta(int index, long offset, int size, String sha256) {

        /**
         * Promotes this pending metadata to a fully realised {@link ChunkDescriptor}
         * once the whole-file hash is known.
         *
         * @param fileHash the SHA-256 of the complete assembled file
         */
        ChunkDescriptor toDescriptor(String fileHash) {
            return new ChunkDescriptor(new ChunkId(fileHash, index), offset, size, sha256);
        }
    }
}