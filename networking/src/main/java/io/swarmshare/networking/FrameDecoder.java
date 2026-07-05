package io.swarmshare.networking;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Deserializes swarm wire-protocol frames from a {@link DataInputStream}.
 *
 * <p>
 * This class contains only static helpers used by both the peer connector
 * and the chunk server to read framed binary messages across a TCP stream.
 * The wire format is intentionally simple: length-prefixed UTF-8 strings
 * and fixed-size integers. All methods are blocking and expect a
 * {@link java.io.DataInputStream} that wraps a socket input stream.
 */
public final class FrameDecoder {

    // Static-helpers-only class; no instances needed.
    private FrameDecoder() {
    }

    /**
     * Reads exactly {@code n} bytes, looping until the buffer is full or the stream
     * ends.
     * Handles partial {@link DataInputStream#read(byte[], int, int)} returns from
     * TCP.
     *
     * @param in the stream to read from
     * @param n  the exact number of bytes to read
     * @return a newly allocated array of length {@code n} containing the bytes read
     * @throws IOException if the stream ends before {@code n} bytes have been read
     */
    public static byte[] readExactly(DataInputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int offset = 0;
        // A single read() call over TCP may return fewer bytes than requested,
        // so we keep reading into the remaining slice of the buffer until it's full.
        while (offset < n) {
            int read = in.read(buf, offset, n - offset);
            if (read == -1) {
                // -1 means the peer closed the connection mid-frame; treat as
                // an error rather than silently returning a short buffer.
                throw new IOException("Stream ended before " + n + " bytes read (got " + offset + ")");
            }
            offset += read;
        }
        return buf;
    }

    /**
     * Parses the body of a CHUNK_REQUEST after the leading message-type byte has
     * been consumed.
     *
     * <p>Wire format: 4-byte length + UTF-8 manifest hash, followed by a
     * 4-byte chunk index.
     *
     * @param in the stream positioned right after the message-type byte
     * @return the parsed manifest hash and chunk index
     * @throws IOException if the declared hash length is negative or the stream
     *                     ends before the full request body is read
     */
    public static ParsedChunkRequest readChunkRequest(DataInputStream in) throws IOException {
        int hashLen = in.readInt();
        // Guard against a corrupt/malicious length that would otherwise cause
        // readExactly to attempt allocating a negative-sized array.
        if (hashLen < 0)
            throw new IOException("Invalid manifest hash length: " + hashLen);
        // Manifest hash is length-prefixed UTF-8 string
        String manifestHash = new String(readExactly(in, hashLen), StandardCharsets.UTF_8);
        int chunkIndex = in.readInt();
        return new ParsedChunkRequest(manifestHash, chunkIndex);
    }

    /**
     * Parses the body of a PIECE_MAP_REQUEST after the leading message-type byte
     * has been consumed.
     *
     * <p>Wire format: 4-byte length + UTF-8 manifest hash.
     *
     * @param in the stream positioned right after the message-type byte
     * @return the requested manifest hash
     * @throws IOException if the declared hash length is negative or the stream
     *                     ends before the full request body is read
     */
    public static String readPieceMapRequest(DataInputStream in) throws IOException {
        int hashLen = in.readInt();
        if (hashLen < 0)
            throw new IOException("Invalid manifest hash length: " + hashLen);
        // Return the requested manifest hash as UTF-8 string
        return new String(readExactly(in, hashLen), StandardCharsets.UTF_8);
    }

    /**
     * Decoded body of a CHUNK_REQUEST message: which manifest, and which chunk index within it.
     */
    public record ParsedChunkRequest(String manifestHash, int chunkIndex) {
    }
}