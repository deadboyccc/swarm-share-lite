package io.swarmshare.networking;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Deserialises swarm wire-protocol frames from a {@link DataInputStream}.
 *
 * <p>
 * This class contains only static helpers used by both the peer connector
 * and the chunk server to read framed binary messages across a TCP stream.
 * The wire format is intentionally simple: length-prefixed UTF-8 strings
 * and fixed-size integers. All methods are blocking and expect a
 * {@link java.io.DataInputStream} that wraps a socket input stream.
 */
public final class FrameDecoder {

    private FrameDecoder() {
    }

    /**
     * Reads exactly {@code n} bytes, looping until the buffer is full or the stream
     * ends.
     * Handles partial {@link DataInputStream#read(byte[], int, int)} returns from
     * TCP.
     */
    public static byte[] readExactly(DataInputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int offset = 0;
        while (offset < n) {
            int read = in.read(buf, offset, n - offset);
            if (read == -1) {
                throw new IOException("Stream ended before " + n + " bytes read (got " + offset + ")");
            }
            offset += read;
        }
        return buf;
    }

    /**
     * Parses the body of a CHUNK_REQUEST after the leading message-type byte has
     * been consumed.
     */
    public static ParsedChunkRequest readChunkRequest(DataInputStream in) throws IOException {
        int hashLen = in.readInt();
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
     */
    public static String readPieceMapRequest(DataInputStream in) throws IOException {
        int hashLen = in.readInt();
        if (hashLen < 0)
            throw new IOException("Invalid manifest hash length: " + hashLen);
        // Return the requested manifest hash as UTF-8 string
        return new String(readExactly(in, hashLen), StandardCharsets.UTF_8);
    }

    public record ParsedChunkRequest(String manifestHash, int chunkIndex) {
    }
}
