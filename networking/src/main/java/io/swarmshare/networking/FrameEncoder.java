package io.swarmshare.networking;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Serializes swarm wire-protocol frames to a {@link DataOutputStream}.
 *
 * <p>
 * All multi-byte integers use big-endian byte order, matching
 * {@link java.io.DataInputStream} / {@link java.io.DataOutputStream} defaults.
 * The helpers are minimal and intentionally explicit about lengths to avoid
 * ambiguity when reading from a raw TCP stream.
 */
public final class FrameEncoder {

    /**
     * Leading message-type byte for a piece-map (BitSet) request.
     */
    public static final byte MSG_PIECE_MAP_REQUEST = 0x01;
    /**
     * Leading message-type byte for a piece-map (BitSet) response.
     */
    public static final byte MSG_PIECE_MAP_RESPONSE = 0x02;
    /**
     * Leading message-type byte for a chunk-data request.
     */
    public static final byte MSG_CHUNK_REQUEST = 0x03;
    /**
     * Leading message-type byte for a chunk-data response.
     */
    public static final byte MSG_CHUNK_RESPONSE = 0x04;

    /**
     * Status code indicating the request succeeded and a payload follows.
     */
    public static final byte STATUS_OK = 0x00;
    /**
     * Status code indicating the requested resource (manifest/chunk) was not found.
     */
    public static final byte STATUS_NOT_FOUND = 0x01;
    /**
     * Status code indicating a generic server-side error while handling the request.
     */
    public static final byte STATUS_ERROR = 0x02;

    // Static-helpers-only class; no instances needed.
    private FrameEncoder() {
    }

    /**
     * Writes a CHUNK_REQUEST frame: a manifest hash identifying the swarm/file,
     * plus the index of the specific chunk being asked for.
     *
     * <pre>
     * Format: [MSG_CHUNK_REQUEST][hashLen:4][hashBytes][chunkIndex:4]
     * </pre>
     *
     * @param out          destination stream (flushed before returning)
     * @param manifestHash hash identifying the manifest/file this chunk belongs to
     * @param chunkIndex   zero-based index of the requested chunk
     * @throws IOException if writing to the stream fails
     */
    public static void writeChunkRequest(DataOutputStream out,
                                         String manifestHash, int chunkIndex)
            throws IOException {
        // Format: [MSG_CHUNK_REQUEST][hashLen:4][hashBytes][chunkIndex:4]
        byte[] hashBytes = manifestHash.getBytes(StandardCharsets.UTF_8);
        out.writeByte(MSG_CHUNK_REQUEST);
        out.writeInt(hashBytes.length);
        out.write(hashBytes);
        out.writeInt(chunkIndex);
        out.flush();
    }

    /**
     * Response layout: status (1) | payload length (4 BE) | payload (N).
     * No message-type prefix — the request/response pairing is implicit on a TCP
     * stream.
     *
     * @param out     destination stream (flushed before returning)
     * @param status  one of {@link #STATUS_OK}, {@link #STATUS_NOT_FOUND}, {@link #STATUS_ERROR}
     * @param payload chunk bytes to send; may be zero-length (e.g. for error/not-found responses)
     * @throws IOException if writing to the stream fails
     */
    public static void writeChunkResponse(DataOutputStream out, byte status, byte[] payload)
            throws IOException {
        // Response layout: status (1) | payload length (4 BE) | payload (N).
        out.writeByte(status);
        out.writeInt(payload.length);
        if (payload.length > 0) {
            out.write(payload);
        }
        out.flush();
    }

    /**
     * Writes a PIECE_MAP_REQUEST frame: a manifest hash identifying which
     * file's piece map (BitSet of held chunks) is being asked for.
     *
     * <pre>
     * Format: [MSG_PIECE_MAP_REQUEST][hashLen:4][hashBytes]
     * </pre>
     *
     * @param out          destination stream (flushed before returning)
     * @param manifestHash hash identifying the manifest/file whose piece map is requested
     * @throws IOException if writing to the stream fails
     */
    public static void writePieceMapRequest(DataOutputStream out, String manifestHash)
            throws IOException {
        // Format: [MSG_PIECE_MAP_REQUEST][hashLen:4][hashBytes]
        byte[] hashBytes = manifestHash.getBytes(StandardCharsets.UTF_8);
        out.writeByte(MSG_PIECE_MAP_REQUEST);
        out.writeInt(hashBytes.length);
        out.write(hashBytes);
        out.flush();
    }

    /**
     * Writes a PIECE_MAP_RESPONSE frame. Always reports {@link #STATUS_OK} —
     * callers that need to reject the request (e.g. unknown manifest hash)
     * should use {@link #writeChunkResponse} with {@link #STATUS_NOT_FOUND}
     * instead of calling this method.
     *
     * @param out         destination stream (flushed before returning)
     * @param bitSetBytes serialized form of the sender's held-chunks {@link java.util.BitSet}
     *                    (as produced by {@code BitSet.toByteArray()})
     * @throws IOException if writing to the stream fails
     */
    public static void writePieceMapResponse(DataOutputStream out, byte[] bitSetBytes)
            throws IOException {
        // Reply with OK status, then length-prefixed bitset bytes
        out.writeByte(STATUS_OK);
        out.writeInt(bitSetBytes.length);
        if (bitSetBytes.length > 0) {
            out.write(bitSetBytes);
        }
        out.flush();
    }
}