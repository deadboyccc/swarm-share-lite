package io.swarmshare.networking;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.core.port.PeerConnector;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * {@link PeerConnector} implementation over plain TCP binary framing.
 *
 * <p>
 * Opens a fresh connection per request — simple and stateless. Connection
 * pooling
 * is a future optimisation. Each fetch method opens a socket, exchanges a
 * single
 * request/response pair, and then closes the socket. Timeouts are applied to
 * both
 * connect and read operations to avoid indefinite blocking.
 */
public final class TcpPeerConnector implements PeerConnector {

    /**
     * Connect and read timeout, in milliseconds, applied to every socket operation.
     */
    private static final int TIMEOUT_MS = 10_000;

    /**
     * Runs each fetch on its own virtual thread. Since every request opens a
     * blocking socket and waits on I/O, virtual threads let us issue many
     * concurrent peer requests without paying for one platform thread each.
     */
    private static final Executor VIRTUAL_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * Fetches a single chunk's bytes from a peer over a dedicated TCP connection.
     *
     * <p>Protocol: send a chunk request, then read a 1-byte status code followed
     * by a 4-byte big-endian length, followed by exactly that many bytes of chunk
     * data. The connection is opened, used, and closed within this call.
     *
     * @param peer the peer to contact
     * @param id   identifies the manifest and chunk index being requested
     * @param size expected chunk size in bytes, validated against the peer's response
     * @return a future completing with the chunk bytes, or completing exceptionally
     * if the connection fails, the peer reports a non-OK status, or the
     * returned length doesn't match {@code size}
     */
    @Override
    public CompletableFuture<byte[]> fetchChunkAsync(PeerInfo peer, ChunkId id, int size) {
        return CompletableFuture.supplyAsync(() -> {
            // try-with-resources guarantees the socket is closed even if an
            // exception is thrown while writing the request or reading the response.
            try (Socket socket = new Socket()) {
                socket.connect(peer.address(), TIMEOUT_MS);
                socket.setSoTimeout(TIMEOUT_MS);
                // Set up Data streams for simple length-prefixed framing
                var out = new DataOutputStream(socket.getOutputStream());
                var in = new DataInputStream(socket.getInputStream());

                // Send chunk request and read response header
                FrameEncoder.writeChunkRequest(out, id.manifestHash(), id.index());

                // Response header: 1-byte status code + 4-byte payload length
                byte status = in.readByte();
                int dataLen = in.readInt();

                // Fail fast if the peer explicitly reported an error status
                if (status != FrameEncoder.STATUS_OK) {
                    throw new IOException("Peer returned status 0x%02x for chunk %d"
                            .formatted(status, id.index()));
                }
                // Guard against a peer sending a different amount of data than
                // the caller expects for this chunk (e.g. stale/corrupt manifest).
                if (dataLen != size) {
                    throw new IOException("Expected %d bytes, peer sent %d".formatted(size, dataLen));
                }

                // Read exactly the number of bytes advertised by the peer
                return FrameDecoder.readExactly(in, dataLen);
            } catch (IOException e) {
                // Wrap as unchecked so it can propagate through CompletableFuture,
                // which does not support checked exceptions in supplyAsync.
                throw new RuntimeException("Chunk fetch failed: " + id, e);
            }
        }, VIRTUAL_EXECUTOR);
    }

    /**
     * Fetches a peer's piece map — a {@link BitSet} indicating which chunks of
     * the given manifest the peer currently holds — over a dedicated TCP
     * connection.
     *
     * <p>Protocol: send a piece-map request, then read a 1-byte status code
     * followed by a 4-byte big-endian length, followed by exactly that many
     * bytes representing the serialized {@code BitSet}.
     *
     * @param peer         the peer to query
     * @param manifestHash identifies which file's piece map is being requested
     * @return a future completing with the peer's piece map, or completing
     * exceptionally if the connection fails or the peer reports a
     * non-OK status
     */
    @Override
    public CompletableFuture<BitSet> fetchPieceMapAsync(PeerInfo peer, String manifestHash) {
        return CompletableFuture.supplyAsync(() -> {
            try (Socket socket = new Socket()) {
                socket.connect(peer.address(), TIMEOUT_MS);
                socket.setSoTimeout(TIMEOUT_MS);
                var out = new DataOutputStream(socket.getOutputStream());
                var in = new DataInputStream(socket.getInputStream());

                // Send request for piece map and parse the length-prefixed response
                FrameEncoder.writePieceMapRequest(out, manifestHash);

                // Response header: 1-byte status code + 4-byte payload length
                byte status = in.readByte();
                int dataLen = in.readInt();

                if (status != FrameEncoder.STATUS_OK) {
                    throw new IOException("PieceMap request failed with status 0x%02x".formatted(status));
                }

                // Deserialize the raw bytes back into a BitSet using the same
                // encoding the peer used to produce them (BitSet.valueOf/toByteArray).
                byte[] bitSetBytes = FrameDecoder.readExactly(in, dataLen);
                return BitSet.valueOf(bitSetBytes);
            } catch (IOException e) {
                throw new RuntimeException("PieceMap fetch failed for peer: " + peer.id(), e);
            }
        }, VIRTUAL_EXECUTOR);
    }
}