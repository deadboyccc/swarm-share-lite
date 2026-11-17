package io.swarmshare.networking;

import io.swarmshare.core.crypto.Sha256;
import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.port.StorageProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Protocol-level tests for {@link TcpChunkServer}: a raw socket client speaks the
 * wire format directly, so every status code the server can send is observable.
 */
@Timeout(20)
class TcpChunkServerTest {

    private static final Sha256 SHA = new Sha256();
    private static final byte[] CHUNK_0 = "chunk-zero".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CHUNK_1 = "chunk-one!".getBytes(StandardCharsets.UTF_8);
    private static final String HASH = SHA.compute("chunk-zerochunk-one!".getBytes(StandardCharsets.UTF_8));
    private static final Manifest MANIFEST = new Manifest(HASH, "two.bin", 20, 10, List.of(
            new ChunkDescriptor(new ChunkId(HASH, 0), 0, 10, SHA.compute(CHUNK_0)),
            new ChunkDescriptor(new ChunkId(HASH, 1), 10, 10, SHA.compute(CHUNK_1))));

    private TcpChunkServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    // ── chunk requests ───────────────────────────────────────────────────────────

    @Test
    void chunkRequest_heldChunk_returnsOkWithBytes() throws Exception {
        int port = start(allHeld(), storageOf(Map.of(0, CHUNK_0, 1, CHUNK_1)));

        Response response = requestChunk(port, HASH, 1);

        assertThat(response.status()).isEqualTo(FrameEncoder.STATUS_OK);
        assertThat(response.payload()).isEqualTo(CHUNK_1);
    }

    @Test
    void chunkRequest_unknownManifest_returnsNotFound() throws Exception {
        int port = start(allHeld(), storageOf(Map.of(0, CHUNK_0, 1, CHUNK_1)));

        Response response = requestChunk(port, "b".repeat(64), 0);

        assertThat(response.status()).isEqualTo(FrameEncoder.STATUS_NOT_FOUND);
        assertThat(response.payload()).isEmpty();
    }

    @Test
    void chunkRequest_indexOutOfRange_returnsNotFound() throws Exception {
        int port = start(allHeld(), storageOf(Map.of(0, CHUNK_0, 1, CHUNK_1)));

        assertThat(requestChunk(port, HASH, 2).status()).isEqualTo(FrameEncoder.STATUS_NOT_FOUND);
        assertThat(requestChunk(port, HASH, -1).status()).isEqualTo(FrameEncoder.STATUS_NOT_FOUND);
    }

    @Test
    void chunkRequest_chunkNotHeld_returnsNotFoundWithoutTouchingStorage() throws Exception {
        BitSet onlyFirst = new BitSet();
        onlyFirst.set(0);
        int port = start(onlyFirst, failingStorage());

        assertThat(requestChunk(port, HASH, 1).status()).isEqualTo(FrameEncoder.STATUS_NOT_FOUND);
    }

    @Test
    void chunkRequest_heldButStorageHasNoBytes_returnsNotFound() throws Exception {
        int port = start(allHeld(), storageOf(Map.of(0, CHUNK_0)));

        assertThat(requestChunk(port, HASH, 1).status()).isEqualTo(FrameEncoder.STATUS_NOT_FOUND);
    }

    @Test
    void chunkRequest_storageThrows_returnsErrorStatus() throws Exception {
        int port = start(allHeld(), failingStorage());

        Response response = requestChunk(port, HASH, 0);

        assertThat(response.status()).isEqualTo(FrameEncoder.STATUS_ERROR);
        assertThat(response.payload()).isEmpty();
    }

    @Test
    void chunkRequest_heldChunksUpdatedAfterStart_areServed() throws Exception {
        BitSet held = new BitSet();
        int port = start(held, storageOf(Map.of(0, CHUNK_0, 1, CHUNK_1)));
        assertThat(requestChunk(port, HASH, 0).status()).isEqualTo(FrameEncoder.STATUS_NOT_FOUND);

        synchronized (held) {
            held.set(0);
        }

        assertThat(requestChunk(port, HASH, 0).payload()).isEqualTo(CHUNK_0);
    }

    // ── piece-map requests ───────────────────────────────────────────────────────

    @Test
    void pieceMapRequest_returnsHeldChunks() throws Exception {
        BitSet held = new BitSet();
        held.set(1);
        int port = start(held, storageOf(Map.of()));

        try (var client = connect(port)) {
            FrameEncoder.writePieceMapRequest(client.out(), HASH);
            Response response = client.readResponse();

            assertThat(response.status()).isEqualTo(FrameEncoder.STATUS_OK);
            assertThat(BitSet.valueOf(response.payload())).isEqualTo(held);
        }
    }

    @Test
    void pieceMapRequest_unknownManifest_returnsNotFound() throws Exception {
        int port = start(allHeld(), storageOf(Map.of()));

        try (var client = connect(port)) {
            FrameEncoder.writePieceMapRequest(client.out(), "b".repeat(64));

            assertThat(client.readResponse().status()).isEqualTo(FrameEncoder.STATUS_NOT_FOUND);
        }
    }

    // ── malformed and idle clients ───────────────────────────────────────────────

    @Test
    void unknownMessageType_returnsErrorStatus() throws Exception {
        int port = start(allHeld(), storageOf(Map.of()));

        try (var client = connect(port)) {
            client.out().writeByte(0x7F);
            client.out().flush();

            assertThat(client.readResponse().status()).isEqualTo(FrameEncoder.STATUS_ERROR);
        }
    }

    @Test
    void oversizedHashLength_closesConnectionWithoutReadingPayload() throws Exception {
        int port = start(allHeld(), storageOf(Map.of()));

        try (var client = connect(port)) {
            client.out().writeByte(FrameEncoder.MSG_CHUNK_REQUEST);
            client.out().writeInt(Integer.MAX_VALUE);
            client.out().flush();

            assertThat(client.in().read()).isEqualTo(-1);
        }
    }

    @Test
    void idleClient_isDisconnectedAfterTimeout() throws Exception {
        server = new TcpChunkServer(0, storageOf(Map.of()), MANIFEST, allHeld(), 200);
        int port = startAndAwaitBind(server);

        try (var client = connect(port)) {
            long started = System.nanoTime();

            assertThat(client.in().read()).as("server closes the idle socket").isEqualTo(-1);
            assertThat((System.nanoTime() - started) / 1_000_000).isLessThan(5_000);
        }
    }

    // ── lifecycle ────────────────────────────────────────────────────────────────

    @Test
    void localPort_beforeStart_throws() {
        server = new TcpChunkServer(0, storageOf(Map.of()), MANIFEST, allHeld());

        assertThatThrownBy(server::localPort).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void start_afterClose_throws() {
        server = new TcpChunkServer(0, storageOf(Map.of()), MANIFEST, allHeld());
        server.close();

        assertThatThrownBy(server::start).isInstanceOf(IOException.class).hasMessageContaining("closed");
    }

    @Test
    void start_portInUse_failsWithBindException() throws Exception {
        try (var occupied = new ServerSocket(0)) {
            server = new TcpChunkServer(occupied.getLocalPort(), storageOf(Map.of()), MANIFEST, allHeld());

            assertThatThrownBy(server::start).isInstanceOf(BindException.class);
        }
    }

    @Test
    void bind_returnsRealPortWhenAskedForEphemeral_beforeServing() throws Exception {
        server = new TcpChunkServer(0, storageOf(Map.of()), MANIFEST, allHeld());

        int port = server.bind();

        assertThat(port).isPositive().isEqualTo(server.localPort());
    }

    @Test
    void bind_twice_isRejected() throws Exception {
        server = new TcpChunkServer(0, storageOf(Map.of()), MANIFEST, allHeld());
        server.bind();

        assertThatThrownBy(server::bind).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void serve_withoutBind_isRejected() {
        server = new TcpChunkServer(0, storageOf(Map.of()), MANIFEST, allHeld());

        assertThatThrownBy(server::serve).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void close_stopsAcceptingConnections() throws Exception {
        int port = start(allHeld(), storageOf(Map.of()));

        server.close();

        assertThatThrownBy(() -> {
            try (var socket = new Socket()) {
                socket.connect(new InetSocketAddress("localhost", port), 1_000);
            }
        }).isInstanceOf(IOException.class);
    }

    @Test
    void constructor_rejectsNonPositiveTimeout() {
        assertThatThrownBy(() -> new TcpChunkServer(0, storageOf(Map.of()), MANIFEST, allHeld(), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private int start(BitSet held, StorageProvider storage) throws InterruptedException {
        server = new TcpChunkServer(0, storage, MANIFEST, held);
        return startAndAwaitBind(server);
    }

    private static int startAndAwaitBind(TcpChunkServer server) throws InterruptedException {
        Thread.startVirtualThread(() -> {
            try {
                server.start();
            } catch (IOException ignored) {
                // closed by tearDown
            }
        });
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            try {
                return server.localPort();
            } catch (IllegalStateException notYetBound) {
                Thread.sleep(10);
            }
        }
        throw new AssertionError("server did not bind within 5s");
    }

    private static BitSet allHeld() {
        BitSet held = new BitSet();
        held.set(0, MANIFEST.totalChunks());
        return held;
    }

    private static Response requestChunk(int port, String hash, int index) throws IOException {
        try (var client = connect(port)) {
            FrameEncoder.writeChunkRequest(client.out(), hash, index);
            return client.readResponse();
        }
    }

    private static Client connect(int port) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("localhost", port), 2_000);
        socket.setSoTimeout(10_000);
        return new Client(socket, new DataInputStream(socket.getInputStream()),
                new DataOutputStream(socket.getOutputStream()));
    }

    private record Response(byte status, byte[] payload) {
    }

    private record Client(Socket socket, DataInputStream in, DataOutputStream out) implements AutoCloseable {
        Response readResponse() throws IOException {
            byte status = in.readByte();
            int length = in.readInt();
            return new Response(status, FrameDecoder.readExactly(in, length));
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static StorageProvider storageOf(Map<Integer, byte[]> chunks) {
        return new FakeStorage(index -> Optional.ofNullable(chunks.get(index)).map(byte[]::clone));
    }

    private static StorageProvider failingStorage() {
        return new FakeStorage(index -> {
            throw new IllegalStateException("disk unavailable");
        });
    }

    /** Read-only fake; the server never writes, so write paths fail loudly. */
    private record FakeStorage(Function<Integer, Optional<byte[]>> reader) implements StorageProvider {

        @Override
        public void preallocateSpace(long totalSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeChunk(ChunkId id, long offset, byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<byte[]> readChunk(ChunkId id, long offset, int size) {
            return reader.apply(id.index());
        }

        @Override
        public BitSet checkExistingChunks(Manifest manifest) {
            throw new UnsupportedOperationException();
        }
    }
}
