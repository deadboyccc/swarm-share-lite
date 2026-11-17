package io.swarmshare.networking;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.core.port.StorageProvider;
import io.swarmshare.manifest.ManifestBuilder;
import io.swarmshare.storage.FileChannelStorage;
import io.swarmshare.transfer.TransferManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Roadmap phase 6 failure scenarios over real TCP sockets and real files: peers
 * that drop connections mid-chunk, peers that serve corrupt bytes, and a download
 * interrupted part-way and resumed from what is already on disk.
 */
@Timeout(60)
class FailureScenariosIntegrationTest {

    private static final int CHUNK = 64 * 1024;

    @TempDir
    Path dir;

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (AutoCloseable resource : resources.reversed()) {
            resource.close();
        }
    }

    @Test
    void peerDroppingConnectionsMidChunk_isRoutedAroundToAHealthySeeder() throws Exception {
        Fixture f = fixture(8);
        PeerInfo dropper = droppingPeer(f.manifest());
        PeerInfo seeder = seeder(f, f.source(), allHeld(f.manifest()));

        download(f, List.of(dropper, seeder));

        assertThat(Files.readAllBytes(f.output())).isEqualTo(f.bytes());
    }

    @Test
    void seederServingCorruptBytes_isRoutedAroundToAnHonestSeeder() throws Exception {
        Fixture f = fixture(8);
        Path corruptCopy = dir.resolve("corrupt.bin");
        byte[] corrupted = f.bytes().clone();
        for (int offset = 0; offset < corrupted.length; offset += CHUNK) {
            corrupted[offset] ^= 0x01; // every chunk is wrong, sizes are right
        }
        Files.write(corruptCopy, corrupted);
        PeerInfo liar = seeder(f, corruptCopy, allHeld(f.manifest()));
        PeerInfo honest = seeder(f, f.source(), allHeld(f.manifest()));

        download(f, List.of(liar, honest));

        assertThat(Files.readAllBytes(f.output())).isEqualTo(f.bytes());
    }

    @Test
    void interruptedDownload_resumesAndFetchesOnlyTheMissingChunks() throws Exception {
        Fixture f = fixture(10);
        BitSet firstHalf = new BitSet();
        firstHalf.set(0, 5);
        PeerInfo partialSeeder = seeder(f, f.source(), firstHalf);

        // First run: the only peer has half the file, so the transfer stops incomplete.
        assertThatThrownBy(() -> download(f, List.of(partialSeeder)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("5 of 10 chunks missing");

        // Second run, new process state, same output file: only chunks 5..9 travel.
        Set<Integer> served = ConcurrentHashMap.newKeySet();
        PeerInfo fullSeeder = seeder(f, new CountingStorage(f.source(), served), allHeld(f.manifest()));
        download(f, List.of(fullSeeder));

        assertThat(Files.readAllBytes(f.output())).isEqualTo(f.bytes());
        assertThat(served).containsExactlyInAnyOrder(5, 6, 7, 8, 9);
    }

    // ── fixtures ─────────────────────────────────────────────────────────────────

    private record Fixture(byte[] bytes, Path source, Manifest manifest, Path output) {
    }

    private Fixture fixture(int chunks) throws IOException {
        byte[] bytes = new byte[chunks * CHUNK - 100]; // short last chunk
        new Random(chunks).nextBytes(bytes);
        Path source = Files.write(dir.resolve("source-" + chunks + ".bin"), bytes);
        return new Fixture(bytes, source, new ManifestBuilder(CHUNK).build(source), dir.resolve("out.bin"));
    }

    private void download(Fixture f, List<PeerInfo> peers) {
        try (var storage = new FileChannelStorage(f.output());
             var connector = new TcpPeerConnector()) {
            new TransferManager(f.manifest(), peers, storage, connector).start();
        }
    }

    private PeerInfo seeder(Fixture f, Path file, BitSet held) throws Exception {
        SeederFileStorage storage = new SeederFileStorage(file);
        resources.add(storage);
        return seeder(f, storage, held);
    }

    private PeerInfo seeder(Fixture f, StorageProvider storage, BitSet held) throws Exception {
        TcpChunkServer server = new TcpChunkServer(0, storage, f.manifest(), held);
        resources.add(server);
        int port = server.bind();
        Thread.startVirtualThread(() -> {
            try {
                server.serve();
            } catch (IOException ignored) {
                // closed after the test
            }
        });
        return new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", port));
    }

    /**
     * A peer that advertises every chunk, then hangs up halfway through each one —
     * the shape of a crashed or rebooting machine.
     */
    private PeerInfo droppingPeer(Manifest manifest) throws IOException {
        ServerSocket socket = new ServerSocket(0);
        resources.add(socket);
        Thread.startVirtualThread(() -> {
            while (!socket.isClosed()) {
                try (Socket client = socket.accept()) {
                    var in = new DataInputStream(client.getInputStream());
                    var out = new DataOutputStream(client.getOutputStream());
                    if (in.readByte() == FrameEncoder.MSG_PIECE_MAP_REQUEST) {
                        FrameDecoder.readPieceMapRequest(in);
                        FrameEncoder.writePieceMapResponse(out, allHeld(manifest).toByteArray());
                    } else {
                        var request = FrameDecoder.readChunkRequest(in);
                        int size = manifest.chunkAt(request.chunkIndex()).size();
                        out.writeByte(FrameEncoder.STATUS_OK);
                        out.writeInt(size);
                        out.write(new byte[size / 2]);
                        out.flush();
                    }
                } catch (IOException ignored) {
                    // next client
                }
            }
        });
        return new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", socket.getLocalPort()));
    }

    private static BitSet allHeld(Manifest manifest) {
        BitSet held = new BitSet();
        held.set(0, manifest.totalChunks());
        return held;
    }

    /** Seeder storage that records which chunk indices it actually served. */
    private final class CountingStorage implements StorageProvider {
        private final SeederFileStorage delegate;
        private final Set<Integer> served;

        CountingStorage(Path file, Set<Integer> served) throws IOException {
            this.delegate = new SeederFileStorage(file);
            this.served = served;
            resources.add(delegate);
        }

        @Override
        public void preallocateSpace(long totalSize) {
            delegate.preallocateSpace(totalSize);
        }

        @Override
        public void writeChunk(ChunkId id, long offset, byte[] data) {
            delegate.writeChunk(id, offset, data);
        }

        @Override
        public Optional<byte[]> readChunk(ChunkId id, long offset, int size) {
            served.add(id.index());
            return delegate.readChunk(id, offset, size);
        }

        @Override
        public BitSet checkExistingChunks(Manifest manifest) {
            return delegate.checkExistingChunks(manifest);
        }
    }
}
