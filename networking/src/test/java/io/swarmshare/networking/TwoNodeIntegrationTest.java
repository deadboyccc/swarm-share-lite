package io.swarmshare.networking;

import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.manifest.ManifestBuilder;
import io.swarmshare.storage.FileChannelStorage;
import io.swarmshare.transfer.TransferManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test: TCP seeder and leecher over loopback.
 *
 * <p>
 * Verifies that a file can be split into chunks, served via TCP,
 * downloaded by a peer, and reassembled correctly. Uses temporary files
 * and automatic cleanup. This test does NOT use mocks — it exercises
 * real TCP sockets, chunk serving, and transfer orchestration.
 */
class TwoNodeIntegrationTest {

    private TcpChunkServer server;
    private Thread serverThread;

    private static void waitForServerBound(TcpChunkServer server, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                server.localPort();
                return;
            } catch (IllegalStateException ignored) {
                Thread.sleep(50);
            }
        }
        throw new AssertionError("Server did not bind within " + timeoutMs + " ms");
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        if (serverThread != null) {
            serverThread.interrupt();
        }
    }

    @Test
    void seederAndLeecher_transferFile(@TempDir Path tmpDir) throws Exception {
        Path srcFile = tmpDir.resolve("source.bin");
        Path outFile = tmpDir.resolve("output.bin");

        byte[] data = new byte[512_000];
        new Random(42).nextBytes(data);
        Files.write(srcFile, data);

        var manifest = new ManifestBuilder(128_000).build(srcFile);

        try (var seederStorage = new SeederFileStorage(srcFile)) {
            BitSet held = new BitSet(manifest.totalChunks());
            held.set(0, manifest.totalChunks());

            server = new TcpChunkServer(0, seederStorage, manifest, held);
            serverThread = Thread.startVirtualThread(() -> {
                try {
                    server.start();
                } catch (IOException ignored) {
                    // closed during tearDown
                }
            });

            waitForServerBound(server, 5_000);
            PeerInfo seederPeer = new PeerInfo(
                    UUID.randomUUID(), new InetSocketAddress("localhost", server.localPort()));

            try (var outStorage = new FileChannelStorage(outFile);
                 var connector = new TcpPeerConnector()) {
                var manager = new TransferManager(manifest, List.of(seederPeer), outStorage, connector);
                manager.start();
            }

            assertThat(Files.readAllBytes(outFile)).isEqualTo(data);
        }
    }

    @Test
    void completedLeecher_servesFileToAnotherPeer(@TempDir Path tmpDir) throws Exception {
        Path srcFile = tmpDir.resolve("source.bin");
        Path leecherFile = tmpDir.resolve("leecher.bin");
        Path thirdFile = tmpDir.resolve("third.bin");

        byte[] data = new byte[256_000];
        new Random(7).nextBytes(data);
        Files.write(srcFile, data);

        var manifest = new ManifestBuilder(64_000).build(srcFile);

        try (var seederStorage = new SeederFileStorage(srcFile)) {
            BitSet seederHeld = new BitSet(manifest.totalChunks());
            seederHeld.set(0, manifest.totalChunks());

            server = new TcpChunkServer(0, seederStorage, manifest, seederHeld);
            serverThread = Thread.startVirtualThread(() -> {
                try {
                    server.start();
                } catch (IOException ignored) {
                }
            });
            waitForServerBound(server, 5_000);
            PeerInfo seederPeer = new PeerInfo(
                    UUID.randomUUID(), new InetSocketAddress("localhost", server.localPort()));

            try (var leecherStorage = new FileChannelStorage(leecherFile);
                 var connector = new TcpPeerConnector()) {
                var leecher = new TransferManager(manifest, List.of(seederPeer), leecherStorage, connector);
                try (var leecherServer = new TcpChunkServer(0, leecherStorage, manifest, leecher.liveHeldChunks())) {
                    Thread leecherThread = Thread.startVirtualThread(() -> {
                        try {
                            leecherServer.start();
                        } catch (IOException ignored) {
                        }
                    });
                    waitForServerBound(leecherServer, 5_000);
                    leecher.start();

                    PeerInfo promoted = new PeerInfo(
                            UUID.randomUUID(), new InetSocketAddress("localhost", leecherServer.localPort()));
                    try (var thirdStorage = new FileChannelStorage(thirdFile);
                         var thirdConnector = new TcpPeerConnector()) {
                        var third = new TransferManager(manifest, List.of(promoted), thirdStorage, thirdConnector);
                        third.start();
                    }
                    leecherServer.close();
                    leecherThread.interrupt();
                }
            }

            assertThat(Files.readAllBytes(leecherFile)).isEqualTo(data);
            assertThat(Files.readAllBytes(thirdFile)).isEqualTo(data);
        }
    }
}
