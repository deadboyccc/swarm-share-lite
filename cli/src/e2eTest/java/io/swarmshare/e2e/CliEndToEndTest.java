package io.swarmshare.e2e;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The main user flows from the README and ARCHITECTURE.md §13, run against the
 * packaged executable on loopback: build a manifest, seed a file, leech it from one
 * or more peers, resume a partial download, and fail clearly when the swarm cannot
 * help. {@link LanSwarmTest} runs the same flows across separate hosts.
 */
@Timeout(120)
class CliEndToEndTest {

    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(60);
    private static final int MIB = 1024 * 1024;
    /**
     * Port 1 (tcpmux) has no listener on any normal host. A just-released ephemeral
     * port would not do: another test may bind it again before it is used.
     */
    private static final String DEAD_PEER = "127.0.0.1:1";

    @TempDir
    Path dir;

    private SwarmCli cli;

    @BeforeEach
    void setUp() {
        cli = new SwarmCli(dir);
    }

    @Test
    void help_listsCommands() throws Exception {
        SwarmCli.Result result = cli.run(COMMAND_TIMEOUT, "--help");

        assertThat(result.exitCode()).as(result.toString()).isZero();
        assertThat(result.stdout()).contains("seed", "leech", "build-manifest", "download", "help");
    }

    @Test
    void seedThenLeech_withSpecOptions_reproducesTheFileByteForByte() throws Exception {
        Path source = randomFile("ubuntu.iso", 3 * MIB + 77);
        Path manifest = dir.resolve("shared/ubuntu.manifest.json");
        Path output = dir.resolve("downloads/ubuntu.iso");

        try (SwarmCli.Running seeder = cli.start("seed", "--file", source.toString(), "--port", "0",
                "--manifest-out", manifest.toString())) {
            int port = seeder.awaitListeningPort(COMMAND_TIMEOUT);
            assertThat(seeder.stdout()).contains("Manifest hash: ", "Total chunks:  4");

            SwarmCli.Result leech = cli.run(COMMAND_TIMEOUT, "leech", "--manifest", manifest.toString(),
                    "--peer", "127.0.0.1:" + port, "--output", output.toString());

            assertThat(leech.exitCode()).as(leech.toString()).isZero();
            assertThat(leech.stdout()).contains("Download complete: " + output);
        }
        assertThat(Files.readAllBytes(output)).isEqualTo(Files.readAllBytes(source));
    }

    @Test
    void leech_fromTwoSeeders_succeedsWhenTheFirstOneStops() throws Exception {
        Path source = randomFile("pair.bin", 2 * MIB);
        Path manifest = dir.resolve("pair.bin.manifest.json");
        Path output = dir.resolve("pair.out");

        try (SwarmCli.Running first = cli.start("seed", "--file", source.toString(), "--port", "0");
             SwarmCli.Running second = cli.start("seed", "--file", source.toString(), "--port", "0")) {
            int firstPort = first.awaitListeningPort(COMMAND_TIMEOUT);
            int secondPort = second.awaitListeningPort(COMMAND_TIMEOUT);
            first.stop();

            SwarmCli.Result leech = cli.run(COMMAND_TIMEOUT, "leech", "--manifest", manifest.toString(),
                    "--peer", "127.0.0.1:" + firstPort, "127.0.0.1:" + secondPort, "--output", output.toString());

            assertThat(leech.exitCode()).as(leech.toString()).isZero();
        }
        assertThat(Files.readAllBytes(output)).isEqualTo(Files.readAllBytes(source));
    }

    @Test
    void leech_bansSeederServingCorruptDataAndCompletesFromHonestOne() throws Exception {
        Path tampered = randomFile("doc.bin", 4 * MIB); // 4 chunks
        Path honest = Files.copy(tampered, Files.createDirectories(dir.resolve("honest")).resolve("doc.bin"));
        Path manifest = dir.resolve("doc.manifest.json");
        Path output = dir.resolve("doc.out");

        try (SwarmCli.Running corrupt = cli.start("seed", "--file", tampered.toString(), "--port", "0",
                     "--manifest-out", manifest.toString());
             SwarmCli.Running good = cli.start("seed", "--file", honest.toString(), "--port", "0",
                     "--manifest-out", dir.resolve("honest.manifest.json").toString())) {
            int corruptPort = corrupt.awaitListeningPort(COMMAND_TIMEOUT);
            int goodPort = good.awaitListeningPort(COMMAND_TIMEOUT);
            // After the manifest is published, flip one byte in every chunk on disk:
            // the first seeder now serves data that fails verification.
            try (var channel = FileChannel.open(tampered, StandardOpenOption.WRITE)) {
                for (long offset = 0; offset < 4L * MIB; offset += MIB) {
                    channel.write(ByteBuffer.wrap(new byte[]{(byte) 0xFF}), offset);
                }
            }

            SwarmCli.Result leech = cli.run(COMMAND_TIMEOUT, "leech", "--manifest", manifest.toString(),
                    "--peer", "127.0.0.1:" + corruptPort, "127.0.0.1:" + goodPort, "--output", output.toString());

            assertThat(leech.exitCode()).as(leech.toString()).isZero();
            assertThat(leech.stderr()).contains("Banned peer 127.0.0.1:" + corruptPort
                    + " after 3 chunks failed checksum verification");
        }
        assertThat(Files.readAllBytes(output)).isEqualTo(Files.readAllBytes(honest));
    }

    @Test
    void leech_fromOnlyACorruptSeeder_failsNamingTheBannedPeer() throws Exception {
        Path tampered = randomFile("bad.bin", 4 * MIB);
        Path manifest = dir.resolve("bad.manifest.json");

        try (SwarmCli.Running corrupt = cli.start("seed", "--file", tampered.toString(), "--port", "0",
                "--manifest-out", manifest.toString())) {
            int port = corrupt.awaitListeningPort(COMMAND_TIMEOUT);
            try (var channel = FileChannel.open(tampered, StandardOpenOption.WRITE)) {
                for (long offset = 0; offset < 4L * MIB; offset += MIB) {
                    channel.write(ByteBuffer.wrap(new byte[]{(byte) 0xFF}), offset);
                }
            }

            SwarmCli.Result leech = cli.run(COMMAND_TIMEOUT, "leech", "--manifest", manifest.toString(),
                    "--peer", "127.0.0.1:" + port, "--output", dir.resolve("bad.out").toString());

            assertThat(leech.exitCode()).isEqualTo(1);
            assertThat(leech.stderr()).contains("leech failed: Transfer incomplete: 4 of 4 chunks missing",
                    "banned for corrupt data: 127.0.0.1:" + port);
        }
    }

    @Test
    void buildManifest_writesManifestAndPrintsSummary() throws Exception {
        Path file = randomFile("data.bin", 2 * MIB + 17);

        SwarmCli.Result result = cli.run(COMMAND_TIMEOUT, "build-manifest", file.toString());

        assertThat(result.exitCode()).as(result.toString()).isZero();
        assertThat(result.stdout()).contains("totalSize=" + (2 * MIB + 17), "totalChunks=3");
        assertThat(Files.readString(dir.resolve("data.bin.manifest.json")))
                .contains("\"fileHash\"", "\"chunks\"", "\"sha256\"");
    }

    @Test
    void seedThenDownload_positionalFormStillWorks() throws Exception {
        Path source = randomFile("movie.bin", 3 * MIB + 123);
        Path output = dir.resolve("downloads/movie.bin");

        try (SwarmCli.Running seeder = cli.start("seed", source.toString(), "0")) {
            int port = seeder.awaitListeningPort(COMMAND_TIMEOUT);

            SwarmCli.Result download = cli.run(COMMAND_TIMEOUT, "download",
                    dir.resolve("movie.bin.manifest.json").toString(), output.toString(), "127.0.0.1:" + port);

            assertThat(download.exitCode()).as(download.toString()).isZero();
            assertThat(download.stdout()).contains("Download complete: " + output);
        }
        assertThat(Files.readAllBytes(output)).isEqualTo(Files.readAllBytes(source));
    }

    @Test
    void download_skipsDeadPeerAndUsesLiveOne() throws Exception {
        Path source = randomFile("iso.bin", 2 * MIB);
        Path output = dir.resolve("iso.out");

        try (SwarmCli.Running seeder = cli.start("seed", source.toString(), "0")) {
            int port = seeder.awaitListeningPort(COMMAND_TIMEOUT);

            SwarmCli.Result download = cli.run(COMMAND_TIMEOUT, "download",
                    dir.resolve("iso.bin.manifest.json").toString(), output.toString(),
                    DEAD_PEER, "127.0.0.1:" + port);

            assertThat(download.exitCode()).as(download.toString()).isZero();
        }
        assertThat(Files.readAllBytes(output)).isEqualTo(Files.readAllBytes(source));
    }

    @Test
    void download_resumesPartialFileAndRepairsCorruptChunks() throws Exception {
        Path source = randomFile("big.bin", 3 * MIB);
        byte[] original = Files.readAllBytes(source);
        // Previous run: chunk 0 finished, chunk 1 torn (garbage), chunk 2 never written.
        byte[] partial = new byte[original.length];
        System.arraycopy(original, 0, partial, 0, MIB);
        Arrays.fill(partial, MIB, MIB + 1000, (byte) 0x5A);
        Path output = Files.write(dir.resolve("big.out"), partial);

        try (SwarmCli.Running seeder = cli.start("seed", source.toString(), "0")) {
            int port = seeder.awaitListeningPort(COMMAND_TIMEOUT);

            SwarmCli.Result download = cli.run(COMMAND_TIMEOUT, "download",
                    dir.resolve("big.bin.manifest.json").toString(), output.toString(), "127.0.0.1:" + port);

            assertThat(download.exitCode()).as(download.toString()).isZero();
            assertThat(download.stderr()).contains("Resuming: 1 of 3 chunks already on disk");
        }
        assertThat(Files.readAllBytes(output)).isEqualTo(original);
    }

    @Test
    void download_withNoReachablePeer_failsWithClearReason() throws Exception {
        Path source = randomFile("lonely.bin", MIB / 2);
        assertThat(cli.run(COMMAND_TIMEOUT, "build-manifest", source.toString()).exitCode()).isZero();

        SwarmCli.Result download = cli.run(COMMAND_TIMEOUT, "download",
                dir.resolve("lonely.bin.manifest.json").toString(), dir.resolve("lonely.out").toString(),
                DEAD_PEER);

        assertThat(download.exitCode()).isEqualTo(1);
        assertThat(download.stderr()).contains("download failed: Transfer incomplete: 1 of 1 chunks missing");
    }

    @Test
    void download_tamperedManifest_isRejectedBeforeAnyTransfer() throws Exception {
        Path source = randomFile("t.bin", 1000);
        assertThat(cli.run(COMMAND_TIMEOUT, "build-manifest", source.toString()).exitCode()).isZero();
        Path manifest = dir.resolve("t.bin.manifest.json");
        Files.writeString(manifest, Files.readString(manifest).replace("\"totalSize\" : 1000", "\"totalSize\" : 999"));

        SwarmCli.Result download = cli.run(COMMAND_TIMEOUT, "download", manifest.toString(),
                dir.resolve("t.out").toString(), DEAD_PEER);

        assertThat(download.exitCode()).isEqualTo(1);
        assertThat(download.stderr()).contains("Invalid manifest", "does not match totalSize");
        assertThat(dir.resolve("t.out")).doesNotExist();
    }

    @Test
    void seed_onBusyPort_exitsWithError() throws Exception {
        Path source = randomFile("busy.bin", 100);
        try (var occupied = new ServerSocket(0)) {
            SwarmCli.Result seed = cli.run(COMMAND_TIMEOUT, "seed", source.toString(),
                    String.valueOf(occupied.getLocalPort()));

            assertThat(seed.exitCode()).isEqualTo(1);
            assertThat(seed.stderr()).containsIgnoringCase("seed failed: address already in use");
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private Path randomFile(String name, int size) throws IOException {
        byte[] bytes = new byte[size];
        new Random(size).nextBytes(bytes);
        return Files.write(dir.resolve(name), bytes);
    }

}
