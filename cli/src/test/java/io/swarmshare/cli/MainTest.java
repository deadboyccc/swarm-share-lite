package io.swarmshare.cli;

import io.swarmshare.core.domain.PeerInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * In-process tests of the picocli wiring. Full multi-process flows live in the
 * {@code e2eTest} suite.
 */
@Timeout(30)
class MainTest {

    private final StringWriter out = new StringWriter();
    private final StringWriter err = new StringWriter();

    private int run(String... args) {
        CommandLine cli = Main.commandLine();
        cli.setOut(new PrintWriter(out, true));
        cli.setErr(new PrintWriter(err, true));
        return cli.execute(args);
    }

    // ── usage ────────────────────────────────────────────────────────────────────

    @Test
    void noArgs_printsUsageAndExitsZero() {
        assertThat(Main.commandLine().execute()).isZero();
    }

    @Test
    void help_listsEverySubcommand() {
        assertThat(run("--help")).isZero();

        assertThat(out.toString()).contains("seed", "build-manifest", "download");
    }

    @ParameterizedTest
    @ValueSource(strings = {"seed", "build-manifest", "download"})
    void subcommandHelp_exitsZeroWithUsage(String command) {
        assertThat(run(command, "--help")).isZero();

        assertThat(out.toString()).contains("Usage:").contains(command);
    }

    @Test
    void unknownSubcommand_isAUsageError() {
        assertThat(run("leach")).isEqualTo(2);
    }

    // ── build-manifest ───────────────────────────────────────────────────────────

    @Test
    void buildManifest_writesJsonNextToFileAndPrintsSummary(@TempDir Path tmp) throws Exception {
        Path file = Files.write(tmp.resolve("sample.bin"), "hello-swarm".getBytes());

        int code = run("build-manifest", file.toString(), "4");

        assertThat(code).isZero();
        assertThat(tmp.resolve("sample.bin.manifest.json")).isNotEmptyFile();
        assertThat(out.toString()).contains("totalSize=11", "chunkSize=4", "totalChunks=3");
    }

    @Test
    void buildManifest_outputOverride_writesThere(@TempDir Path tmp) throws Exception {
        Path file = Files.write(tmp.resolve("sample.bin"), "hello".getBytes());
        Path target = tmp.resolve("custom/out.json");

        assertThat(run("build-manifest", file.toString(), "--output", target.toString())).isZero();

        assertThat(target).isNotEmptyFile();
        assertThat(tmp.resolve("sample.bin.manifest.json")).doesNotExist();
    }

    @Test
    void buildManifest_missingFile_reportsReasonWithoutStackTrace(@TempDir Path tmp) {
        int code = run("build-manifest", tmp.resolve("absent.bin").toString());

        assertThat(code).isEqualTo(1);
        assertThat(err.toString())
                .startsWith("build-manifest failed: Failed to read file for manifest building")
                .doesNotContain("\tat ");
    }

    @Test
    void buildManifest_emptyFile_isRejected(@TempDir Path tmp) throws Exception {
        Path empty = Files.createFile(tmp.resolve("empty.bin"));

        assertThat(run("build-manifest", empty.toString())).isEqualTo(1);
        assertThat(err.toString()).contains("empty file");
    }

    // ── download ─────────────────────────────────────────────────────────────────

    @Test
    void download_withoutPeers_isAUsageError(@TempDir Path tmp) {
        int code = run("download", tmp.resolve("m.json").toString(), tmp.resolve("out.bin").toString());

        assertThat(code).isEqualTo(2);
        assertThat(err.toString()).contains("Missing required parameter");
    }

    @Test
    void download_invalidManifest_failsWithViolations(@TempDir Path tmp) throws Exception {
        Path manifest = tmp.resolve("bad.manifest.json");
        Files.writeString(manifest, """
                {
                  "fileHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                  "fileName": "x.bin",
                  "totalSize": 100,
                  "chunkSize": 100,
                  "chunks": []
                }
                """);

        int code = run("download", manifest.toString(), tmp.resolve("out.bin").toString(), "127.0.0.1:1");

        assertThat(code).isEqualTo(1);
        assertThat(err.toString()).contains("download failed: Invalid manifest", "at least one chunk");
    }

    @Test
    void download_missingManifest_fails(@TempDir Path tmp) {
        int code = run("download", tmp.resolve("absent.json").toString(), tmp.resolve("o.bin").toString(),
                "127.0.0.1:1");

        assertThat(code).isEqualTo(1);
        assertThat(err.toString()).contains("Failed to read manifest");
    }

    @Test
    void download_badPeerAddress_fails(@TempDir Path tmp) throws Exception {
        Path file = Files.write(tmp.resolve("f.bin"), "content".getBytes());
        assertThat(run("build-manifest", file.toString())).isZero();

        int code = run("download", tmp.resolve("f.bin.manifest.json").toString(),
                tmp.resolve("o.bin").toString(), "no-port-here");

        assertThat(code).isEqualTo(1);
        assertThat(err.toString()).contains("Expected host:port");
    }

    // ── seed ─────────────────────────────────────────────────────────────────────

    @Test
    void seed_portInUse_failsImmediatelyWithTheRealCause(@TempDir Path tmp) throws Exception {
        Path file = Files.write(tmp.resolve("f.bin"), "content".getBytes());
        try (var occupied = new ServerSocket(0)) {
            int code = run("seed", file.toString(), String.valueOf(occupied.getLocalPort()));

            assertThat(code).isEqualTo(1);
            assertThat(err.toString()).startsWith("seed failed:").containsIgnoringCase("address already in use");
        }
    }

    @Test
    void seed_servesUntilInterrupted(@TempDir Path tmp) throws Exception {
        Path file = Files.write(tmp.resolve("f.bin"), "content".getBytes());
        AtomicInteger exit = new AtomicInteger(-1);
        Thread seeder = Thread.ofVirtual().start(() -> exit.set(run("seed", file.toString(), "0")));

        int port = awaitListeningPort();
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", port), 2_000);
            assertThat(socket.isConnected()).isTrue();
        }
        assertThat(tmp.resolve("f.bin.manifest.json")).isNotEmptyFile();

        seeder.interrupt();
        seeder.join(10_000);

        assertThat(seeder.isAlive()).isFalse();
        assertThat(exit).hasValue(0);
    }

    // ── helpers under test ───────────────────────────────────────────────────────

    @Test
    void parsePeer_ipv4HostPort() {
        PeerInfo peer = Main.DownloadCommand.parsePeer("127.0.0.1:9000");

        assertThat(peer.address().getHostString()).isEqualTo("127.0.0.1");
        assertThat(peer.address().getPort()).isEqualTo(9000);
    }

    @Test
    void parsePeer_bracketedIpv6() {
        PeerInfo peer = Main.DownloadCommand.parsePeer("[::1]:8080");

        assertThat(peer.address().getPort()).isEqualTo(8080);
        assertThat(peer.address().getAddress().isLoopbackAddress()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost", "localhost:", ":7070", "localhost:abc"})
    void parsePeer_rejectsMalformedAddresses(String bad) {
        assertThatThrownBy(() -> Main.DownloadCommand.parsePeer(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host:port");
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost:0", "localhost:70000"})
    void parsePeer_rejectsPortOutOfRange(String bad) {
        assertThatThrownBy(() -> Main.DownloadCommand.parsePeer(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Port out of range");
    }

    @Test
    void describe_joinsCauseChainWithoutRepeatingMessages() {
        var failure = new UncheckedIOException("Failed to read x", new IOException("x (No such file)"));

        assertThat(Main.describe(failure)).isEqualTo("Failed to read x: x (No such file)");
    }

    @Test
    void describe_fallsBackToExceptionTypeWithoutMessage() {
        assertThat(Main.describe(new IllegalStateException())).isEqualTo("IllegalStateException");
    }

    private int awaitListeningPort() throws InterruptedException {
        Pattern listening = Pattern.compile("Listening on port (\\d+)");
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (System.nanoTime() < deadline) {
            Matcher m = listening.matcher(out.toString());
            if (m.find()) {
                return Integer.parseInt(m.group(1));
            }
            Thread.sleep(20);
        }
        throw new AssertionError("seed never reported its port; stderr: " + err);
    }
}
