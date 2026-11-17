package io.swarmshare.cli;

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

/**
 * In-process tests of the picocli wiring and of each command's argument handling.
 * Full multi-process flows live in the {@code e2eTest} suite.
 */
@Timeout(30)
class SwarmShareCliTest {

    private final StringWriter out = new StringWriter();
    private final StringWriter err = new StringWriter();

    @TempDir
    Path tmp;

    private int run(String... args) {
        CommandLine cli = SwarmShareCli.commandLine();
        cli.setOut(new PrintWriter(out, true));
        cli.setErr(new PrintWriter(err, true));
        return cli.execute(args);
    }

    // ── usage ────────────────────────────────────────────────────────────────────

    @Test
    void noArgs_printsUsageAndExitsZero() {
        assertThat(run()).isZero();

        assertThat(out.toString()).contains("Usage: swarm-share");
    }

    @Test
    void help_listsEverySubcommand() {
        assertThat(run("--help")).isZero();

        assertThat(out.toString()).contains("seed", "leech", "build-manifest", "download", "help");
    }

    @ParameterizedTest
    @ValueSource(strings = {"seed", "leech", "build-manifest", "download"})
    void subcommandHelp_exitsZeroWithUsage(String command) {
        assertThat(run(command, "--help")).isZero();

        assertThat(out.toString()).contains("Usage: swarm-share " + command);
    }

    @Test
    void helpSubcommand_printsUsageOfTheNamedCommand() {
        assertThat(run("help", "leech")).isZero();

        assertThat(out.toString()).contains("--manifest", "--peer", "--output");
    }

    @Test
    void unknownSubcommand_isAUsageError() {
        assertThat(run("leach")).isEqualTo(2);
    }

    // ── build-manifest ───────────────────────────────────────────────────────────

    @Test
    void buildManifest_writesJsonNextToFileAndPrintsSummary() throws Exception {
        Path file = Files.write(tmp.resolve("sample.bin"), "hello-swarm".getBytes());

        assertThat(run("build-manifest", file.toString(), "4")).isZero();

        assertThat(tmp.resolve("sample.bin.manifest.json")).isNotEmptyFile();
        assertThat(out.toString()).contains("totalSize=11", "chunkSize=4", "totalChunks=3");
    }

    @Test
    void buildManifest_outputOverride_writesThere() throws Exception {
        Path file = Files.write(tmp.resolve("sample.bin"), "hello".getBytes());
        Path target = tmp.resolve("custom/out.json");

        assertThat(run("build-manifest", file.toString(), "--output", target.toString())).isZero();

        assertThat(target).isNotEmptyFile();
        assertThat(tmp.resolve("sample.bin.manifest.json")).doesNotExist();
    }

    @Test
    void buildManifest_missingFile_reportsReasonWithoutStackTrace() {
        assertThat(run("build-manifest", tmp.resolve("absent.bin").toString())).isEqualTo(1);

        assertThat(err.toString())
                .startsWith("build-manifest failed: Failed to read file for manifest building")
                .doesNotContain("\tat ");
    }

    @Test
    void buildManifest_emptyFile_isRejected() throws Exception {
        Path empty = Files.createFile(tmp.resolve("empty.bin"));

        assertThat(run("build-manifest", empty.toString())).isEqualTo(1);
        assertThat(err.toString()).contains("empty file");
    }

    @Test
    void buildManifest_chunkSizeAboveMaximum_isRejected() throws Exception {
        Path file = Files.write(tmp.resolve("f.bin"), "x".getBytes());

        assertThat(run("build-manifest", file.toString(), String.valueOf(64 * 1024 * 1024))).isEqualTo(1);
        assertThat(err.toString()).contains("chunkSize must be <=");
    }

    // ── leech / download argument handling ───────────────────────────────────────

    @Test
    void leech_requiresManifestPeerAndOutput() {
        assertThat(run("leech", "--manifest", "m.json")).isEqualTo(2);

        assertThat(err.toString()).contains("Missing required options", "--output", "--peer");
    }

    @Test
    void download_withoutPeers_isAUsageError() {
        int code = run("download", tmp.resolve("m.json").toString(), tmp.resolve("out.bin").toString());

        assertThat(code).isEqualTo(2);
        assertThat(err.toString()).contains("Missing required parameter");
    }

    @ParameterizedTest
    @ValueSource(strings = {"leech", "download"})
    void invalidManifest_failsWithViolationsBeforeCreatingOutput(String command) throws Exception {
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
        Path output = tmp.resolve("out.bin");

        int code = command.equals("leech")
                ? run("leech", "--manifest", manifest.toString(), "--peer", "127.0.0.1:1", "--output", output.toString())
                : run("download", manifest.toString(), output.toString(), "127.0.0.1:1");

        assertThat(code).isEqualTo(1);
        assertThat(err.toString()).contains(command + " failed: Invalid manifest", "at least one chunk");
        assertThat(output).doesNotExist();
    }

    @Test
    void leech_missingManifest_fails() {
        int code = run("leech", "--manifest", tmp.resolve("absent.json").toString(),
                "--peer", "127.0.0.1:1", "--output", tmp.resolve("o.bin").toString());

        assertThat(code).isEqualTo(1);
        assertThat(err.toString()).contains("Failed to read manifest");
    }

    @Test
    void leech_badPeerAddress_fails() throws Exception {
        Path file = Files.write(tmp.resolve("f.bin"), "content".getBytes());
        assertThat(run("build-manifest", file.toString())).isZero();

        int code = run("leech", "--manifest", tmp.resolve("f.bin.manifest.json").toString(),
                "--peer", "no-port-here", "--output", tmp.resolve("o.bin").toString());

        assertThat(code).isEqualTo(1);
        assertThat(err.toString()).contains("Expected host:port");
    }

    @Test
    void leech_downloadsFromInProcessSeeder() throws Exception {
        Path source = Files.write(tmp.resolve("f.bin"), "swarm payload".repeat(1000).getBytes());
        Thread seeder = Thread.ofVirtual().start(() -> run("seed", "--file", source.toString(), "--port", "0"));
        int port = awaitListeningPort();
        Path output = tmp.resolve("copy.bin");

        int code = run("leech", "--manifest", tmp.resolve("f.bin.manifest.json").toString(),
                "--peer", "127.0.0.1:" + port, "--output", output.toString());

        assertThat(code).as(err.toString()).isZero();
        assertThat(out.toString()).contains("Download complete: " + output);
        assertThat(Files.readAllBytes(output)).isEqualTo(Files.readAllBytes(source));
        seeder.interrupt();
        seeder.join(10_000);
    }

    // ── seed ─────────────────────────────────────────────────────────────────────

    @Test
    void seed_withOptions_printsManifestSummaryAndServesUntilInterrupted() throws Exception {
        Path file = Files.write(tmp.resolve("f.bin"), "content".getBytes());
        Path manifestOut = tmp.resolve("shared/f.json");
        AtomicInteger exit = new AtomicInteger(-1);
        Thread seeder = Thread.ofVirtual().start(() -> exit.set(
                run("seed", "--file", file.toString(), "--port", "0", "--manifest-out", manifestOut.toString())));

        int port = awaitListeningPort();
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", port), 2_000);
        }

        assertThat(out.toString()).contains("Manifest hash: ", "Total chunks:  1", "Manifest file: " + manifestOut);
        assertThat(manifestOut).isNotEmptyFile();
        assertThat(tmp.resolve("f.bin.manifest.json")).doesNotExist();

        seeder.interrupt();
        seeder.join(10_000);
        assertThat(seeder.isAlive()).isFalse();
        assertThat(exit).hasValue(0);
    }

    @Test
    void seed_positionalForm_stillWorks() throws Exception {
        Path file = Files.write(tmp.resolve("f.bin"), "content".getBytes());
        Thread seeder = Thread.ofVirtual().start(() -> run("seed", file.toString(), "0"));

        assertThat(awaitListeningPort()).isPositive();
        assertThat(tmp.resolve("f.bin.manifest.json")).isNotEmptyFile();

        seeder.interrupt();
        seeder.join(10_000);
    }

    @Test
    void seed_withoutFile_isAUsageError() {
        assertThat(run("seed", "--port", "0")).isEqualTo(2);

        assertThat(err.toString()).contains("Missing required option: '--file=<file>'");
    }

    @Test
    void seed_conflictingFileValues_isAUsageError() throws Exception {
        Path a = Files.write(tmp.resolve("a.bin"), "a".getBytes());

        assertThat(run("seed", "--file", a.toString(), tmp.resolve("b.bin").toString())).isEqualTo(2);
        assertThat(err.toString()).contains("Conflicting values for file");
    }

    @Test
    void seed_portInUse_failsImmediatelyWithTheRealCause() throws Exception {
        Path file = Files.write(tmp.resolve("f.bin"), "content".getBytes());
        try (var occupied = new ServerSocket(0)) {
            int code = run("seed", "--file", file.toString(), "--port", String.valueOf(occupied.getLocalPort()));

            assertThat(code).isEqualTo(1);
            assertThat(err.toString()).startsWith("seed failed:").containsIgnoringCase("address already in use");
        }
    }

    // ── error descriptions ───────────────────────────────────────────────────────

    @Test
    void describe_joinsCauseChainWithoutRepeatingMessages() {
        var failure = new UncheckedIOException("Failed to read x", new IOException("x (No such file)"));

        assertThat(SwarmShareCli.describe(failure)).isEqualTo("Failed to read x: x (No such file)");
    }

    @Test
    void describe_fallsBackToExceptionTypeWithoutMessage() {
        assertThat(SwarmShareCli.describe(new IllegalStateException())).isEqualTo("IllegalStateException");
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
