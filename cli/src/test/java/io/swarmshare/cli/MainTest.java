package io.swarmshare.cli;

import io.swarmshare.core.domain.PeerInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MainTest {

    @Test
    void noArgs_printsUsageAndExitsZero() {
        int code = new CommandLine(new Main()).execute();
        assertThat(code).isEqualTo(0);
    }

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

    @Test
    void parsePeer_rejectsMissingPort() {
        assertThatThrownBy(() -> Main.DownloadCommand.parsePeer("localhost"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host:port");
    }

    @Test
    void parsePeer_rejectsNonNumericPort() {
        assertThatThrownBy(() -> Main.DownloadCommand.parsePeer("localhost:abc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host:port");
    }

    @Test
    void parsePeer_rejectsPortOutOfRange() {
        assertThatThrownBy(() -> Main.DownloadCommand.parsePeer("localhost:70000"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Port out of range");
    }

    @Test
    void buildManifest_writesJsonNextToFile(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("sample.bin");
        Files.write(file, "hello-swarm".getBytes());

        int code = new CommandLine(new Main()).execute(
                "build-manifest", file.toAbsolutePath().toString(), "4");

        assertThat(code).isZero();
        assertThat(tmp.resolve("sample.bin.manifest.json")).exists();
    }

    @Test
    void download_invalidManifest_nonzeroExit(@TempDir Path tmp) throws Exception {
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
        Path out = tmp.resolve("out.bin");

        int code = new CommandLine(new Main()).execute(
                "download",
                manifest.toAbsolutePath().toString(),
                out.toAbsolutePath().toString(),
                "127.0.0.1:1");

        assertThat(code).isNotZero();
    }
}
