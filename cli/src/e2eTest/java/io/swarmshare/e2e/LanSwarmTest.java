package io.swarmshare.e2e;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Roadmap phase 5, "multi-machine test on LAN", with containers as machines: a
 * seeder and two leechers run in separate containers on a private Docker network
 * and find each other by host name, exactly as hosts on a LAN would.
 *
 * <p>
 * The executable under test (JVM distribution or native binary, from the
 * {@code swarm.cli} property) is copied into each container. The image only needs
 * a Linux userland and, for the JVM distribution, a Java 25 runtime.
 */
@Timeout(300)
class LanSwarmTest {

    private static final DockerImageName IMAGE =
            DockerImageName.parse(System.getProperty("swarm.e2e.image", "eclipse-temurin:25.0.5_8-jdk"));
    private static final long CONTAINER_MEMORY_BYTES = 256L * 1024 * 1024;
    private static final int FILE_SIZE = 5 * 1024 * 1024 + 4321; // 6 chunks, short last one

    @BeforeAll
    static void requireDocker() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required for the LAN test");
    }

    @Test
    void seederServesTwoLeechersOnOtherHosts() throws Exception {
        byte[] payload = new byte[FILE_SIZE];
        new Random(2026).nextBytes(payload);
        String expectedSha = sha256(payload);
        Path executable = SwarmCli.executable();
        Path home = installationRoot(executable);
        String command = "/opt/swarm/" + home.relativize(executable);

        try (Network lan = Network.newNetwork();
             GenericContainer<?> seeder = host(lan, "seeder", home)
                     .withCopyToContainer(Transferable.of(payload), "/data/movie.bin")
                     .withCommand(command, "seed", "--file", "/data/movie.bin", "--port", "7070",
                             "--manifest-out", "/data/movie.manifest.json")
                     .waitingFor(Wait.forLogMessage(".*Listening on port 7070.*", 1)
                             .withStartupTimeout(Duration.ofMinutes(2)));
             GenericContainer<?> leecherA = host(lan, "leecher-a", home).withCommand("sleep", "infinity");
             GenericContainer<?> leecherB = host(lan, "leecher-b", home).withCommand("sleep", "infinity")) {

            seeder.start();
            leecherA.start();
            leecherB.start();
            // Peers share the manifest out of band; here it is copied host to host.
            byte[] manifest = seeder.copyFileFromContainer("/data/movie.manifest.json", in -> in.readAllBytes());
            leecherA.copyFileToContainer(Transferable.of(manifest), "/data/movie.manifest.json");
            leecherB.copyFileToContainer(Transferable.of(manifest), "/data/movie.manifest.json");

            ExecResult a = leecherA.execInContainer(command, "leech", "--manifest", "/data/movie.manifest.json",
                    "--peer", "seeder:7070", "--output", "/downloads/movie.bin");
            // leecher-b also lists a host that does not exist: it must be skipped.
            ExecResult b = leecherB.execInContainer(command, "leech", "--manifest", "/data/movie.manifest.json",
                    "--peer", "no-such-host:7070", "seeder:7070", "--output", "/downloads/movie.bin");

            assertThat(a.getExitCode()).as(describe(a)).isZero();
            assertThat(b.getExitCode()).as(describe(b)).isZero();
            assertThat(a.getStdout()).contains("Download complete: /downloads/movie.bin");
            assertThat(sha256Of(leecherA, "/downloads/movie.bin")).isEqualTo(expectedSha);
            assertThat(sha256Of(leecherB, "/downloads/movie.bin")).isEqualTo(expectedSha);
        }
    }

    /** A container on {@code lan}, reachable by {@code name}, with the CLI installed under /opt/swarm. */
    private static GenericContainer<?> host(Network lan, String name, Path home) {
        return new GenericContainer<>(IMAGE)
                .withNetwork(lan)
                .withNetworkAliases(name)
                .withCopyFileToContainer(MountableFile.forHostPath(home, 0755), "/opt/swarm")
                .withEnv("JAVA_OPTS", "-Xmx64m -XX:+UseSerialGC -XX:TieredStopAtLevel=1")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withMemory(CONTAINER_MEMORY_BYTES)
                        .withMemorySwap(CONTAINER_MEMORY_BYTES)
                        // On a shared host, these containers are the first the kernel kills.
                        .withOomScoreAdj(1000));
    }

    /**
     * The directory to copy into the container: the distribution root for the JVM
     * start script ({@code <root>/bin/swarm-share}), the binary's own directory otherwise.
     */
    private static Path installationRoot(Path executable) {
        Path parent = executable.getParent();
        return parent.getFileName().toString().equals("bin") ? parent.getParent() : parent;
    }

    private static String sha256Of(GenericContainer<?> container, String path) throws Exception {
        byte[] bytes = container.copyFileFromContainer(path, in -> in.readAllBytes());
        return sha256(bytes);
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static String describe(ExecResult result) {
        return "exit=" + result.getExitCode() + "\nstdout:\n" + result.getStdout() + "\nstderr:\n" + result.getStderr();
    }
}
