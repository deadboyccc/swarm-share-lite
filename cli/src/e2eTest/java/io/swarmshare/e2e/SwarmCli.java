package io.swarmshare.e2e;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Launches the packaged {@code swarm-share} executable as a separate OS process.
 *
 * <p>
 * The executable comes from the {@code swarm.cli} system property, set by Gradle to
 * either the JVM distribution's start script or the GraalVM native binary. Output is
 * redirected to files rather than pipes so a chatty process can never block on a
 * full pipe buffer while the test waits for it.
 */
final class SwarmCli {

    private static final Pattern LISTENING = Pattern.compile("Listening on port (\\d+)");

    /** Keeps each JVM child small; ignored by the native binary. */
    private static final String CHILD_JVM_OPTS = "-Xmx64m -XX:+UseSerialGC -XX:TieredStopAtLevel=1";

    private final Path workDir;

    SwarmCli(Path workDir) {
        this.workDir = workDir;
    }

    static Path executable() {
        String configured = System.getProperty("swarm.cli");
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException("System property swarm.cli is not set; run via Gradle");
        }
        Path path = Path.of(configured);
        if (!Files.isExecutable(path)) {
            throw new IllegalStateException("swarm.cli is not an executable file: " + path);
        }
        return path;
    }

    /** Runs a command to completion. */
    Result run(Duration timeout, String... args) throws IOException, InterruptedException {
        Running running = start(args);
        if (!running.process().waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            running.stop();
            throw new AssertionError("swarm-share " + String.join(" ", args) + " timed out after "
                    + timeout + "\nstdout:\n" + running.stdout() + "\nstderr:\n" + running.stderr());
        }
        return new Result(running.process().exitValue(), running.stdout(), running.stderr());
    }

    /** Starts a long-running command (e.g. {@code seed}) in the background. */
    Running start(String... args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(executable().toString());
        command.addAll(List.of(args));
        Path logs = Files.createTempDirectory(workDir, "proc-");
        Path out = logs.resolve("stdout.txt");
        Path err = logs.resolve("stderr.txt");
        ProcessBuilder builder = new ProcessBuilder(command)
                .directory(workDir.toFile())
                .redirectOutput(out.toFile())
                .redirectError(err.toFile());
        builder.environment().put("JAVA_OPTS", CHILD_JVM_OPTS);
        return new Running(builder.start(), out, err);
    }

    record Result(int exitCode, String stdout, String stderr) {
        @Override
        public String toString() {
            return "exit=" + exitCode + "\nstdout:\n" + stdout + "\nstderr:\n" + stderr;
        }
    }

    record Running(Process process, Path out, Path err) implements AutoCloseable {

        String stdout() throws IOException {
            return Files.readString(out);
        }

        String stderr() throws IOException {
            return Files.readString(err);
        }

        /** Blocks until the process prints its listening port, then returns it. */
        int awaitListeningPort(Duration timeout) throws IOException, InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                Matcher m = LISTENING.matcher(stdout());
                if (m.find()) {
                    return Integer.parseInt(m.group(1));
                }
                if (!process.isAlive()) {
                    throw new AssertionError("process exited with " + process.exitValue()
                            + " before listening\nstderr:\n" + stderr());
                }
                Thread.sleep(25);
            }
            throw new AssertionError("no 'Listening on port' line within " + timeout + "\nstderr:\n" + stderr());
        }

        void stop() throws InterruptedException {
            process.destroy();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly().waitFor();
            }
        }

        @Override
        public void close() throws InterruptedException {
            stop();
        }
    }
}
