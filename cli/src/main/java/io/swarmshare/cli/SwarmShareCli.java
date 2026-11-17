package io.swarmshare.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Entry point of the {@code swarm-share} command line.
 *
 * <p>
 * Commands:
 * <ul>
 * <li>{@link SeedCommand seed} — build a manifest for a file and serve it to peers</li>
 * <li>{@link LeechCommand leech} — download a file described by a manifest from peers</li>
 * <li>{@link BuildManifestCommand build-manifest} — write a manifest without serving</li>
 * <li>{@link DownloadCommand download} — {@code leech} with positional arguments</li>
 * <li>{@code help} — usage of any command</li>
 * </ul>
 *
 * <p>
 * Exit codes: {@code 0} success, {@code 1} the command failed (one-line reason on
 * stderr), {@code 2} invalid usage.
 */
@Command(name = "swarm-share",
        description = "P2P chunk-based file sharing",
        subcommands = {
                SeedCommand.class,
                LeechCommand.class,
                BuildManifestCommand.class,
                DownloadCommand.class,
                CommandLine.HelpCommand.class
        })
public final class SwarmShareCli implements Runnable {

    /**
     * One line per record ({@code [LEVEL] message}) instead of JUL's default
     * two-line, timestamped layout, which buries the message in CLI output.
     */
    private static final String LOG_FORMAT = "[%4$s] %5$s%6$s%n";

    @Mixin
    private HelpOption help;

    @Spec
    private CommandSpec spec;

    public static void main(String[] args) {
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null) {
            System.setProperty("java.util.logging.SimpleFormatter.format", LOG_FORMAT);
        }
        System.exit(commandLine().execute(args));
    }

    /**
     * Builds the configured command line. Failures inside a command are reported as a
     * single {@code "<command> failed: <reason>"} line on stderr with exit code 1 —
     * the reason, not a stack trace, is what a CLI user can act on.
     */
    static CommandLine commandLine() {
        CommandLine cli = new CommandLine(new SwarmShareCli());
        cli.setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
            commandLine.getErr().println(commandLine.getCommandName() + " failed: " + describe(ex));
            return 1;
        });
        return cli;
    }

    /** Joins the messages of an exception chain, skipping wrappers that only repeat their cause. */
    static String describe(Throwable failure) {
        StringBuilder text = new StringBuilder();
        for (Throwable t = failure; t != null; t = t.getCause()) {
            String message = t.getMessage();
            if (message == null || message.isBlank()) {
                message = t.getClass().getSimpleName();
            }
            if (!text.toString().contains(message)) {
                if (!text.isEmpty()) {
                    text.append(": ");
                }
                text.append(message);
            }
        }
        return text.toString();
    }

    @Override
    public void run() {
        // No subcommand given — print usage instead of doing nothing silently.
        spec.commandLine().usage(spec.commandLine().getOut());
    }
}
