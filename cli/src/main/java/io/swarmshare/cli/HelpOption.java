package io.swarmshare.cli;

import picocli.CommandLine.Option;

/**
 * Adds {@code -h/--help} to a command. A mixin rather than
 * {@code mixinStandardHelpOptions} because the CLI has no version flag.
 */
final class HelpOption {

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    boolean helpRequested;
}
