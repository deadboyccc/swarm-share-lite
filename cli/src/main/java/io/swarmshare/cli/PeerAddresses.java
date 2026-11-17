package io.swarmshare.cli;

import io.swarmshare.core.domain.PeerInfo;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Parses {@code host:port} peer addresses given on the command line.
 */
final class PeerAddresses {

    private PeerAddresses() {
    }

    /**
     * Parses {@code host:port}, {@code a.b.c.d:port} or {@code [ipv6]:port}.
     * The host is resolved here because {@code Socket.connect} refuses unresolved
     * addresses; a name that does not resolve yields an unresolved address, and that
     * peer is then skipped at download time like any unreachable one.
     *
     * @throws IllegalArgumentException if the address is malformed or the port is outside 1–65535
     */
    static PeerInfo parse(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        if (colon <= 0 || colon == hostPort.length() - 1) {
            throw new IllegalArgumentException("Expected host:port, got: " + hostPort);
        }
        String host = hostPort.substring(0, colon);
        if (host.startsWith("[") && host.endsWith("]") && host.length() > 2) {
            host = host.substring(1, host.length() - 1);
        }
        int port;
        try {
            port = Integer.parseInt(hostPort.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected host:port, got: " + hostPort, e);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port out of range: " + port);
        }
        return new PeerInfo(UUID.randomUUID(), new InetSocketAddress(host, port));
    }
}
