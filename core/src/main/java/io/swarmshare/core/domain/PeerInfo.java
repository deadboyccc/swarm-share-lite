// core/domain/PeerInfo.java
package io.swarmshare.core.domain;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Identity and address of a swarm participant.
 * UUID is stable across reconnects (read from config or generated once on first run).
 */
public record PeerInfo(UUID id, InetSocketAddress address) {
    public String displayAddress() {
        return address.getHostString() + ":" + address.getPort();
    }
}