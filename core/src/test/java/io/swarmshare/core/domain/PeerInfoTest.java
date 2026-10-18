package io.swarmshare.core.domain;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PeerInfoTest {

    @Test
    void displayAddress_usesHostAndPort() {
        var peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 7070));
        assertThat(peer.displayAddress()).isEqualTo("127.0.0.1:7070");
    }

    @Test
    void rejectsNullIdOrAddress() {
        var addr = new InetSocketAddress("localhost", 1);
        assertThatThrownBy(() -> new PeerInfo(null, addr))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new PeerInfo(UUID.randomUUID(), null))
                .isInstanceOf(NullPointerException.class);
    }
}
