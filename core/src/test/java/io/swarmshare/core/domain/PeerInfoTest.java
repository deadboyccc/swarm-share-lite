package io.swarmshare.core.domain;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PeerInfoTest {

    @Test
    void displayAddress_isHostColonPort() {
        PeerInfo peer = new PeerInfo(UUID.randomUUID(), InetSocketAddress.createUnresolved("seed.lan", 7070));

        assertThat(peer.displayAddress()).isEqualTo("seed.lan:7070");
    }

    @Test
    void displayAddress_keepsLiteralIpWithoutReverseLookup() {
        PeerInfo peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 9000));

        assertThat(peer.displayAddress()).isEqualTo("127.0.0.1:9000");
    }

    @Test
    void identity_isTheUuidAndAddress() {
        UUID id = UUID.randomUUID();
        InetSocketAddress address = InetSocketAddress.createUnresolved("a", 1);

        assertThat(new PeerInfo(id, address)).isEqualTo(new PeerInfo(id, address));
        assertThat(new PeerInfo(id, address)).isNotEqualTo(new PeerInfo(UUID.randomUUID(), address));
    }
}
