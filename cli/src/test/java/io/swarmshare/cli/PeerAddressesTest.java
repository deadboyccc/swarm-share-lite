package io.swarmshare.cli;

import io.swarmshare.core.domain.PeerInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PeerAddressesTest {

    @Test
    void ipv4HostPort() {
        PeerInfo peer = PeerAddresses.parse("127.0.0.1:9000");

        assertThat(peer.address().getHostString()).isEqualTo("127.0.0.1");
        assertThat(peer.address().getPort()).isEqualTo(9000);
    }

    @Test
    void hostName_isResolvedSoItCanBeConnectedTo() {
        PeerInfo peer = PeerAddresses.parse("localhost:7070");

        assertThat(peer.address().isUnresolved()).isFalse();
        assertThat(peer.displayAddress()).isEqualTo("localhost:7070");
    }

    @Test
    void bracketedIpv6() {
        PeerInfo peer = PeerAddresses.parse("[::1]:8080");

        assertThat(peer.address().getPort()).isEqualTo(8080);
        assertThat(peer.address().getAddress().isLoopbackAddress()).isTrue();
    }

    @Test
    void eachParsedPeer_getsItsOwnIdentity() {
        assertThat(PeerAddresses.parse("127.0.0.1:1").id()).isNotEqualTo(PeerAddresses.parse("127.0.0.1:1").id());
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost", "localhost:", ":7070", "localhost:abc"})
    void malformedAddresses_areRejected(String bad) {
        assertThatThrownBy(() -> PeerAddresses.parse(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host:port");
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost:0", "localhost:70000", "localhost:-1"})
    void portOutOfRange_isRejected(String bad) {
        assertThatThrownBy(() -> PeerAddresses.parse(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Port out of range");
    }
}
