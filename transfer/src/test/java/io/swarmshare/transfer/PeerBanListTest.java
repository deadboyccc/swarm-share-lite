package io.swarmshare.transfer;

import io.swarmshare.core.domain.PeerInfo;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PeerBanListTest {

    private final PeerInfo peer = new PeerInfo(UUID.randomUUID(), InetSocketAddress.createUnresolved("a", 1));
    private final PeerInfo other = new PeerInfo(UUID.randomUUID(), InetSocketAddress.createUnresolved("b", 1));

    @Test
    void peer_isBannedOnTheThirdChecksumFailureByDefault() {
        PeerBanList bans = new PeerBanList();

        assertThat(bans.recordChecksumFailure(peer)).isFalse();
        assertThat(bans.recordChecksumFailure(peer)).isFalse();
        assertThat(bans.isBanned(peer)).isFalse();
        assertThat(bans.recordChecksumFailure(peer)).as("third strike bans").isTrue();

        assertThat(bans.isBanned(peer)).isTrue();
        assertThat(bans.banned()).containsExactly(peer);
    }

    @Test
    void failuresAfterTheBan_doNotReportItAgain() {
        PeerBanList bans = new PeerBanList(1);
        bans.recordChecksumFailure(peer);

        assertThat(bans.recordChecksumFailure(peer)).isFalse();
        assertThat(bans.checksumFailures(peer)).isEqualTo(2);
    }

    @Test
    void failuresAreCountedPerPeer() {
        PeerBanList bans = new PeerBanList(2);
        bans.recordChecksumFailure(peer);
        bans.recordChecksumFailure(other);

        assertThat(bans.banned()).isEmpty();
        assertThat(bans.checksumFailures(peer)).isEqualTo(1);
        assertThat(bans.checksumFailures(other)).isEqualTo(1);
    }

    @Test
    void unknownPeer_isCleanAndUnbanned() {
        PeerBanList bans = new PeerBanList();

        assertThat(bans.checksumFailures(peer)).isZero();
        assertThat(bans.isBanned(peer)).isFalse();
    }

    @Test
    void concurrentFailures_banExactlyOnce() throws Exception {
        PeerBanList bans = new PeerBanList(10);
        AtomicInteger banReports = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 200; i++) {
                executor.submit(() -> {
                    start.await();
                    if (bans.recordChecksumFailure(peer)) {
                        banReports.incrementAndGet();
                    }
                    return null;
                });
            }
            start.countDown();
        }

        assertThat(banReports).hasValue(1);
        assertThat(bans.checksumFailures(peer)).isEqualTo(200);
    }

    @Test
    void bannedSnapshot_isImmutable() {
        PeerBanList bans = new PeerBanList(1);
        bans.recordChecksumFailure(peer);

        assertThatThrownBy(() -> bans.banned().add(other)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void threshold_mustBePositive() {
        assertThatThrownBy(() -> new PeerBanList(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
