package io.swarmshare.transfer;

import io.swarmshare.core.domain.PeerInfo;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Corrupt-peer detection (roadmap phase 6): counts the chunks each peer served
 * that failed SHA-256 verification and bans a peer once it reaches the threshold.
 *
 * <p>
 * Only checksum failures count. A timeout or refused connection says the peer is
 * unreachable, not that it lies, and is already handled by trying another peer.
 * A peer that keeps sending bytes that do not match the manifest is either
 * corrupt or malicious; asking it again only wastes bandwidth.
 *
 * <p>
 * Bans last for one transfer — the project keeps no persistent peer state.
 *
 * <h3>Thread safety</h3>
 * Called concurrently from every download virtual thread. Counters are
 * {@link AtomicInteger}s in a {@link ConcurrentHashMap}, and the ban is decided by
 * the single increment that reaches the threshold, so it is reported exactly once.
 */
public final class PeerBanList {

    /** Checksum failures after which a peer is banned. */
    public static final int DEFAULT_MAX_CHECKSUM_FAILURES = 3;

    private final int maxChecksumFailures;
    private final Map<PeerInfo, AtomicInteger> checksumFailures = new ConcurrentHashMap<>();
    private final Set<PeerInfo> banned = ConcurrentHashMap.newKeySet();

    public PeerBanList() {
        this(DEFAULT_MAX_CHECKSUM_FAILURES);
    }

    /**
     * @param maxChecksumFailures failures after which a peer is banned; must be at least 1
     */
    public PeerBanList(int maxChecksumFailures) {
        if (maxChecksumFailures < 1) {
            throw new IllegalArgumentException("maxChecksumFailures must be >= 1, got: " + maxChecksumFailures);
        }
        this.maxChecksumFailures = maxChecksumFailures;
    }

    /**
     * Records that {@code peer} served a chunk whose hash did not match the manifest.
     *
     * @return {@code true} if this failure got the peer banned (only the call that
     * reaches the threshold returns {@code true})
     */
    public boolean recordChecksumFailure(PeerInfo peer) {
        int failures = checksumFailures.computeIfAbsent(peer, _ -> new AtomicInteger()).incrementAndGet();
        if (failures == maxChecksumFailures) {
            banned.add(peer);
            return true;
        }
        return false;
    }

    /** Checksum failures at which a peer is banned. */
    public int maxChecksumFailures() {
        return maxChecksumFailures;
    }

    /** Whether {@code peer} has been banned; banned peers must not be asked for chunks. */
    public boolean isBanned(PeerInfo peer) {
        return banned.contains(peer);
    }

    /** Checksum failures recorded for {@code peer} so far. */
    public int checksumFailures(PeerInfo peer) {
        AtomicInteger failures = checksumFailures.get(peer);
        return failures == null ? 0 : failures.get();
    }

    /** Snapshot of the banned peers. */
    public Set<PeerInfo> banned() {
        return Set.copyOf(banned);
    }
}
