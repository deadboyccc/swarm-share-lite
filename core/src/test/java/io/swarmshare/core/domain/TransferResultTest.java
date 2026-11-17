package io.swarmshare.core.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link TransferResult} is sealed so the orchestrator must handle both outcomes;
 * the switch below fails to compile if a permitted subtype is ever added.
 */
class TransferResultTest {

    private static final ChunkId ID = new ChunkId("abc", 2);

    private static String describe(TransferResult result) {
        return switch (result) {
            case TransferResult.Success s -> "ok:" + s.data().length;
            case TransferResult.Failure f when f.shouldRetry() -> "retry:" + f.reason();
            case TransferResult.Failure f -> "fatal:" + f.reason();
        };
    }

    @Test
    void success_carriesIdAndData() {
        var success = new TransferResult.Success(ID, new byte[]{1, 2, 3});

        assertThat(success.id()).isEqualTo(ID);
        assertThat(describe(success)).isEqualTo("ok:3");
    }

    @Test
    void retryableFailure_isDistinguishedFromFatal() {
        assertThat(describe(new TransferResult.Failure(ID, "timeout", true))).isEqualTo("retry:timeout");
        assertThat(describe(new TransferResult.Failure(ID, "bad manifest", false))).isEqualTo("fatal:bad manifest");
    }
}
