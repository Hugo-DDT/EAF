package io.eaf.model.infrastructure;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProviderFailureSupportTest {
    @Test
    void causeLookupAndDescriptionStopAtIdentityCycles() {
        var first = new CyclicFailure("first");
        var second = new CyclicFailure("second");
        var target = new IllegalStateException("target");
        first.cause = second;
        second.cause = first;

        assertThat(ProviderFailureSupport.causedBy(first, IllegalStateException.class)).isNull();
        assertThat(ProviderFailureSupport.describe(first)).contains("first", "second").doesNotContain("target");

        second.cause = target;
        assertThat(ProviderFailureSupport.causedBy(first, IllegalStateException.class)).isSameAs(target);
    }

    @Test
    void capacityAndQuotaLookupRemainCycleSafe() {
        var cycle = new CyclicFailure("cycle");
        cycle.cause = cycle;

        assertThat(ProviderFailureSupport.capacityExceeded(cycle)).isFalse();
        assertThat(ProviderFailureSupport.quotaUnavailable(cycle)).isFalse();
    }

    private static final class CyclicFailure extends RuntimeException {
        private Throwable cause;

        private CyclicFailure(String message) { super(message); }

        @Override public synchronized Throwable getCause() { return cause; }
    }
}
