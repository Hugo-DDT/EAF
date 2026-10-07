package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelFailure;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class TypedDecisionConfigurationTest {
    private static final String STOP = "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP";

    @Test
    void jevDefaultsToDisabledAndDeterministicModeDoesNotCreateAnExternalCall() {
        var configuration = new ModelConfiguration();
        var disabled = configure(configuration, "disabled", "https://api.typesafe.ai/v1/systemone", true,
                "synthetic-customer-summary-only", "local-operator", "run-1", "2099-01-01T00:00:00Z");
        var deterministic = configure(configuration, "deterministic", "https://api.typesafe.ai/v1/systemone", false,
                "", "", "", "");

        assertThat(disabled.enabled()).isFalse();
        assertThat(deterministic.enabled()).isTrue();
        assertThat(deterministic.external()).isFalse();
        assertThat(deterministic.billingProfile()).isNull();
    }

    @Test
    void liveModeRequiresProviderSpecificAuthorizationAndARegisteredEndpoint() {
        var missingAuthorization = configure(new ModelConfiguration(), "live", "https://api.typesafe.ai/v1/systemone",
                false, "synthetic-customer-summary-only", "local-operator", "run-1", "2099-01-01T00:00:00Z");
        var untrustedEndpoint = configure(new ModelConfiguration(), "live", "http://127.0.0.1/v1/systemone",
                true, "synthetic-customer-summary-only", "local-operator", "run-1", "2099-01-01T00:00:00Z");

        assertThatThrownBy(() -> missingAuthorization.decide(request()))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> assertThat(((ModelFailure) error).called()).isFalse());
        assertThatThrownBy(() -> untrustedEndpoint.decide(request()))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> assertThat(((ModelFailure) error).called()).isFalse());
        assertThat(missingAuthorization.callCount()).isZero();
        assertThat(untrustedEndpoint.callCount()).isZero();
    }

    @Test
    void validLiveConfigurationUsesDedicatedDecisionBillingIdentity() {
        var gateway = configure(new ModelConfiguration(), "live", "https://api.typesafe.ai/v1/systemone", true,
                "synthetic-customer-summary-only", "local-operator", "run-1", "2099-01-01T00:00:00Z");

        assertThat(gateway.external()).isTrue();
        assertThat(gateway.billingProfile().provider()).isEqualTo("typesafe");
        assertThat(gateway.billingProfile().model()).isEqualTo("jev-1.13.0");
        assertThat(gateway.billingProfile().callType()).isEqualTo("DECISION");
        assertThat(gateway.callCount()).isZero();
    }

    private io.eaf.model.api.TypedDecisionGateway configure(ModelConfiguration configuration, String mode,
            String endpoint, boolean dataAuthorized, String dataScope, String owner, String authorizationId,
            String expiry) {
        return configuration.typedDecisionGateway(mode, endpoint, "jev-1.13.0", dataAuthorized, dataScope,
                owner, authorizationId, expiry, "1.00", "USD", STOP,
                mock(ProviderCredentialRequestInterceptor.class));
    }

    private io.eaf.model.api.TypedDecisionRequest request() {
        return new io.eaf.model.api.TypedDecisionRequest("合成摘要", Instant.now().plusSeconds(10), 100,
                UUID.randomUUID(), "trace", 1);
    }
}
