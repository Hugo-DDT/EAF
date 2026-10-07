package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.ModelRequest;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class ModelConfigurationTest {
    @Test
    void liveModeWithoutModelIdFailsBeforeProviderLookup() {
        var gateway = new ModelConfiguration().modelGateway("live-model", "SUCCESS", "", true, "1", "USD", "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP", mock(ApplicationContext.class));

        assertThatThrownBy(() -> gateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> {
                    var failure = (ModelFailure) error;
                    assertThat(failure.code()).isEqualTo("DEPENDENCY_UNAVAILABLE");
                    assertThat(failure.called()).isFalse();
                });
        assertThat(gateway.callCount()).isZero();
    }

    @Test
    void liveModeRequiresExplicitRunbookGuards() {
        var gateway = new ModelConfiguration().modelGateway("live-model", "SUCCESS", "configured-model", false, "", "", "", mock(ApplicationContext.class));

        assertThatThrownBy(() -> gateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> {
                    var failure = (ModelFailure) error;
                    assertThat(failure.code()).isEqualTo("DEPENDENCY_UNAVAILABLE");
                    assertThat(failure.called()).isFalse();
                });
        assertThat(gateway.callCount()).isZero();
    }

    @Test
    // 已过期的单轮授权必须在查找 Provider 前失败关闭。
    void liveModeRejectsExpiredAuthorizationBeforeProviderLookup() {
        var configuration = new ModelConfiguration();
        var gateway = configuration.modelGateway("live-model", "SUCCESS", "deepseek", "deepseek-flash",
                "https://api.deepseek.com", "model-deepseek", true, "synthetic-evaluation-only", "1.00", "USD",
                "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP", "local-operator", "expired-run",
                "2020-01-01T00:00:00Z", mock(ApplicationContext.class));
        var malformedExpiryGateway = configuration.modelGateway("live-model", "SUCCESS", "deepseek", "deepseek-flash",
                "https://api.deepseek.com", "model-deepseek", true, "synthetic-evaluation-only", "1.00", "USD",
                "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP", "local-operator", "malformed-run",
                "not-an-instant", mock(ApplicationContext.class));

        assertThatThrownBy(() -> gateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> assertThat(((ModelFailure) error).called()).isFalse());
        assertThat(gateway.callCount()).isZero();
        assertThatThrownBy(() -> malformedExpiryGateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> assertThat(((ModelFailure) error).called()).isFalse());
        assertThat(malformedExpiryGateway.callCount()).isZero();
    }

    @Test
    //  只允许有限合成数据范围和费用上限，任一扩大都应拒绝。
    void liveModeRejectsBroaderDataScopeAndFeeCap() {
        var configuration = new ModelConfiguration();
        var scopeGateway = configuration.modelGateway("live-model", "SUCCESS", "deepseek", "deepseek-flash",
                "https://api.deepseek.com", "model-deepseek", true, "all-data", "1.00", "USD",
                "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP", "local-operator", "current-run",
                "2099-01-01T00:00:00Z", mock(ApplicationContext.class));
        var feeGateway = configuration.modelGateway("live-model", "SUCCESS", "deepseek", "deepseek-flash",
                "https://api.deepseek.com", "model-deepseek", true, "synthetic-evaluation-only", "1.01", "USD",
                "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP", "local-operator", "current-run",
                "2099-01-01T00:00:00Z", mock(ApplicationContext.class));

        assertThatThrownBy(() -> scopeGateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class);
        assertThatThrownBy(() -> feeGateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class);
        assertThat(scopeGateway.callCount()).isZero();
        assertThat(feeGateway.callCount()).isZero();
    }

    @Test
    // Gateway 即使已装配，也须在调用时重新校验授权截止时间。
    void gatewayRechecksAuthorizationImmediatelyBeforeProviderCall() {
        var gateway = new SpringAiModelGateway(new Object(), "deepseek-flash", "deepseek", Instant.parse("2020-01-01T00:00:00Z"));
        var dashScopeGateway = new SpringAiAlibabaModelGateway(new Object(), "qwen-plus", Instant.parse("2020-01-01T00:00:00Z"));

        assertThatThrownBy(() -> gateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> {
                    var failure = (ModelFailure) error;
                    assertThat(failure.code()).isEqualTo("AUTHORIZATION_EXPIRED");
                    assertThat(failure.called()).isFalse();
                });
        assertThat(gateway.callCount()).isZero();
        assertThatThrownBy(() -> dashScopeGateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> {
                    var failure = (ModelFailure) error;
                    assertThat(failure.code()).isEqualTo("AUTHORIZATION_EXPIRED");
                    assertThat(failure.called()).isFalse();
                });
        assertThat(dashScopeGateway.callCount()).isZero();
    }

    @Test
    void liveModeWithoutProviderFailsClearly() {
        var gateway = new ModelConfiguration().modelGateway("live-model", "SUCCESS", "configured-model", true, "1", "USD", "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP", mock(ApplicationContext.class));

        assertThatThrownBy(() -> gateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> {
                    var failure = (ModelFailure) error;
                    assertThat(failure.code()).isEqualTo("DEPENDENCY_UNAVAILABLE");
                    assertThat(failure.called()).isFalse();
                });
        assertThat(gateway.callCount()).isZero();
    }

    @Test
    void unknownModeDoesNotSilentlyUseDeterministicGateway() {
        var gateway = new ModelConfiguration().modelGateway("typo", "SUCCESS", "", true, "1", "USD", "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP", mock(ApplicationContext.class));

        assertThatThrownBy(() -> gateway.call(new ModelRequest(UUID.randomUUID(), List.of(), Instant.now().plusSeconds(20), 8_000, UUID.randomUUID(), "trace")))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> assertThat(((ModelFailure) error).called()).isFalse());
        assertThat(gateway.callCount()).isZero();
    }

    @Test
    void mapsProviderFailuresWithoutFallback() {
        var gateway = new SpringAiModelGateway(new Object(), "configured-model", "test", Instant.MAX);

        assertThat(gateway.providerFailure(new RuntimeException("HTTP 401 Unauthorized")).code()).isEqualTo("UPSTREAM_AUTH");
        assertThat(gateway.providerFailure(new RuntimeException("HTTP 429 Too Many Requests")).code()).isEqualTo("UPSTREAM_RATE_LIMITED");
        assertThat(gateway.providerFailure(new RuntimeException("read timed out")).code()).isEqualTo("UPSTREAM_TIMEOUT");
        assertThat(gateway.providerFailure(new RuntimeException("HTTP 500 server error")).code()).isEqualTo("UPSTREAM_FAILURE");
    }
}
// 本文件负责实现 EAF 的 ModelConfigurationTest.java 相关代码。
