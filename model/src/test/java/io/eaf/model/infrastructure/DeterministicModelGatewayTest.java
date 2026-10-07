package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.ModelMessage;
import io.eaf.model.api.ModelRequest;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeterministicModelGatewayTest {
    private static final UUID TASK = UUID.randomUUID();

    @Test
    void fixedScenariosKeepGatewaySemantics() {
        var success = new DeterministicModelGateway("SUCCESS");
        assertThat(success.call(request("客户续约摘要")).publicOutput()).contains("\"riskLevel\":\"LOW\"");
        assertThat(success.callCount()).isEqualTo(1);

        var invalid = new DeterministicModelGateway("INVALID_JSON");
        assertThat(invalid.call(request("材料" )).publicOutput()).isEqualTo("not-json");
        assertThat(invalid.callCount()).isEqualTo(1);

        var unknown = new DeterministicModelGateway("UNKNOWN_USAGE");
        var unknownResult = unknown.call(request("材料"));
        assertThat(unknownResult.inputTokens()).isNull();
        assertThat(unknownResult.outputTokens()).isNull();
        assertThat(unknownResult.usageStatus()).isEqualTo("UNKNOWN");

        for (var scenario : List.of("REJECT", "TIMEOUT")) {
            var gateway = new DeterministicModelGateway(scenario);
            assertThatThrownBy(() -> gateway.call(request("材料")))
                    .isInstanceOf(ModelFailure.class)
                    .satisfies(error -> assertThat(((ModelFailure) error).called()).isTrue());
            assertThat(gateway.callCount()).isEqualTo(1);
        }

        var budget = new DeterministicModelGateway("BUDGET_EXCEEDED");
        assertThatThrownBy(() -> budget.call(request("材料")))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> assertThat(((ModelFailure) error).called()).isFalse());
        assertThat(budget.callCount()).isZero();
    }

    @Test
    void expiredDeadlineIsRejectedBeforeCallingModel() {
        var gateway = new DeterministicModelGateway("SUCCESS");
        assertThatThrownBy(() -> gateway.call(request("材料", Instant.now().minusSeconds(1))))
                .isInstanceOf(ModelFailure.class)
                .satisfies(error -> assertThat(((ModelFailure) error).called()).isFalse());
        assertThat(gateway.callCount()).isZero();
    }

    private ModelRequest request(String input) { return request(input, Instant.now().plusSeconds(20)); }

    private ModelRequest request(String input, Instant deadline) {
        return new ModelRequest(UUID.randomUUID(), List.of(new ModelMessage("system", "system"), new ModelMessage("user", input)), deadline, 8_000, TASK, "trace");
    }
}
// 本文件负责实现 EAF 的 DeterministicModelGatewayTest.java 相关代码。
