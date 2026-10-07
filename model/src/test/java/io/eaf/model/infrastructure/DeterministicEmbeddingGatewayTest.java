package io.eaf.model.infrastructure;

import io.eaf.model.api.EmbeddingFailure;
import io.eaf.model.api.EmbeddingCallScope;
import io.eaf.model.api.EmbeddingRequest;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeterministicEmbeddingGatewayTest {
    @Test
    void returnsStableConfiguredVectorsAndUsage() {
        var gateway = new DeterministicEmbeddingGateway();
        var request = request(List.of("客户服务规范", "客户服务规范"), 100);
        var first = gateway.embed(request);
        var second = gateway.embed(request);

        assertThat(first.vectors()).isEqualTo(second.vectors());
        assertThat(first.vectors()).hasSize(2).allSatisfy(vector -> assertThat(vector).hasSize(8));
        assertThat(first.inputTokens()).isEqualTo(4);
        assertThat(gateway.callCount()).isEqualTo(2);
    }

    @Test
    void rejectsUnboundedOrUnattributedCallsBeforeProviderWork() {
        var gateway = new DeterministicEmbeddingGateway();
        assertThatThrownBy(() -> gateway.embed(new EmbeddingRequest("deterministic", "p3-test-embedding-8", "SHA256-NORMALIZED-V1", "EMBEDDING_QUERY",
                List.of("正文"), 8, 0, Instant.now().plusSeconds(10), scope("EMBEDDING_QUERY"))))
                .isInstanceOf(EmbeddingFailure.class)
                .satisfies(error -> assertThat(((EmbeddingFailure) error).code()).isEqualTo("BUDGET_EXCEEDED"));
        assertThatThrownBy(() -> gateway.embed(new EmbeddingRequest("deterministic", "p3-test-embedding-8", "SHA256-NORMALIZED-V1", "EMBEDDING_BATCH",
                List.of("正文"), 8, 100, Instant.now().plusSeconds(10), null)))
                .isInstanceOf(EmbeddingFailure.class)
                .satisfies(error -> assertThat(((EmbeddingFailure) error).code()).isEqualTo("INVALID_USAGE_SCOPE"));
        assertThat(gateway.callCount()).isZero();
    }

    private EmbeddingRequest request(List<String> texts, int maxTokens) {
        return new EmbeddingRequest("deterministic", "p3-test-embedding-8", "SHA256-NORMALIZED-V1", "EMBEDDING_BATCH",
                texts, 8, maxTokens, Instant.now().plusSeconds(10), scope("EMBEDDING_BATCH"));
    }

    private EmbeddingCallScope scope(String callType) {
        var tenant = UUID.randomUUID();
        var workspace = UUID.randomUUID();
        var id = UUID.randomUUID();
        return new EmbeddingCallScope(tenant, workspace, null, null, "USER", "JOB", id, 1,
                "job:embedding:" + id, callType);
    }
}
// 本测试验证批次边界、授权、预算、稳定向量和计量值，不宣称确定性替身具备语义质量。
