package io.eaf.agentruntime.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.context.api.ContextItem;
import io.eaf.context.api.EnterpriseContext;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ServiceRequestPresentationTest {
    private final ObjectMapper json = new ObjectMapper().findAndRegisterModules();

    @Test
    void compactViewKeepsEvidenceAndBusinessFieldsWhileDroppingTechnicalCounters() {
        var item = new ContextItem("k-1", "KNOWLEDGE", UUID.randomUUID(), 3, UUID.randomUUID(), UUID.randomUUID(),
                null, null, "政策/账号", "source-hash", "账号申请🙂", 0.2, 14, "WORKSPACE", List.of("access"),
                Instant.parse("2026-12-01T00:00:00Z"), "SERVICE_REQUEST", "request-1", List.of("IT", "Access"),
                1, 8, "CODE_POINT");
        var snapshot = new EnterpriseContext("FOUND", 5, 2_000, 14, 1, "TOKEN_BUDGET", List.of(item));

        var compact = ServiceRequestPresentation.compactKnowledge(json, snapshot);

        assertThat(compact.path("status").asText()).isEqualTo("FOUND");
        assertThat(compact.path("omittedCount").asInt()).isEqualTo(1);
        assertThat(compact.path("truncationReason").asText()).isEqualTo("TOKEN_BUDGET");
        assertThat(compact.path("items").isArray()).isTrue();
        assertThat(compact.path("items").size()).isEqualTo(1);
        var projected = compact.path("items").get(0);
        assertThat(projected.path("citationId").asText()).isEqualTo("k-1");
        assertThat(projected.path("content").asText()).isEqualTo("账号申请🙂");
        assertThat(projected.path("headingPath").toString()).isEqualTo("[\"IT\",\"Access\"]");
        assertThat(projected.path("sourceRef").asText()).isEqualTo("政策/账号");
        assertThat(projected.path("businessEntityId").asText()).isEqualTo("request-1");
        assertThat(projected.has("documentId")).isFalse();
        assertThat(projected.has("contentHash")).isFalse();
        assertThat(projected.has("estimatedTokens")).isFalse();
        assertThat(compact.has("topK")).isFalse();
        assertThat(compact.has("usedTokens")).isFalse();
        assertThat(ServiceRequestPresentation.chars("知识🙂")).isEqualTo(3);
        assertThat(ServiceRequestPresentation.utf8Bytes("知识🙂")).isEqualTo(10);
    }
}
