package io.eaf.bootstrap;

import io.eaf.shared.EafException;
import io.eaf.usage.api.UsageRecord;
import io.eaf.usage.api.UsageRecorder;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
// 在隔离 PostgreSQL 中核验价格快照、稳定调用去重和未知费用状态。
class P7UsageAccountingTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final Instant EFFECTIVE = Instant.parse("2026-09-01T00:00:00Z");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired UsageRecorder usage;

    @Test
    void capturesEffectivePriceAndDeduplicatesStableCallKey() {
        // 单价改版只影响生效时间之后的新调用，同一稳定键重放不得重估历史成本。
        addPrice("fixture-chat", "CHAT", "v1", "USD", "TOKEN_MILLION", "2.5", "10", null, EFFECTIVE, null);
        var task = UUID.randomUUID();
        var run = UUID.randomUUID();
        var original = taskUsage(task, run, 1, "fixture-chat", 500_000, 100_000, "KNOWN", "SUCCEEDED", EFFECTIVE.plusSeconds(10));
        usage.record(original);
        var saved = usage.findForTask(TENANT, WORKSPACE, task).getFirst();
        assertThat(saved.estimatedCost()).isEqualByComparingTo("2.25000000");
        assertThat(saved.costCurrency()).isEqualTo("USD");
        assertThat(saved.costStatus()).isEqualTo("ESTIMATED");
        assertThat(saved.priceVersion()).isEqualTo("v1");
        assertThat(saved.priceSourceVersion()).isEqualTo("fixture-2026-09");
        assertThat(saved.inputPricePerMillion()).isEqualByComparingTo("2.5");

        addPrice("fixture-chat", "CHAT", "v2", "USD", "TOKEN_MILLION", "4", "20", null,
                Instant.parse("2026-10-01T00:00:00Z"), null);
        usage.record(original);
        assertThat(usage.findForTask(TENANT, WORKSPACE, task)).hasSize(1);
        assertThat(usage.findForTask(TENANT, WORKSPACE, task).getFirst().priceVersion()).isEqualTo("v1");

        var next = taskUsage(task, run, 2, "fixture-chat", 500_000, 100_000, "KNOWN", "SUCCEEDED",
                Instant.parse("2026-10-02T00:00:00Z"));
        usage.record(next);
        var nextSaved = usage.findForTask(TENANT, WORKSPACE, task).get(1);
        assertThat(nextSaved.estimatedCost()).isEqualByComparingTo("4.00000000");
        assertThat(nextSaved.priceVersion()).isEqualTo("v2");

        assertThatThrownBy(() -> usage.record(taskUsage(task, run, 1, "fixture-chat", 400_000, 100_000,
                "KNOWN", "SUCCEEDED", EFFECTIVE.plusSeconds(10))))
                .isInstanceOf(EafException.class).hasMessageContaining("不同的 Usage 事实");
        assertThatThrownBy(() -> jdbc.update("update usage.price_schedule set source_version = 'rewritten' where provider = 'fixture-provider' and model = 'fixture-chat' and price_version = 'v1'"))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
    }

    @Test
    void unknownPriceAndUnknownTokensNeverBecomeZeroCost() {
        // 缺价与缺计量分别保留原因和空金额，不把未核验支出伪装为免费。
        var noPrice = taskUsage(UUID.randomUUID(), UUID.randomUUID(), 1, "unpriced-model", 100, 20, "KNOWN", "SUCCEEDED", EFFECTIVE);
        usage.record(noPrice);
        var unknownPrice = usage.findForTask(TENANT, WORKSPACE, noPrice.taskId()).getFirst();
        assertThat(unknownPrice.costStatus()).isEqualTo("UNKNOWN_PRICE");
        assertThat(unknownPrice.estimatedCost()).isNull();
        assertThat(unknownPrice.costCurrency()).isNull();

        addPrice("fixture-unknown-usage", "CHAT", "v1", "USD", "TOKEN_MILLION", "3", "12", null, EFFECTIVE, null);
        var missingUsage = taskUsage(UUID.randomUUID(), UUID.randomUUID(), 1, "fixture-unknown-usage", null, null,
                "UNKNOWN", "FAILED", EFFECTIVE.plusSeconds(1));
        usage.record(missingUsage);
        var unknownUsage = usage.findForTask(TENANT, WORKSPACE, missingUsage.taskId()).getFirst();
        assertThat(unknownUsage.costStatus()).isEqualTo("UNKNOWN_USAGE");
        assertThat(unknownUsage.estimatedCost()).isNull();
        assertThat(unknownUsage.priceVersion()).isEqualTo("v1");
    }

    @Test
    void pricesEmbeddingBatchesAndPeersByTheirOwnScopeAndStoresBilledFacts() {
        // 索引作业和 peer 调用使用自己的 Scope；后到账单只补记实付，不抹去原估算。
        addPrice("fixture-embedding", "EMBEDDING_BATCH", "v1", "USD", "TOKEN_MILLION", "0.12", "0", null, EFFECTIVE, null);
        addPrice("fixture-peer", "PEER", "v1", "USD", "REQUEST", null, null, "0.0008", EFFECTIVE, null);
        var jobId = UUID.randomUUID();
        var embedded = scopedUsage("job:embedding:" + jobId + ":batch:1", "EMBEDDING_BATCH", "JOB", jobId,
                "fixture-embedding", 250_000, null, "KNOWN", "SUCCEEDED");
        usage.record(embedded);
        var embeddedSaved = usage.findForScope(TENANT, WORKSPACE, "JOB", jobId).getFirst();
        assertThat(embeddedSaved.taskId()).isNull();
        assertThat(embeddedSaved.estimatedCost()).isEqualByComparingTo("0.03000000");
        assertThat(embeddedSaved.priceVersion()).isEqualTo("v1");

        var workflowId = UUID.randomUUID();
        var peer = scopedUsage("workflow:" + workflowId + ":peer:1", "PEER", "WORKFLOW", workflowId,
                "fixture-peer", null, null, "UNKNOWN", "SUCCEEDED");
        usage.record(peer);
        var peerSaved = usage.findForScope(TENANT, WORKSPACE, "WORKFLOW", workflowId).getFirst();
        assertThat(peerSaved.costStatus()).isEqualTo("ESTIMATED");
        assertThat(peerSaved.estimatedCost()).isEqualByComparingTo("0.00080000");

        usage.recordBilledCost(peer.callKey(), new BigDecimal("0.0011"), "USD", "fixture-statement-v1");
        usage.recordBilledCost(peer.callKey(), new BigDecimal("0.00110000"), "USD", "fixture-statement-v1");
        var billed = usage.findForScope(TENANT, WORKSPACE, "WORKFLOW", workflowId).getFirst();
        assertThat(billed.costStatus()).isEqualTo("BILLED");
        assertThat(billed.estimatedCost()).isEqualByComparingTo("0.00080000");
        assertThat(billed.actualCost()).isEqualByComparingTo("0.00110000");
        assertThat(billed.actualCostCurrency()).isEqualTo("USD");
        assertThat(billed.billingSource()).isEqualTo("fixture-statement-v1");
        assertThatThrownBy(() -> usage.recordBilledCost(peer.callKey(), new BigDecimal("0.002"), "USD", "fixture-statement-v2"))
                .isInstanceOf(EafException.class).hasMessageContaining("不同账单事实");
    }

    private void addPrice(String model, String callType, String version, String currency, String unit,
                          String input, String output, String request, Instant effective, Instant expires) {
        jdbc.update("insert into usage.price_schedule(provider, model, call_type, price_version, source, source_version, currency, billing_unit, "
                        + "input_price_per_million, output_price_per_million, request_price, effective_at, expires_at) values "
                        + "('fixture-provider', ?, ?, ?, 'local-fixture', 'fixture-2026-09', ?, ?, ?, ?, ?, ?, ?)",
                model, callType, version, currency, unit, decimal(input), decimal(output), decimal(request),
                java.sql.Timestamp.from(effective), expires == null ? null : java.sql.Timestamp.from(expires));
    }

    private UsageRecord taskUsage(UUID taskId, UUID runId, int callNo, String model, Integer inputTokens,
                                  Integer outputTokens, String usageStatus, String status, Instant startedAt) {
        return new UsageRecord(TENANT, WORKSPACE, taskId, runId, "USER", "fixture-provider", model,
                inputTokens, outputTokens, usageStatus, null, null, "UNKNOWN_PRICE", null, 100,
                status, null, startedAt, startedAt.plusSeconds(1), callNo);
    }

    private UsageRecord scopedUsage(String key, String type, String scopeType, UUID scopeId, String model,
                                    Integer inputTokens, Integer outputTokens, String usageStatus, String status) {
        return new UsageRecord(TENANT, WORKSPACE, null, null, "USER", "fixture-provider", model,
                inputTokens, outputTokens, usageStatus, 0, status, null, EFFECTIVE, EFFECTIVE.plusSeconds(1),
                1, key, type, scopeType, scopeId);
    }

    private static BigDecimal decimal(String value) { return value == null ? null : new BigDecimal(value); }
}
