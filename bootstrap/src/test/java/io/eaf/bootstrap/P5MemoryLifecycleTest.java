package io.eaf.bootstrap;

import io.eaf.memory.api.CreateMemoryCommand;
import io.eaf.memory.api.CreateMemoryVersionCommand;
import io.eaf.memory.api.MemoryService;
import io.eaf.memory.infrastructure.MemoryOutboxPublisher;
import io.eaf.audit.api.AuditPort;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.memory.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
// 在隔离 PostgreSQL 中验证 Memory 版本、Owner、scope、来源、审计、到期和撤回。
class P5MemoryLifecycleTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID SEED_ID = UUID.fromString("56000000-0000-4000-8000-000000000001");
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final ActorContext BOB = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final ActorContext CAROL = new ActorContext(Ids.CAROL, Ids.TENANT_B, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired MemoryService memories;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;
    @Autowired AuditPort audit;
    @Autowired PlatformTransactionManager transactionManager;

    @Test
    void memoryVersionsRespectOwnerScopeSourceExpiryAndReleaseAudit() throws Exception {
        var seed = memories.get(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0");
        assertThat(seed.status()).isEqualTo("DRAFT");
        assertThat(seed.sourceType()).isEqualTo("CONTROLLED_SEED");

        mvc.perform(post("/api/v1/workspaces/{workspaceId}/memories", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"logicalKey":"forged-source","version":"1.0.0","type":"SEMANTIC","scope":"PERSONAL",
                                 "content":"未经允许的来源声明","confidence":0.7,"expiresAt":"2099-12-31T23:59:59Z",
                                 "sourceRef":"feedback:arbitrary","evidenceRefs":["test:evidence"],
                                 "sourceType":"FEEDBACK","ownerId":"80000000-0000-4000-8000-000000000002"}
                                """))
                .andExpect(status().isBadRequest());

        assertThatThrownBy(() -> memories.create(new CreateMemoryCommand(ALICE, Ids.WORKSPACE_A, "missing-source",
                "1.0.0", "SEMANTIC", "PERSONAL", "需要有来源的内容", 0.7,
                Instant.now().plusSeconds(300), null, List.of("test:evidence"))))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(400));
        assertThatThrownBy(() -> memories.create(new CreateMemoryCommand(ALICE, Ids.WORKSPACE_A, "bad-confidence",
                "1.0.0", "SEMANTIC", "PERSONAL", "置信度必须在范围内", 1.01,
                Instant.now().plusSeconds(300), "manual:source", List.of("test:evidence"))))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(400));
        assertThatThrownBy(() -> memories.create(new CreateMemoryCommand(null, Ids.WORKSPACE_A, "missing-owner",
                "1.0.0", "SEMANTIC", "PERSONAL", "Owner 必须来自已验证身份", 0.7,
                Instant.now().plusSeconds(300), "manual:source", List.of("test:evidence"))))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(400));

        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'memory:read', 'ACTIVE') on conflict do nothing",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'memory:write', 'ACTIVE') on conflict do nothing",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
        var publishedSeed = memories.publish(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0", seed.rowVersion());
        assertThat(publishedSeed.status()).isEqualTo("PUBLISHED");
        assertThat(jdbc.queryForObject("select content_hash from memory.release where memory_id = ? and memory_version = '1.0.0' and action = 'PUBLISHED'",
                String.class, SEED_ID)).isEqualTo(publishedSeed.contentHash());
        assertThat(jdbc.queryForObject("select count(*) from memory.outbox where event_type = 'eaf.memory.version-published.v1' and payload->>'contentHash' = ?",
                Integer.class, publishedSeed.contentHash())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class,
                "memory:" + SEED_ID + ":1.0.0:MEMORY_PUBLISHED")).isEqualTo(1);
        assertThat(memories.requireUsable(BOB, Ids.WORKSPACE_A, SEED_ID, "1.0.0").scope()).isEqualTo("TEAM");

        // 运维重投按 Memory Owner 的发布版本可见性授权，并原样复用已发布 eventId/payload。
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, 'memory:outbox:replay', 'ACTIVE') on conflict do nothing",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        var releaseEvent = jdbc.queryForObject("select event_id from memory.outbox where event_type = 'eaf.memory.version-published.v1' "
                        + "and payload->>'contentHash' = ?", UUID.class, publishedSeed.contentHash());
        var originalPayload = jdbc.queryForObject("select payload::text from memory.outbox where event_id = ?", String.class, releaseEvent);
        jdbc.update("update memory.outbox set status = 'FAILED', attempt_count = 6, next_attempt_at = now() - interval '1 second' where event_id = ?",
                releaseEvent);
        var replayKey = "p7-memory-replay-" + UUID.randomUUID();
        var replayReceipt = memories.replayOutbox(ALICE, Ids.WORKSPACE_A, releaseEvent, replayKey, "本域审计投递故障恢复");
        assertThat(replayReceipt.status()).isEqualTo("PENDING");
        assertThat(replayReceipt.attempts()).isEqualTo(6);
        assertThat(jdbc.queryForObject("select payload::text from memory.outbox where event_id = ?", String.class, releaseEvent))
                .isEqualTo(originalPayload);
        assertThat(memories.replayOutbox(ALICE, Ids.WORKSPACE_A, releaseEvent, replayKey, "本域审计投递故障恢复").replayed())
                .isTrue();
        assertThatThrownBy(() -> memories.replayOutbox(ALICE, Ids.WORKSPACE_A, releaseEvent, replayKey, "异参重放"))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("MEMORY_COMMAND_CONFLICT"));
        new MemoryOutboxPublisher(jdbc, audit, transactionManager).publish();
        assertThat(jdbc.queryForObject("select status from memory.outbox where event_id = ?", String.class, releaseEvent))
                .isEqualTo("DELIVERED");
        assertThat(jdbc.queryForObject("select attempt_count from memory.outbox where event_id = ?", Integer.class, releaseEvent))
                .isEqualTo(6);

        assertThatThrownBy(() -> memories.addVersion(BOB, Ids.WORKSPACE_A, SEED_ID,
                new CreateMemoryVersionCommand("2.0.0", "SEMANTIC", "TEAM", "越权修改", 0.8,
                        Instant.now().plusSeconds(300), "manual:owner", List.of("test:evidence"))))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(403));
        assertThatThrownBy(() -> memories.create(new CreateMemoryCommand(BOB, Ids.WORKSPACE_A, "team-scope-denied",
                "1.0.0", "SEMANTIC", "TEAM", "缺少团队范围授权", 0.8,
                Instant.now().plusSeconds(300), "manual:bob", List.of("test:evidence"))))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(403));
        assertThatThrownBy(() -> memories.get(CAROL, Ids.WORKSPACE_A, SEED_ID, "1.0.0"))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(404));
        assertThatThrownBy(() -> memories.get(ALICE, Ids.WORKSPACE_A2, SEED_ID, "1.0.0"))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(404));

        var personal = memories.create(new CreateMemoryCommand(ALICE, Ids.WORKSPACE_A, "alice-private-note", "1.0.0",
                "PREFERENCE", "PERSONAL", "只对 Owner 可见", 0.6, Instant.now().plusSeconds(300),
                "manual:alice", List.of("test:alice")));
        var publishedPersonal = memories.publish(ALICE, Ids.WORKSPACE_A, personal.id(), personal.version(), personal.rowVersion());
        assertThatThrownBy(() -> memories.get(BOB, Ids.WORKSPACE_A, personal.id(), personal.version()))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(404));
        assertThat(memories.get(ALICE, Ids.WORKSPACE_A, personal.id(), personal.version()).ownerId()).isEqualTo(Ids.ALICE);

        var renewed = memories.addVersion(ALICE, Ids.WORKSPACE_A, SEED_ID,
                new CreateMemoryVersionCommand("2.0.0", "SEMANTIC", "TEAM", "新版本要求重新查询 CRM 确认。", 0.9,
                        Instant.now().plusSeconds(2), "manual:renewed", List.of("test:renewed")));
        assertThat(renewed.contentHash()).isNotEqualTo(publishedSeed.contentHash());
        var publishedRenewal = memories.publish(ALICE, Ids.WORKSPACE_A, SEED_ID, "2.0.0", renewed.rowVersion());
        assertThat(publishedRenewal.status()).isEqualTo("PUBLISHED");
        assertThatThrownBy(() -> memories.requireUsable(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0"))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(404));
        Thread.sleep(2_200);
        assertThatThrownBy(() -> memories.requireUsable(ALICE, Ids.WORKSPACE_A, SEED_ID, "2.0.0"))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(404));

        var revoked = memories.revoke(ALICE, Ids.WORKSPACE_A, personal.id(), personal.version(), publishedPersonal.rowVersion());
        assertThat(revoked.status()).isEqualTo("REVOKED");
        assertThat(jdbc.queryForObject("select count(*) from memory.outbox where event_type = 'eaf.memory.version-revoked.v1' and payload->>'contentHash' = ?",
                Integer.class, revoked.contentHash())).isEqualTo(1);
        assertThatThrownBy(() -> memories.requireUsable(ALICE, Ids.WORKSPACE_A, personal.id(), personal.version()))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(404));
    }
}
// 本文件负责实现 MemoryLifecycleTest.java 相关代码。
