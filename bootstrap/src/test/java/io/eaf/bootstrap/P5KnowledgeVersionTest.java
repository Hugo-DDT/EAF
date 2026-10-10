package io.eaf.bootstrap;

import io.eaf.knowledge.api.CreateKnowledgeDocumentCommand;
import io.eaf.knowledge.api.CreateKnowledgeVersionCommand;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.infrastructure.KnowledgeOutboxPublisher;
import io.eaf.model.api.ModelProfileCatalog;
import io.eaf.audit.api.AuditPort;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.shared.Ids;
import io.eaf.task.infrastructure.TaskClusterCapacityMaintenance;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
// 在隔离 PostgreSQL 验证 Knowledge 新版本预备、CAS 发布、检索指针和历史撤回。
class P5KnowledgeVersionTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
        registry.add("spring.flyway.target", () -> "60");
    }

    @Autowired KnowledgeService knowledge;
    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource dataSource;
    @Autowired MockMvc mvc;
    @Autowired AuditPort audit;
    @Autowired PlatformTransactionManager transactionManager;
    // 本测试从 V60 启动并手动迁移；隔离只在当前 schema 才可用的后台与档位服务。
    @MockitoBean TaskClusterCapacityMaintenance taskClusterCapacityMaintenance;
    @MockitoBean ModelProfileCatalog modelProfileCatalog;

    @Test
    void preparedVersionStaysOutOfRetrievalUntilBaseVersionPublication() throws Exception {
        upgradeV60PublicationAndVerifyP3Read();
        var workspace = Ids.WORKSPACE_A;
        var v1 = knowledge.create(new CreateKnowledgeDocumentCommand(ALICE, workspace, "版本规范",
                "manual://p5-version", "正式知识第一版，保留为当前检索内容。", Map.of(), "p5-v1", "trace-p5-v1"));
        var v1Chunks = knowledge.chunk(ALICE, workspace, v1.id(), 1, "p3-plain-1");
        var v1Build = knowledge.buildIndex(ALICE, workspace, v1.id(), 1, "p3-plain-1", "p5-v1-index");
        assertThat(v1Build.status()).isEqualTo("READY");
        var firstRelease = knowledge.publish(ALICE, workspace, v1.id(), v1.rowVersion(), 0, v1Build.id(), "p5-v1-publish");
        assertThat(firstRelease.contentHash()).isEqualTo(v1.contentHash());

        // 把固定发布事实置于耗尽自动重试的合成状态，验证 Owner 重放仍保留原事件和 payload。
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, 'knowledge:outbox:replay', 'ACTIVE') on conflict do nothing",
                Ids.TENANT_A, workspace, Ids.ALICE);
        var releaseEvent = jdbc.queryForObject("select id from knowledge.publication_event where document_id = ? and idempotency_key = 'p5-v1-publish'",
                UUID.class, v1.id());
        var originalPayload = jdbc.queryForObject("select payload::text from knowledge.outbox where event_id = ?", String.class, releaseEvent);
        jdbc.update("update knowledge.outbox set status = 'FAILED', attempt_count = 6, next_attempt_at = now() - interval '1 second' where event_id = ?",
                releaseEvent);
        var replayKey = "p7-knowledge-replay-" + UUID.randomUUID();
        var replayReceipt = knowledge.replayOutbox(ALICE, workspace, releaseEvent, replayKey, "本域审计投递故障恢复");
        assertThat(replayReceipt.status()).isEqualTo("PENDING");
        assertThat(replayReceipt.attempts()).isEqualTo(6);
        assertThat(jdbc.queryForObject("select payload::text from knowledge.outbox where event_id = ?", String.class, releaseEvent))
                .isEqualTo(originalPayload);
        assertThat(knowledge.replayOutbox(ALICE, workspace, releaseEvent, replayKey, "本域审计投递故障恢复").replayed()).isTrue();
        assertThatThrownBy(() -> knowledge.replayOutbox(ALICE, workspace, releaseEvent, replayKey, "异参重放"))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("KNOWLEDGE_COMMAND_CONFLICT"));
        new KnowledgeOutboxPublisher(jdbc, audit, transactionManager).publish();
        assertThat(jdbc.queryForObject("select status from knowledge.outbox where event_id = ?", String.class, releaseEvent))
                .isEqualTo("DELIVERED");
        assertThat(jdbc.queryForObject("select attempt_count from knowledge.outbox where event_id = ?", Integer.class, releaseEvent))
                .isEqualTo(6);

        var v2Content = "正式知识第二版，等待基线确认后切换。";
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/versions", workspace, v1.id())
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-v2-create")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedRowVersion\":" + firstRelease.documentRowVersion() + ",\"content\":\"" + v2Content + "\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.version").value(2));
        var v2 = knowledge.getVersion(ALICE, workspace, v1.id(), 2);
        mvc.perform(get("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/versions/{assetVersion}",
                        workspace, v1.id(), 2)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content").value(v2Content));
        assertThat(v2.version()).isEqualTo(2);
        assertThat(knowledge.createVersion(new CreateKnowledgeVersionCommand(ALICE, workspace, v1.id(),
                firstRelease.documentRowVersion(), v2.content(), "p5-v2-create")).version()).isEqualTo(2);
        var v2Chunks = knowledge.chunk(ALICE, workspace, v1.id(), v2.version(), "p3-plain-1");
        var v2Build = knowledge.buildIndex(ALICE, workspace, v1.id(), v2.version(), "p3-plain-1", "p5-v2-index");
        assertThat(v2Build.status()).isEqualTo("READY");
        assertThat(v2Build.assetVersion()).isEqualTo(2);
        assertThat(v2Chunks).isNotEmpty().allSatisfy(chunk ->
                assertThat(chunk.contentHash()).isEqualTo(Hashing.sha256(chunk.content())));

        assertSearchShows(workspace, v1.id(), 1);
        assertThat(jdbc.queryForObject("select asset_version from knowledge.document_publication where document_id = ? and status = 'ACTIVE'",
                Integer.class, v1.id())).isEqualTo(1);
        assertThatThrownBy(() -> knowledge.publish(ALICE, workspace, v1.id(), v2.rowVersion(), 0,
                v2Build.id(), "p5-v2-wrong-base"))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("BASE_VERSION_CONFLICT"));
        assertSearchShows(workspace, v1.id(), 1);

        var secondRelease = knowledge.publish(ALICE, workspace, v1.id(), v2.rowVersion(), 1,
                v2Build.id(), "p5-v2-publish");
        assertThat(secondRelease.assetVersion()).isEqualTo(2);
        assertThat(secondRelease.contentHash()).isEqualTo(v2.contentHash());
        assertSearchShows(workspace, v1.id(), 2);
        assertThat(knowledge.isUsable(ALICE, workspace, v1.id(), 1, v1Chunks.getFirst().id(),
                v1Build.id(), v1Chunks.getFirst().contentHash())).isTrue();

        var thirdRowVersion = secondRelease.documentRowVersion();
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/versions/{assetVersion}/revoke",
                        workspace, v1.id(), 1)
                        .param("expectedVersion", Long.toString(thirdRowVersion))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-v1-retire"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.action").value("REVOKED"))
                .andExpect(jsonPath("$.assetVersion").value(1));
        assertThat(knowledge.isUsable(ALICE, workspace, v1.id(), 1, v1Chunks.getFirst().id(),
                v1Build.id(), v1Chunks.getFirst().contentHash())).isFalse();
        assertThat(jdbc.queryForObject("select asset_version from knowledge.document_publication where document_id = ? and status = 'ACTIVE'",
                Integer.class, v1.id())).isEqualTo(2);
        assertThat(knowledge.getCurrentPublication(ALICE, workspace, v1.id()).assetVersion()).isEqualTo(2);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.outbox where event_type = 'eaf.knowledge.version-published.v1'",
                Integer.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.outbox where event_type = 'eaf.knowledge.version-revoked.v1'",
                Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.publication_event where document_id = ? and content_hash = ?",
                Integer.class, v1.id(), v2.contentHash())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select build_id from knowledge.publication_event where document_id = ? and idempotency_key = 'p5-v1-retire'",
                UUID.class, v1.id())).isEqualTo(v1Build.id());

        mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/versions/{assetVersion}/revoke",
                        workspace, v1.id(), 2)
                        .param("expectedVersion", Long.toString(thirdRowVersion + 1))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-v2-retire"))
                .andExpect(status().isOk());
        assertThat(jdbc.queryForObject("select status from knowledge.document where id = ?", String.class, v1.id()))
                .isEqualTo("DRAFT");
        assertThat(jdbc.queryForObject("select status from knowledge.document_publication where document_id = ?",
                String.class, v1.id())).isEqualTo("REVOKED");
        assertThat(knowledge.isUsable(ALICE, workspace, v1.id(), 2, v2Chunks.getFirst().id(),
                v2Build.id(), v2Chunks.getFirst().contentHash())).isFalse();
    }

    private void assertSearchShows(java.util.UUID workspace, java.util.UUID documentId, int expectedVersion) {
        var hits = knowledge.search(ALICE, workspace, "正式知识版本内容", 10).hits().stream()
                .filter(hit -> hit.documentId().equals(documentId)).toList();
        assertThat(hits).isNotEmpty().allSatisfy(hit -> assertThat(hit.documentVersion()).isEqualTo(expectedVersion));
    }

    private void upgradeV60PublicationAndVerifyP3Read() {
        var documentId = UUID.fromString("59000000-0000-4000-8000-000000000001");
        var buildId = UUID.fromString("59000000-0000-4000-8000-000000000002");
        var eventId = UUID.fromString("59000000-0000-4000-8000-000000000003");
        var now = Timestamp.from(Instant.now());
        var content = "旧版知识升级后仍可读取。";
        var signature = Hashing.sha256("deterministic|p3-test-embedding-8|UNKNOWN|8|p3-plain-1|COSINE");
        jdbc.update("insert into knowledge.document(id, tenant_id, workspace_id, owner_id, title, source_ref, status, idempotency_key, request_hash, row_version, created_at, updated_at) values (?, ?, ?, ?, ?, ?, 'PUBLISHED', ?, ?, 2, ?, ?)",
                documentId, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE, "升级文档", "manual://p3-upgrade", "p5-p3-upgrade",
                Hashing.sha256("legacy-request"), now, now);
        jdbc.update("insert into knowledge.document_version(id, tenant_id, workspace_id, document_id, asset_version, content, content_hash, status, created_at) values (?, ?, ?, ?, 1, ?, ?, 'PUBLISHED', ?)",
                UUID.randomUUID(), Ids.TENANT_A, Ids.WORKSPACE_A, documentId, content, Hashing.sha256(content), now);
        jdbc.update("insert into knowledge.document_permission(tenant_id, workspace_id, document_id, actor_id, action, status) values (?, ?, ?, ?, 'knowledge:read', 'ACTIVE')",
                Ids.TENANT_A, Ids.WORKSPACE_A, documentId, Ids.ALICE);
        jdbc.update("insert into knowledge.index_build(id, tenant_id, workspace_id, document_id, asset_version, chunking_version, provider, model, model_revision, dimension, distance_metric, configuration_signature, idempotency_key, status, total_chunks, completed_chunks, next_chunk_order, input_tokens, embedding_calls, created_at, updated_at) values (?, ?, ?, ?, 1, 'p3-plain-1', 'deterministic', 'p3-test-embedding-8', 'UNKNOWN', 8, 'COSINE', ?, 'legacy-build', 'READY', 1, 1, 2, 0, 0, ?, ?)",
                buildId, Ids.TENANT_A, Ids.WORKSPACE_A, documentId, signature, now, now);
        jdbc.update("insert into knowledge.document_publication(tenant_id, workspace_id, document_id, asset_version, build_id, status, row_version, updated_at) values (?, ?, ?, 1, ?, 'ACTIVE', 1, ?)",
                Ids.TENANT_A, Ids.WORKSPACE_A, documentId, buildId, now);
        jdbc.update("insert into knowledge.publication_event(id, tenant_id, workspace_id, document_id, asset_version, build_id, action, document_status, document_row_version, idempotency_key, request_hash, occurred_at) values (?, ?, ?, ?, 1, ?, 'PUBLISHED', 'PUBLISHED', 2, 'legacy-publish', ?, ?)",
                eventId, Ids.TENANT_A, Ids.WORKSPACE_A, documentId, buildId, Hashing.sha256("legacy-publish"), now);

        // 先验证旧数据跨过发布事实迁移，再把 schema 升到当前版本供现行服务继续读写。
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").defaultSchema("eaf_meta")
                .schemas("eaf_meta").load().migrate();
        assertThat(jdbc.queryForObject("select content_hash from knowledge.publication_event where id = ?", String.class, eventId))
                .isEqualTo(Hashing.sha256(content));
        assertThat(knowledge.get(ALICE, Ids.WORKSPACE_A, documentId).content()).isEqualTo(content);
        assertThat(knowledge.getCurrentPublication(ALICE, Ids.WORKSPACE_A, documentId).contentHash())
                .isEqualTo(Hashing.sha256(content));
    }
}
