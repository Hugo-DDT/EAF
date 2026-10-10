package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.context.api.ContextQuery;
import io.eaf.context.api.ContextService;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.api.ManagedKnowledgeSource;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
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
import org.springframework.http.MediaType;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
                "eaf.knowledge.outbox-publisher-enabled=false", "eaf.model.mode=deterministic",
                "eaf.security.mode=local"})
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class P28ManagedKnowledgeTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID WORKSPACE = Ids.WORKSPACE_A;
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final String OWNER_TOKEN = "eaf-local-alice";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
    }

    @Autowired KnowledgeService knowledge;
    @Autowired ContextService contexts;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;

    @Test
    void managesStableSourceVersionsAndStopsStaleOrUnauthorizedKnowledge() throws Exception {
        var sourceJson = mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/sources", WORKSPACE)
                        .header("Authorization", "Bearer " + OWNER_TOKEN)
                        .header("Idempotency-Key", "p28-source-create")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(Map.of("name", "设备维护资料", "type", "MANAGED_TEXT_V1"))))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        var source = json.readValue(sourceJson, ManagedKnowledgeSource.class);
        var sourceId = source.id();
        assertThat(knowledge.createManagedSource(new ManagedKnowledgeSource.CreateCommand(ALICE, WORKSPACE,
                "设备维护资料", "MANAGED_TEXT_V1", "p28-source-create")).id()).isEqualTo(sourceId);

        var original = "# 设备维护\n设备编号 EAF-OPS-2048 的蓝色告警处理规范。" + "现场检查记录和设备状态说明。".repeat(90)
                + "\n\n确认条件：只有现场压力持续超过 7 bar 且蓝色告警连续 3 分钟，才执行停机处置。".repeat(24);
        var first = sync(sourceId, 1, "p28-sync-1", upsert("revision-1", "acl-1", original, Set.of(Ids.ALICE)));
        var documentId = first.items().getFirst().documentId();
        assertThat(first.items().getFirst().result()).isEqualTo("CREATED_DRAFT");
        var firstVersion = first.items().getFirst().documentVersion();
        var firstBuild = publish(sourceId, documentId, firstVersion, 0, "p28-index-1", "p28-publish-1");

        var businessNumber = knowledge.search(ALICE, WORKSPACE, "EAF-OPS-2048", 10, null, "HYBRID").hits().stream()
                .filter(hit -> hit.documentId().equals(documentId)).toList();
        var chineseTerm = knowledge.search(ALICE, WORKSPACE, "蓝色告警 压力阈值", 10, null, "HYBRID").hits().stream()
                .filter(hit -> hit.documentId().equals(documentId)).toList();
        assertThat(businessNumber).isNotEmpty().anySatisfy(hit -> assertThat(hit.channels()).contains("LEXICAL"));
        assertThat(chineseTerm).isNotEmpty();

        var chunks = knowledge.chunk(ALICE, WORKSPACE, documentId, firstVersion, "p9-structure-1");
        assertThat(chunks).hasSizeGreaterThan(2);
        var neighborhood = knowledge.neighborhood(new ManagedKnowledgeSource.NeighborhoodRequest(ALICE, WORKSPACE,
                documentId, firstVersion, firstBuild.id(), chunks.get(1).id(), 2000));
        assertThat(neighborhood.items()).extracting(ManagedKnowledgeSource.NeighborhoodItem::relation)
                .containsExactly("PREVIOUS", "SEED", "NEXT");
        assertThat(neighborhood.items()).allSatisfy(item -> {
            assertThat(item.chunk().documentId()).isEqualTo(documentId);
            assertThat(item.chunk().documentVersion()).isEqualTo(firstVersion);
            assertThat(item.chunk().contentHash()).isNotBlank();
        });

        var unchanged = sync(sourceId, 2, "p28-sync-2", upsert("revision-2", "acl-1", original, Set.of(Ids.ALICE)));
        assertThat(unchanged.items().getFirst().result()).isEqualTo("UNCHANGED");
        assertThat(unchanged.items().getFirst().documentVersion()).isEqualTo(firstVersion);
        assertThat(knowledge.getManagedSourceVersion(ALICE, WORKSPACE, documentId, firstVersion))
                .satisfies(version -> {
                    assertThat(version.sourceVersion()).isEqualTo("revision-1");
                    assertThat(version.currentSourceVersion()).isEqualTo("revision-2");
                });
        assertThat(knowledge.applyManagedSourceSync(new ManagedKnowledgeSource.BatchCommand(ALICE, WORKSPACE,
                sourceId, 2, List.of(upsert("revision-2", "acl-1", original, Set.of(Ids.ALICE))), "p28-sync-2")).replayed())
                .isTrue();

        var revised = original + "\n\n新增处置规则：达到 9 bar 时立即停机并上报。";
        var second = sync(sourceId, 3, "p28-sync-3", upsert("revision-3", "acl-1", revised, Set.of(Ids.ALICE)));
        assertThat(second.items().getFirst().result()).isEqualTo("UPDATED_DRAFT");
        var secondVersion = second.items().getFirst().documentVersion();
        assertThat(knowledge.search(ALICE, WORKSPACE, "EAF-OPS-2048", 10).hits())
                .noneMatch(hit -> hit.documentId().equals(documentId));
        assertThatThrownBy(() -> knowledge.get(ALICE, WORKSPACE, documentId)).isInstanceOf(EafException.class);
        var secondBuild = publish(sourceId, documentId, secondVersion, firstVersion, "p28-index-2", "p28-publish-2");
        assertThat(knowledge.isUsable(ALICE, WORKSPACE, documentId, secondVersion,
                knowledge.chunk(ALICE, WORKSPACE, documentId, secondVersion, "p9-structure-1").getFirst().id(),
                secondBuild.id(), knowledge.chunk(ALICE, WORKSPACE, documentId, secondVersion, "p9-structure-1").getFirst().contentHash()))
                .isTrue();
        var currentContext = contexts.query(ALICE, WORKSPACE, new ContextQuery("EAF-OPS-2048", 5, 2000));
        assertThat(currentContext.items()).anySatisfy(item -> assertThat(item.documentId()).isEqualTo(documentId));
        assertThat(contexts.isCurrent(ALICE, WORKSPACE, currentContext)).isTrue();

        // 内容回到已发布旧版本的 hash 时，仍须匹配当前 publication 指针，不能列出旧版内容。
        var reverted = sync(sourceId, 4, "p28-sync-4", upsert("revision-4", "acl-1", original, Set.of(Ids.ALICE)));
        assertThat(reverted.items().getFirst().result()).isEqualTo("UPDATED_DRAFT");
        assertThat(knowledge.listDocuments(ALICE, WORKSPACE, null, null, 50).items())
                .noneMatch(item -> item.id().equals(documentId));
        assertThat(knowledge.search(ALICE, WORKSPACE, "EAF-OPS-2048", 10).hits())
                .noneMatch(hit -> hit.documentId().equals(documentId));

        // 源正文恢复为当前已发布内容后可继续使用，但草稿仍保持未发布状态。
        var restoredBody = sync(sourceId, 5, "p28-sync-5", upsert("revision-5", "acl-1", revised, Set.of(Ids.ALICE)));
        assertThat(restoredBody.items().getFirst().result()).isEqualTo("UPDATED_DRAFT");
        assertThat(knowledge.get(ALICE, WORKSPACE, documentId).content()).isEqualTo(revised);
        assertThat(knowledge.listDocuments(ALICE, WORKSPACE, null, null, 50).items())
                .anySatisfy(item -> assertThat(item.id()).isEqualTo(documentId));

        var buildCount = jdbc.queryForObject("select count(*) from knowledge.index_build where document_id = ?", Integer.class, documentId);
        var accessRemoved = sync(sourceId, 6, "p28-sync-6", new ManagedKnowledgeSource.Change("ACCESS", "device-2048",
                null, null, null, null, "acl-2", Set.of(), null));
        assertThat(accessRemoved.items().getFirst().sourceVersion()).isEqualTo("revision-5");
        assertThat(jdbc.queryForObject("select count(*) from knowledge.index_build where document_id = ?", Integer.class, documentId))
                .isEqualTo(buildCount);
        assertThat(knowledge.search(ALICE, WORKSPACE, "EAF-OPS-2048", 10).hits())
                .noneMatch(hit -> hit.documentId().equals(documentId));
        assertThatThrownBy(() -> knowledge.get(ALICE, WORKSPACE, documentId)).isInstanceOf(EafException.class);
        assertThat(knowledge.isUsable(ALICE, WORKSPACE, documentId, secondVersion,
                knowledge.chunk(ALICE, WORKSPACE, documentId, secondVersion, "p9-structure-1").getFirst().id(),
                secondBuild.id(), knowledge.chunk(ALICE, WORKSPACE, documentId, secondVersion, "p9-structure-1").getFirst().contentHash()))
                .isFalse();
        assertThat(contexts.isCurrent(ALICE, WORKSPACE, currentContext)).isFalse();
        assertThat(knowledge.applyManagedSourceSync(new ManagedKnowledgeSource.BatchCommand(ALICE, WORKSPACE,
                sourceId, 6, List.of(new ManagedKnowledgeSource.Change("ACCESS", "device-2048", null, null, null,
                        null, "acl-2", Set.of(), null)), "p28-sync-6")).replayed()).isTrue();

        sync(sourceId, 7, "p28-sync-7", new ManagedKnowledgeSource.Change("ACCESS", "device-2048", null,
                null, null, null, "acl-3", Set.of(Ids.ALICE), null));
        var unavailable = sync(sourceId, 8, "p28-sync-8", new ManagedKnowledgeSource.Change("UNAVAILABLE",
                "device-2048", null, null, null, null, null, null, "SOURCE_TIMEOUT"));
        assertThat(unavailable.items().getFirst().sourceVersion()).isEqualTo("revision-5");
        assertThatThrownBy(() -> knowledge.get(ALICE, WORKSPACE, documentId)).isInstanceOf(EafException.class);
        var recovered = sync(sourceId, 9, "p28-sync-9", upsert("revision-6", "acl-3", revised, Set.of(Ids.ALICE)));
        assertThat(recovered.items().getFirst().result()).isEqualTo("UNCHANGED");
        assertThat(knowledge.get(ALICE, WORKSPACE, documentId).content()).isEqualTo(revised);

        var deleted = sync(sourceId, 10, "p28-sync-10", new ManagedKnowledgeSource.Change("DELETE", "device-2048",
                "revision-7", null, null, null, null, null, null));
        assertThat(deleted.items().getFirst().availability()).isEqualTo("DELETED");
        assertThatThrownBy(() -> knowledge.get(ALICE, WORKSPACE, documentId)).isInstanceOf(EafException.class);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.document where id = ?", Integer.class, documentId)).isEqualTo(1);

        sync(sourceId, 11, "p28-sync-11", upsert("revision-8", "acl-3", revised, Set.of(Ids.ALICE)));
        var disabled = knowledge.changeManagedSourceState(ALICE, WORKSPACE, sourceId, 12, "DISABLED", "p28-state-disable");
        assertThat(disabled.sourceRevision()).isEqualTo(13);
        assertThatThrownBy(() -> knowledge.get(ALICE, WORKSPACE, documentId)).isInstanceOf(EafException.class);
        assertThat(knowledge.changeManagedSourceState(ALICE, WORKSPACE, sourceId, 12, "DISABLED",
                "p28-state-disable").replayed()).isTrue();
        assertThat(knowledge.changeManagedSourceState(ALICE, WORKSPACE, sourceId, 13, "ACTIVE",
                "p28-state-enable").sourceRevision()).isEqualTo(14);
        assertThat(knowledge.get(ALICE, WORKSPACE, documentId).content()).isEqualTo(revised);
    }

    private ManagedKnowledgeSource.Change upsert(String sourceVersion, String aclVersion, String content,
                                                 Set<UUID> readers) {
        return new ManagedKnowledgeSource.Change("UPSERT", "device-2048", sourceVersion, "设备 2048 处置规范",
                "MARKDOWN", content, aclVersion, readers, null);
    }

    private ManagedKnowledgeSource.SyncReceipt sync(UUID sourceId, long revision, String key,
                                                     ManagedKnowledgeSource.Change change) {
        return knowledge.applyManagedSourceSync(new ManagedKnowledgeSource.BatchCommand(ALICE, WORKSPACE,
                sourceId, revision, List.of(change), key));
    }

    private io.eaf.knowledge.api.KnowledgeIndexBuild publish(UUID sourceId, UUID documentId, int version,
                                                              int baseVersion, String indexKey, String publishKey) {
        var document = knowledge.getManagedSourceVersionForOwner(ALICE, WORKSPACE, sourceId, "device-2048", version);
        knowledge.chunk(ALICE, WORKSPACE, documentId, version, "p9-structure-1");
        var build = knowledge.buildIndex(ALICE, WORKSPACE, documentId, version, "p9-structure-1", indexKey);
        assertThat(build.status()).isEqualTo("READY");
        knowledge.publish(ALICE, WORKSPACE, documentId, document.rowVersion(), baseVersion, build.id(), publishKey);
        return build;
    }
}
