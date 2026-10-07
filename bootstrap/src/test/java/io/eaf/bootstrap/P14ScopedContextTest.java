package io.eaf.bootstrap;

import io.eaf.context.api.ContextQuery;
import io.eaf.context.api.ContextService;
import io.eaf.context.api.ScopedContextQueryService;
import io.eaf.knowledge.api.CreateKnowledgeDocumentCommand;
import io.eaf.knowledge.api.CreateKnowledgeVersionCommand;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.memory.api.CreateMemoryCommand;
import io.eaf.memory.api.MemoryService;
import io.eaf.provisioning.api.MembershipProvisioning;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceCatalog;
import io.eaf.workspace.api.WorkspaceKind;
import io.eaf.workspace.api.WorkspaceMembershipAdministration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
//  只检查新增的 Workspace/共享 Context 路径与个人空间停用，不启动演示页面。
class P14ScopedContextTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final ActorContext BOB = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final String SHARED_CONTENT = "P14-SHARED-SENTINEL：服务发布版本的操作规范。";
    private static final String MEMORY_CONTENT = "P14-MEMORY-SENTINEL：团队经验的通用操作步骤。";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired WorkspaceCatalog catalog;
    @Autowired WorkspaceAuthorization authorization;
    @Autowired WorkspaceMembershipAdministration memberships;
    @Autowired KnowledgeService knowledge;
    @Autowired MemoryService memories;
    @Autowired ContextService contexts;
    @Autowired ScopedContextQueryService scopedContexts;
    @Autowired MembershipProvisioning provisioning;

    @Test
    void createsLayeredWorkspacesSharesPublishedContextAndClosesPrivateSpaceOnOffboarding() throws Exception {
        // 初始 tenant administrator 来自受保护部署流程；测试显式预置该已批准身份。
        jdbc.update("insert into organization.tenant_admin(tenant_id, subject_id, status) values (?, ?, 'ACTIVE') "
                        + "on conflict (tenant_id, subject_id) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.ALICE);

        var enterprise = create("enterprise", WorkspaceKind.ENTERPRISE, null);
        var department = create("department", WorkspaceKind.DEPARTMENT, enterprise.workspaceId());
        var project = create("project", WorkspaceKind.PROJECT, department.workspaceId());
        var alicePersonal = create("Alice private", WorkspaceKind.PERSONAL, null);
        assertThat(enterprise.kind()).isEqualTo(WorkspaceKind.ENTERPRISE);
        assertThat(department.parentWorkspaceId()).isEqualTo(enterprise.workspaceId());
        assertThat(project.parentWorkspaceId()).isEqualTo(department.workspaceId());
        assertThat(alicePersonal.ownerId()).isEqualTo(Ids.ALICE);
        assertThatThrownBy(() -> catalog.create(ALICE, UUID.randomUUID(), "second Alice private",
                WorkspaceKind.PERSONAL, null))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("PERSONAL_WORKSPACE_EXISTS"));
        assertThatThrownBy(() -> memberships.grant(ALICE, alicePersonal.workspaceId(), Ids.BOB,
                Set.of("context:read")))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("POLICY_DENIED"));
        // 模拟旧数据或绕过 Owner 写入的恶意授权行，直接读取 API 仍须执行 Personal 所有者校验。
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, 'context:read', 'ACTIVE') on conflict (workspace_id, actor_id, action) "
                        + "do update set status = 'ACTIVE'",
                Ids.TENANT_A, alicePersonal.workspaceId(), Ids.BOB);
        assertThat(authorization.isAuthorized(Ids.TENANT_A, Ids.BOB, alicePersonal.workspaceId(), "context:read")).isFalse();
        assertThatThrownBy(() -> authorization.require(BOB, alicePersonal.workspaceId(), "context:read"))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("RESOURCE_NOT_FOUND"));

        grant(Ids.BOB, project.workspaceId(), "context:read", "knowledge:read");
        grant(Ids.BOB, department.workspaceId(), "context:read", "knowledge:read");
        assertThat(jdbc.queryForObject("select tenant_id from workspace.workspace where id = ?",
                UUID.class, Ids.WORKSPACE_B)).isEqualTo(Ids.TENANT_B);
        assertThatThrownBy(() -> catalog.resolveContextSources(BOB, project.workspaceId(), List.of(Ids.WORKSPACE_B)))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("RESOURCE_NOT_FOUND"));
        assertThatThrownBy(() -> catalog.replaceContextSources(BOB, project.workspaceId(), List.of(alicePersonal.workspaceId())))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("RESOURCE_NOT_FOUND"));
        var departmentDocument = publish(department.workspaceId(), "department source");
        var projectDocument = publish(project.workspaceId(), "project source");
        mvc.perform(put("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/context-shares/{recipientId}",
                        department.workspaceId(), departmentDocument, Ids.BOB)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("documentVersion", 1, "expectedVersion", 0))))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.effective").value(true));
        var deptShare = knowledge.listContextShares(ALICE, department.workspaceId(), departmentDocument, 20, 0)
                .items().getFirst();
        knowledge.shareContext(ALICE, project.workspaceId(), projectDocument, Ids.BOB, 1, 0);
        assertThat(deptShare.effective()).isTrue();
        assertThat(deptShare.rowVersion()).isEqualTo(1);

        // Memory 继续由现有 Memory/Workspace 发布和授权规则管理； 只从已授权 GENERAL 来源聚合读取。
        for (var action : List.of("context:read", "knowledge:read", "memory:read"))
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                            + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) "
                            + "do update set status = 'ACTIVE'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB, action);
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, 'workspace:members:manage', 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        var teamMemory = memories.create(new CreateMemoryCommand(ALICE, Ids.WORKSPACE_A, "p14-team-guidance",
                "1.0.0", "SEMANTIC", "TEAM", MEMORY_CONTENT, 0.9, Instant.now().plusSeconds(86_400),
                "manual:p14-team-guidance", List.of("p14-test:memory")));
        memories.publish(ALICE, Ids.WORKSPACE_A, teamMemory.id(), teamMemory.version(), teamMemory.rowVersion());

        var preference = catalog.replaceContextSources(BOB, project.workspaceId(),
                List.of(department.workspaceId(), Ids.WORKSPACE_A));
        assertThat(preference.selectedSourceWorkspaceIds()).containsExactly(department.workspaceId(), Ids.WORKSPACE_A);
        assertThat(preference.sources()).extracting(source -> source.workspaceId())
                .containsSubsequence(project.workspaceId(), department.workspaceId(), Ids.WORKSPACE_A);
        mvc.perform(get("/api/v1/workspaces").param("kind", "PROJECT")
                        .header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath(
                        "$.items[0].kind").value("PROJECT"));
        mvc.perform(put("/api/v1/workspaces/{workspaceId}/context/sources", project.workspaceId())
                        .header("Authorization", "Bearer eaf-local-bob")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("sourceWorkspaceIds",
                                List.of(department.workspaceId(), Ids.WORKSPACE_A)))))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get(
                        "/api/v1/workspaces/{workspaceId}/context/sources", project.workspaceId())
                        .header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath(
                        "$.selectedSourceWorkspaceIds[0]").value(department.workspaceId().toString()));
        var scoped = scopedContexts.query(BOB, project.workspaceId(), SHARED_CONTENT, null, null, null);
        assertThat(scoped.status()).isEqualTo("READY");
        assertThat(scoped.items()).hasSize(2);
        var sharedItem = scoped.items().stream().filter(item -> item.content().equals(SHARED_CONTENT)).findFirst().orElseThrow();
        var memoryItem = scoped.items().stream().filter(item -> item.content().equals(MEMORY_CONTENT)).findFirst().orElseThrow();
        assertThat(sharedItem.origins()).hasSize(2);
        assertThat(sharedItem.origins()).extracting(origin -> origin.accessPath()).containsOnly("SHARE");
        assertThat(memoryItem.origins()).hasSize(1);
        assertThat(memoryItem.origins().getFirst().sourceWorkspaceId()).isEqualTo(Ids.WORKSPACE_A);
        assertThat(memoryItem.origins().getFirst().memoryVersion()).isEqualTo("1.0.0");
        assertThat(scoped.omitted().duplicateCount()).isEqualTo(1);
        assertThat(scoped.usedTokens()).isLessThanOrEqualTo(scoped.tokenBudget());
        var exhausted = scopedContexts.query(BOB, project.workspaceId(), SHARED_CONTENT, 5, 1, null);
        assertThat(exhausted.status()).isEqualTo("BUDGET_EXHAUSTED");
        assertThat(exhausted.usedTokens()).isZero();
        assertThat(exhausted.omitted().tokenBudgetCount()).isGreaterThan(0);
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post(
                        "/api/v1/workspaces/{workspaceId}/context/scoped-queries", project.workspaceId())
                        .header("Authorization", "Bearer eaf-local-bob")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("query", SHARED_CONTENT))))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath(
                        "$.items[0].origins.length()").value(2));

        // 旧 Context 仍只查本 Workspace 的直接文档权限，不继承新增分享。
        var legacy = contexts.query(BOB, project.workspaceId(), new ContextQuery(SHARED_CONTENT, 5, 2_000));
        assertThat(legacy.items()).noneMatch(item -> SHARED_CONTENT.equals(item.content()));

        // 正式版更新不会扩大旧共享；旧版本请求不能将共享静默迁移到新内容。
        var currentDepartmentDocument = knowledge.get(ALICE, department.workspaceId(), departmentDocument);
        var revised = knowledge.createVersion(new CreateKnowledgeVersionCommand(ALICE, department.workspaceId(),
                departmentDocument, currentDepartmentDocument.rowVersion(),
                "第二版：共享不会自动随正式内容更新。", "p14-department-v2"));
        knowledge.chunk(ALICE, department.workspaceId(), departmentDocument, revised.version(), "p3-plain-1");
        var revisedBuild = knowledge.buildIndex(ALICE, department.workspaceId(), departmentDocument, revised.version(),
                "p3-plain-1", "p14-department-v2-build");
        assertThat(revisedBuild.status()).isEqualTo("READY");
        knowledge.publish(ALICE, department.workspaceId(), departmentDocument, revised.rowVersion(), 1,
                revisedBuild.id(), "p14-department-v2-publish");
        assertThat(knowledge.listContextShares(ALICE, department.workspaceId(), departmentDocument, 20, 0)
                .items().getFirst()).satisfies(share -> {
                    assertThat(share.effective()).isFalse();
                    assertThat(share.validityReason()).isEqualTo("PUBLICATION_CHANGED");
                });
        assertThatThrownBy(() -> knowledge.shareContext(ALICE, department.workspaceId(), departmentDocument,
                Ids.BOB, 1, deptShare.rowVersion()))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("PUBLICATION_NOT_CURRENT"));
        var revoked = knowledge.revokeContextShare(ALICE, department.workspaceId(), departmentDocument,
                Ids.BOB, deptShare.rowVersion());
        assertThat(revoked.status()).isEqualTo("REVOKED");
        var afterRevoke = scopedContexts.query(BOB, project.workspaceId(), SHARED_CONTENT, null, null, null);
        assertThat(afterRevoke.items()).hasSize(2);
        assertThat(afterRevoke.items()).extracting(item -> item.origins().getFirst().sourceWorkspaceId())
                .containsExactly(project.workspaceId(), Ids.WORKSPACE_A);
        assertThat(knowledge.listContextShares(ALICE, department.workspaceId(), departmentDocument, 20, 0)
                .items().getFirst()).satisfies(share -> {
                    assertThat(share.effective()).isFalse();
                    assertThat(share.validityReason()).isEqualTo("REVOKED");
                });
        knowledge.revokeContextShare(ALICE, project.workspaceId(), projectDocument, Ids.BOB, 1);
        memberships.revoke(ALICE, Ids.WORKSPACE_A, Ids.BOB, Set.of("context:read"));
        var noCurrentSources = scopedContexts.query(BOB, project.workspaceId(), SHARED_CONTENT, null, null, null);
        assertThat(noCurrentSources.status()).isEqualTo("NO_EVIDENCE");
        assertThat(noCurrentSources.unavailableSourceCount()).isEqualTo(1);

        // Bob 的个人空间及来源偏好在同一停用事务清理；共享空间管理员检查仍保留。
        var bobPersonal = catalog.create(BOB, UUID.randomUUID(), "Bob private", WorkspaceKind.PERSONAL, null).workspace();
        catalog.replaceContextSources(BOB, project.workspaceId(), java.util.List.of(bobPersonal.workspaceId()));
        provisioning.offboardMember(ALICE, Ids.TENANT_A, Ids.BOB);
        assertThat(jdbc.queryForObject("select status from workspace.workspace where id = ?", String.class,
                bobPersonal.workspaceId())).isEqualTo("DISABLED");
        assertThat(jdbc.queryForObject("select count(*) from workspace.context_source_preference where tenant_id = ? and actor_id = ?",
                Integer.class, Ids.TENANT_A, Ids.BOB)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and status = 'ACTIVE'",
                Integer.class, Ids.TENANT_A, bobPersonal.workspaceId())).isZero();
    }

    private io.eaf.workspace.api.WorkspaceSummary create(String name, WorkspaceKind kind, UUID parentWorkspaceId)
            throws Exception {
        var workspaceId = UUID.randomUUID();
        var body = new java.util.LinkedHashMap<String, Object>();
        body.put("name", name);
        body.put("kind", kind.name());
        body.put("parentWorkspaceId", parentWorkspaceId == null ? null : parentWorkspaceId.toString());
        var response = mvc.perform(put("/api/v1/workspaces/{workspaceId}", workspaceId)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(body)))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return json.readValue(response, io.eaf.workspace.api.WorkspaceSummary.class);
    }

    private void grant(UUID subjectId, UUID workspaceId, String... actions) throws Exception {
        mvc.perform(put("/api/v1/workspaces/{workspaceId}/members/{subjectId}/grants", workspaceId, subjectId)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("actions", List.of(actions)))))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk());
    }

    private UUID publish(UUID workspaceId, String key) throws Exception {
        var document = knowledge.create(new CreateKnowledgeDocumentCommand(ALICE, workspaceId, key,
                "manual://" + key.replace(' ', '-'), SHARED_CONTENT, Map.of(), key + "-create", key + "-trace"));
        knowledge.chunk(ALICE, workspaceId, document.id(), 1, "p3-plain-1");
        var build = knowledge.buildIndex(ALICE, workspaceId, document.id(), 1, "p3-plain-1", key + "-build");
        assertThat(build.status()).isEqualTo("READY");
        knowledge.publish(ALICE, workspaceId, document.id(), document.rowVersion(), 0,
                build.id(), key + "-publish");
        return document.id();
    }
}
