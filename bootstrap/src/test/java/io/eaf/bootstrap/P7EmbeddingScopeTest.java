package io.eaf.bootstrap;

import io.eaf.context.api.ContextQuery;
import io.eaf.context.api.ContextTaskScope;
import io.eaf.context.infrastructure.EnterpriseContextService;
import io.eaf.audit.api.AuditPort;
import io.eaf.knowledge.api.KnowledgeSearchResult;
import io.eaf.knowledge.api.KnowledgeSearchScope;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.infrastructure.JdbcKnowledgeService;
import io.eaf.memory.api.MemoryService;
import io.eaf.model.api.EmbeddingGateway;
import io.eaf.model.api.EmbeddingProfile;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import org.springframework.jdbc.core.JdbcTemplate;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 核实 Runtime 根 Task 归属只由服务端路径传入 Knowledge。 */
class P7EmbeddingScopeTest {
    @Test
    void runtimeContextSharesRootSpendScopeWithKnowledgeEmbedding() {
        var tenant = UUID.randomUUID();
        var workspace = UUID.randomUUID();
        var actorId = UUID.randomUUID();
        var taskId = UUID.randomUUID();
        var rootId = UUID.randomUUID();
        var runId = UUID.randomUUID();
        var actor = new ActorContext(actorId, tenant, ActorType.HUMAN, Set.of());
        var knowledge = mock(KnowledgeService.class);
        var memories = mock(MemoryService.class);
        var workspaces = mock(WorkspaceAuthorization.class);
        when(workspaces.isAuthorized(tenant, actorId, workspace, "knowledge:read")).thenReturn(true);
        when(knowledge.search(eq(actor), eq(workspace), eq("renewal"), eq(1), any(KnowledgeSearchScope.class), eq("VECTOR")))
                .thenReturn(new KnowledgeSearchResult(1, List.of()));
        var context = new EnterpriseContextService(knowledge, memories, workspaces);
        var taskScope = new ContextTaskScope(tenant, workspace, actorId, taskId, rootId, runId, "USER");

        context.query(actor, workspace, new ContextQuery("renewal", 1, 10), taskScope);

        verify(knowledge).search(eq(actor), eq(workspace), eq("renewal"), eq(1),
                eq(new KnowledgeSearchScope(tenant, workspace, actorId, taskId, runId,
                        "USER", "TASK", rootId, 0)), eq("VECTOR"));
    }

    @Test
    void mismatchedTaskActorCannotCauseEmbeddingSpend() {
        var tenant = UUID.randomUUID();
        var workspace = UUID.randomUUID();
        var actorId = UUID.randomUUID();
        var actor = new ActorContext(actorId, tenant, ActorType.HUMAN, Set.of());
        var knowledge = mock(KnowledgeService.class);
        var workspaces = mock(WorkspaceAuthorization.class);
        when(workspaces.isAuthorized(tenant, actorId, workspace, "knowledge:read")).thenReturn(true);
        var context = new EnterpriseContextService(knowledge, null, workspaces);
        var forgedActorScope = new ContextTaskScope(tenant, workspace, UUID.randomUUID(), UUID.randomUUID(), null,
                UUID.randomUUID(), "USER");

        assertThatThrownBy(() -> context.query(actor, workspace, new ContextQuery("renewal", 1, 10), forgedActorScope))
                .isInstanceOf(EafException.class);
        verify(knowledge, never()).search(eq(actor), eq(workspace), eq("renewal"), eq(1), any(KnowledgeSearchScope.class));
    }

    @Test
    void externalEmbeddingRequiresSeparateWorkspaceOutflowPermission() {
        var tenant = UUID.randomUUID();
        var workspace = UUID.randomUUID();
        var actorId = UUID.randomUUID();
        var actor = new ActorContext(actorId, tenant, ActorType.HUMAN, Set.of());
        var jdbc = mock(JdbcTemplate.class);
        var workspaces = mock(WorkspaceAuthorization.class);
        var embeddings = mock(EmbeddingGateway.class);
        when(workspaces.require(actor, workspace, "knowledge:read"))
                .thenReturn(new io.eaf.workspace.api.WorkspaceAccess(workspace, tenant, "authorized"));
        when(embeddings.profile()).thenReturn(new EmbeddingProfile("dashscope", "text-embedding-v3", "revision-1", 8, 8, 100, 512));
        doThrow(EafException.forbidden("外发权限缺失。"))
                .when(workspaces).require(actor, workspace, "knowledge:external-embedding");
        var knowledge = new JdbcKnowledgeService(jdbc, workspaces, new ObjectMapper(), Clock.systemUTC(), embeddings, mock(AuditPort.class));

        assertThatThrownBy(() -> knowledge.search(actor, workspace, "authorized query", 1))
                .isInstanceOf(EafException.class);
        verify(workspaces).require(actor, workspace, "knowledge:external-embedding");
        verify(embeddings, never()).embed(any());
    }

    @Test
    void rejectsDimensionOutsideDatabaseVectorRangeBeforeAnyPaidCall() {
        var tenant = UUID.randomUUID();
        var workspace = UUID.randomUUID();
        var actorId = UUID.randomUUID();
        var actor = new ActorContext(actorId, tenant, ActorType.HUMAN, Set.of());
        var jdbc = mock(JdbcTemplate.class);
        var workspaces = mock(WorkspaceAuthorization.class);
        var embeddings = mock(EmbeddingGateway.class);
        when(workspaces.require(actor, workspace, "knowledge:read"))
                .thenReturn(new io.eaf.workspace.api.WorkspaceAccess(workspace, tenant, "authorized"));
        when(embeddings.profile()).thenReturn(new EmbeddingProfile("fixture", "fixture-embedding", "revision-1", 16_001, 10, 8_192, 8_192));
        var knowledge = new JdbcKnowledgeService(jdbc, workspaces, new ObjectMapper(), Clock.systemUTC(), embeddings, mock(AuditPort.class));

        assertThatThrownBy(() -> knowledge.search(actor, workspace, "authorized query", 1))
                .isInstanceOf(EafException.class)
                .satisfies(error -> assertThat(((EafException) error).code()).isEqualTo("EMBEDDING_SCHEMA_PROFILE_UNSUPPORTED"));
        verify(embeddings, never()).embed(any());
        verify(workspaces, never()).require(actor, workspace, "knowledge:external-embedding");
    }
}
// 此用例通过根 Task ID 观察预算归属，避免 Context/RAG 查询形成未计费旁路。
