package io.eaf.evaluation.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.knowledge.api.KnowledgeDocument;
import io.eaf.knowledge.api.KnowledgeDocumentPage;
import io.eaf.knowledge.api.KnowledgeIndexBuild;
import io.eaf.knowledge.api.KnowledgePublication;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceAccess;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ScenarioContextManifestTest {
    private static final UUID TENANT = UUID.randomUUID();
    private static final UUID WORKSPACE = UUID.randomUUID();
    private static final ActorContext ACTOR = new ActorContext(UUID.randomUUID(), TENANT, ActorType.HUMAN, Set.of());

    private final KnowledgeService knowledge = org.mockito.Mockito.mock(KnowledgeService.class);
    private final WorkspaceAuthorization workspaces = org.mockito.Mockito.mock(WorkspaceAuthorization.class);
    private final ScenarioContextManifest manifest = new ScenarioContextManifest(knowledge, workspaces, new ObjectMapper());

    @Test
    void skipsOnlyRevokedHistoryWithoutVersion() {
        var revokedId = UUID.randomUUID();
        var validId = UUID.randomUUID();
        when(workspaces.require(ACTOR, WORKSPACE, "knowledge:read"))
                .thenReturn(new WorkspaceAccess(WORKSPACE, TENANT, "scenario"));
        when(knowledge.listDocuments(ACTOR, WORKSPACE, null, null, 50)).thenReturn(new KnowledgeDocumentPage(
                java.util.List.of(new KnowledgeDocumentPage.Item(revokedId, "revoked", "REVOKED", null, Instant.now()),
                        new KnowledgeDocumentPage.Item(validId, "valid", "PUBLISHED", 1, Instant.now())),
                2, null, null));
        var document = new KnowledgeDocument(validId, TENANT, WORKSPACE, ACTOR.actorId(), "valid", "test",
                Map.of("synthetic", "true"), 1, "body", "a".repeat(64), "PUBLISHED", Instant.now(), 1);
        var buildId = UUID.randomUUID();
        when(knowledge.get(ACTOR, WORKSPACE, validId)).thenReturn(document);
        when(knowledge.getCurrentPublication(ACTOR, WORKSPACE, validId)).thenReturn(new KnowledgePublication(
                UUID.randomUUID(), TENANT, WORKSPACE, validId, 1, buildId, "PUBLISH", "PUBLISHED", 1,
                Instant.now(), null, null, null, null, document.contentHash()));
        when(knowledge.getIndexBuild(ACTOR, WORKSPACE, validId, buildId)).thenReturn(new KnowledgeIndexBuild(
                buildId, TENANT, WORKSPACE, validId, 1, "v1", "synthetic", "synthetic", "1", 1,
                "COSINE", "signature", "READY", 1, 1, 1, 1, 1, null, Instant.now(), Instant.now(),
                Instant.now(), Instant.now()));

        var captured = manifest.capture(ACTOR, WORKSPACE);

        assertThat(captured).hasSize(1);
        assertThat(captured.get(0).path("documentId").asText()).isEqualTo(validId.toString());
        verify(knowledge, never()).get(ACTOR, WORKSPACE, revokedId);
    }

    @Test
    void returnsEmptyWhenOnlyRevokedHistoryRemains() {
        var revokedId = UUID.randomUUID();
        when(workspaces.require(ACTOR, WORKSPACE, "knowledge:read"))
                .thenReturn(new WorkspaceAccess(WORKSPACE, TENANT, "scenario"));
        when(knowledge.listDocuments(ACTOR, WORKSPACE, null, null, 50)).thenReturn(new KnowledgeDocumentPage(
                java.util.List.of(new KnowledgeDocumentPage.Item(revokedId, "revoked", "REVOKED", null, Instant.now())),
                1, null, null));

        assertThatThrownBy(() -> manifest.capture(ACTOR, WORKSPACE))
                .isInstanceOf(EafException.class)
                .extracting(error -> ((EafException) error).code()).isEqualTo("SCENARIO_KNOWLEDGE_EMPTY");
        verify(knowledge, never()).get(ACTOR, WORKSPACE, revokedId);
    }
}
