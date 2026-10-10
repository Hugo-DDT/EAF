package io.eaf.evaluation.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.time.Instant;
import java.util.Comparator;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** 只在专用 Workspace 的正式合成资料上执行，并冻结可检索版本与 READY 索引摘要。 */
@Component
final class ScenarioContextManifest {
    private final KnowledgeService knowledge;
    private final WorkspaceAuthorization workspaces;
    private final ObjectMapper json;

    ScenarioContextManifest(KnowledgeService knowledge, WorkspaceAuthorization workspaces, ObjectMapper json) {
        this.knowledge = knowledge;
        this.workspaces = workspaces;
        this.json = json;
    }

    ArrayNode capture(ActorContext actor, UUID workspaceId) {
        workspaces.require(actor, workspaceId, "knowledge:read");
        var items = new java.util.ArrayList<com.fasterxml.jackson.databind.node.ObjectNode>();
        Instant cursorTime = null;
        UUID cursorId = null;
        // Knowledge Owner 单页上限为 50；40 页维持原来至多 2000 篇的快照边界。
        for (int page = 0; page < 40; page++) {
            var result = knowledge.listDocuments(actor, workspaceId, cursorTime, cursorId, 50);
            for (var item : result.items()) {
                if ("REVOKED".equals(item.status()) && item.version() == null) continue;
                var document = knowledge.get(actor, workspaceId, item.id());
                if (!"PUBLISHED".equals(document.status())
                        || !"true".equalsIgnoreCase(document.metadata().get("synthetic")))
                    throw EafException.forbidden("Workspace 只能包含已发布并标记 synthetic=true 的正式知识文档。");
                var publication = knowledge.getCurrentPublication(actor, workspaceId, document.id());
                if (publication == null || !"PUBLISHED".equals(publication.documentStatus())
                        || publication.assetVersion() != document.version()
                        || !publication.contentHash().equals(document.contentHash()))
                    throw EafException.conflict("SCENARIO_KNOWLEDGE_CHANGED", "Knowledge 发布版本已变化。");
                var build = knowledge.getIndexBuild(actor, workspaceId, document.id(), publication.buildId());
                if (build == null || !"READY".equals(build.status()))
                    throw EafException.conflict("SCENARIO_INDEX_NOT_READY", "知识索引尚未 READY。");
                var manifest = json.createObjectNode();
                manifest.put("documentId", document.id().toString());
                manifest.put("version", document.version());
                manifest.put("contentHash", document.contentHash());
                manifest.put("buildId", build.id().toString());
                manifest.put("indexSignature", build.configurationSignature());
                manifest.put("embeddingProvider", build.provider());
                manifest.put("embeddingModel", build.model());
                manifest.put("embeddingRevision", build.modelRevision());
                items.add(manifest);
            }
            if (result.nextCreatedAt() == null || result.nextId() == null) break;
            if (page == 39) throw EafException.conflict("SCENARIO_KNOWLEDGE_TOO_LARGE", "合成 Workspace 知识文档数量超过 2000。"
            );
            cursorTime = result.nextCreatedAt();
            cursorId = result.nextId();
        }
        if (items.isEmpty()) throw EafException.conflict("SCENARIO_KNOWLEDGE_EMPTY", "需要至少一个已发布的合成知识文档及 READY 索引。");
        items.sort(Comparator.comparing(item -> item.path("documentId").asText()));
        var manifest = json.createArrayNode();
        items.forEach(manifest::add);
        return manifest;
    }
}
