package io.eaf.agentprotocol;

import io.eaf.shared.EafException;
import io.eaf.workflow.api.CreateProjectBriefCommand;
import io.eaf.workflow.api.ProjectBriefArtifact;
import io.eaf.workflow.api.ProjectBriefSnapshot;
import io.eaf.workflow.api.ProjectBriefWorkItem;
import io.eaf.workflow.api.WorkflowService;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}")
public class ProjectBriefController {
    private final WorkflowService workflows;

    public ProjectBriefController(WorkflowService workflows) {
        this.workflows = workflows;
    }

    @PostMapping("/project-briefs")
    ResponseEntity<ProjectBriefSnapshot> create(@PathVariable UUID workspaceId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        var allowed = Set.of("title", "goal", "knowledgeRefs", "workItemRefs", "oaQueryRef",
                "reviewerId", "recipientId", "deadlineAt");
        if (body == null || !allowed.containsAll(body.keySet()) || !body.keySet().containsAll(Set.of("title", "goal", "knowledgeRefs")))
            throw EafException.invalid("项目简报请求字段缺失或包含未允许字段。");
        var knowledge = new ArrayList<CreateProjectBriefCommand.KnowledgeRef>();
        for (var entry : list(body.get("knowledgeRefs"), "knowledgeRefs")) {
            requireKeys(entry, Set.of("documentId", "documentVersion", "chunkId", "buildId", "contentHash"));
            knowledge.add(new CreateProjectBriefCommand.KnowledgeRef(uuid(entry.get("documentId"), "documentId"),
                    integer(entry.get("documentVersion"), "documentVersion"), uuid(entry.get("chunkId"), "chunkId"),
                    uuid(entry.get("buildId"), "buildId"), string(entry.get("contentHash"), "contentHash")));
        }
        var workItems = new ArrayList<CreateProjectBriefCommand.WorkItemRef>();
        for (var entry : list(body.getOrDefault("workItemRefs", java.util.List.of()), "workItemRefs")) {
            requireKeys(entry, Set.of("workItemId", "expectedRowVersion"));
            workItems.add(new CreateProjectBriefCommand.WorkItemRef(uuid(entry.get("workItemId"), "workItemId"),
                    longNumber(entry.get("expectedRowVersion"), "expectedRowVersion")));
        }
        CreateProjectBriefCommand.OaQueryRef oa = null;
        if (body.get("oaQueryRef") != null) {
            var entry = object(body.get("oaQueryRef"), "oaQueryRef");
            requireKeys(entry, Set.of("queryId", "expectedRowVersion"));
            oa = new CreateProjectBriefCommand.OaQueryRef(uuid(entry.get("queryId"), "queryId"),
                    longNumber(entry.get("expectedRowVersion"), "expectedRowVersion"));
        }
        var deadline = body.get("deadlineAt") == null ? null : instant(body.get("deadlineAt"), "deadlineAt");
        var result = workflows.createProjectBrief(new CreateProjectBriefCommand(ApiSupport.actor(authentication), workspaceId,
                string(body.get("title"), "title"), string(body.get("goal"), "goal"), knowledge, workItems, oa,
                optionalUuid(body.get("reviewerId"), "reviewerId"), optionalUuid(body.get("recipientId"), "recipientId"),
                deadline, idempotencyKey));
        return ResponseEntity.accepted().body(result);
    }

    @GetMapping("/project-briefs/{briefId}")
    ProjectBriefSnapshot get(@PathVariable UUID workspaceId, @PathVariable UUID briefId, Authentication authentication) {
        return workflows.getProjectBrief(ApiSupport.actor(authentication), workspaceId, briefId);
    }

    @PostMapping("/project-briefs/{briefId}/cancel")
    ProjectBriefSnapshot cancel(@PathVariable UUID workspaceId, @PathVariable UUID briefId,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        requireKeys(body, Set.of("expectedVersion"));
        return workflows.cancelProjectBrief(ApiSupport.actor(authentication), workspaceId, briefId,
                longNumber(body.get("expectedVersion"), "expectedVersion"));
    }

    @GetMapping("/project-brief-work-items/{workItemId}")
    ProjectBriefWorkItem workItem(@PathVariable UUID workspaceId, @PathVariable UUID workItemId,
            Authentication authentication) {
        return workflows.getProjectBriefWorkItem(ApiSupport.actor(authentication), workspaceId, workItemId);
    }

    @PatchMapping("/project-brief-work-items/{workItemId}/assignee")
    ProjectBriefWorkItem reassign(@PathVariable UUID workspaceId, @PathVariable UUID workItemId,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        requireKeys(body, Set.of("expectedVersion", "assigneeId"));
        return workflows.reassignProjectBriefWorkItem(ApiSupport.actor(authentication), workspaceId, workItemId,
                longNumber(body.get("expectedVersion"), "expectedVersion"), uuid(body.get("assigneeId"), "assigneeId"));
    }

    @PostMapping("/project-brief-work-items/{workItemId}/review")
    ProjectBriefWorkItem review(@PathVariable UUID workspaceId, @PathVariable UUID workItemId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        requireKeys(body, Set.of("expectedVersion", "decision", "notes", "nextAction"));
        return workflows.reviewProjectBriefWorkItem(ApiSupport.actor(authentication), workspaceId, workItemId,
                longNumber(body.get("expectedVersion"), "expectedVersion"), idempotencyKey,
                string(body.get("decision"), "decision"), string(body.get("notes"), "notes"),
                string(body.get("nextAction"), "nextAction"));
    }

    @PostMapping("/project-brief-work-items/{workItemId}/receive")
    ProjectBriefWorkItem receive(@PathVariable UUID workspaceId, @PathVariable UUID workItemId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        requireKeys(body, Set.of("expectedVersion", "disposition", "note"));
        return workflows.receiveProjectBriefWorkItem(ApiSupport.actor(authentication), workspaceId, workItemId,
                longNumber(body.get("expectedVersion"), "expectedVersion"), idempotencyKey,
                string(body.get("disposition"), "disposition"), string(body.get("note"), "note"));
    }

    @GetMapping("/project-briefs/{briefId}/artifacts/{version}")
    ProjectBriefArtifact artifact(@PathVariable UUID workspaceId, @PathVariable UUID briefId,
            @PathVariable int version, Authentication authentication) {
        return workflows.getProjectBriefArtifact(ApiSupport.actor(authentication), workspaceId, briefId, version);
    }

    @GetMapping("/project-briefs/{briefId}/artifacts/{version}.md")
    ResponseEntity<String> markdown(@PathVariable UUID workspaceId, @PathVariable UUID briefId,
            @PathVariable int version, Authentication authentication) {
        var artifact = workflows.getProjectBriefArtifact(ApiSupport.actor(authentication), workspaceId, briefId, version);
        if (artifact.blocked() || artifact.markdown() == null)
            throw EafException.conflict("BRIEF_SOURCE_UNAVAILABLE", "简报来源当前不可用，不能导出旧正文。");
        return ResponseEntity.ok().contentType(MediaType.parseMediaType("text/markdown;charset=UTF-8"))
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=\"project-brief-%s-v%d.md\"".formatted(briefId, version))
                .body(artifact.markdown());
    }

    private static java.util.List<Map<String, Object>> list(Object value, String field) {
        if (!(value instanceof java.util.List<?> values)) throw EafException.invalid(field + " 必须是数组。");
        var result = new ArrayList<Map<String, Object>>();
        for (var item : values) result.add(object(item, field + " item"));
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value, String field) {
        if (!(value instanceof Map<?, ?> map) || map.keySet().stream().anyMatch(key -> !(key instanceof String)))
            throw EafException.invalid(field + " 必须是对象。");
        return (Map<String, Object>) map;
    }

    private static void requireKeys(Map<String, Object> body, Set<String> keys) {
        if (body == null || !body.keySet().equals(keys)) throw EafException.invalid("请求字段缺失或包含未允许字段。");
    }

    private static String string(Object value, String field) {
        if (!(value instanceof String text)) throw EafException.invalid(field + " 必须是字符串。");
        return text;
    }

    private static UUID uuid(Object value, String field) {
        try {
            var parsed = UUID.fromString(string(value, field));
            if (!parsed.toString().equals(value)) throw new IllegalArgumentException();
            return parsed;
        }
        catch (IllegalArgumentException invalid) { throw EafException.invalid(field + " 必须是 UUID。"); }
    }

    private static UUID optionalUuid(Object value, String field) { return value == null ? null : uuid(value, field); }

    private static int integer(Object value, String field) {
        if (!(value instanceof Number number) || number.doubleValue() != number.intValue())
            throw EafException.invalid(field + " 必须是整数。");
        return number.intValue();
    }

    private static long longNumber(Object value, String field) {
        if (!(value instanceof Number number) || number.doubleValue() != number.longValue())
            throw EafException.invalid(field + " 必须是整数。");
        return number.longValue();
    }

    private static Instant instant(Object value, String field) {
        try { return Instant.parse(string(value, field)); }
        catch (RuntimeException invalid) { throw EafException.invalid(field + " 必须是 ISO-8601 时间。"); }
    }
}
