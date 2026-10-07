package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.task.api.TaskService;
import io.eaf.workflow.api.CreateServiceRequestBatchCommand;
import io.eaf.workflow.api.ServiceRequestBatchSnapshot;
import io.eaf.workflow.api.TeamExperienceRef;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 只投影批次 Owner 的状态；分支正文在读时重新核对 Task 与 Context 授权。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/service-request-batches")
public class ServiceRequestBatchController {
    private final WorkflowService workflows;
    private final TaskService tasks;
    private final RuntimeQuery runtime;
    private final WorkspaceAuthorization workspaces;
    private final ObjectMapper json;

    public ServiceRequestBatchController(WorkflowService workflows, TaskService tasks, RuntimeQuery runtime,
            WorkspaceAuthorization workspaces, ObjectMapper json) {
        this.workflows = workflows;
        this.tasks = tasks;
        this.runtime = runtime;
        this.workspaces = workspaces;
        this.json = json;
    }

    @PostMapping
    ResponseEntity<BatchResponse> create(@PathVariable UUID workspaceId,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        requireKeys(raw, Set.of("maxActiveItems", "deadlineAt", "items"));
        validateItemKeys(raw.get("items"));
        var body = json.convertValue(raw, CreateBody.class);
        if (body.items() == null) throw EafException.invalid("items 必须是数组。");
        var command = new CreateServiceRequestBatchCommand(ApiSupport.actor(authentication), workspaceId,
                idempotencyKey, body.maxActiveItems(), body.deadlineAt(), body.items().stream()
                .map(item -> new CreateServiceRequestBatchCommand.Item(item.itemKey(), item.requestText(),
                        item.scenarioKey(), item.experienceRefs())).toList());
        var created = workflows.createServiceRequestBatch(command);
        return ResponseEntity.status(created.replayed() ? HttpStatus.OK : HttpStatus.CREATED)
                .body(project(command.actor(), workspaceId, created.batch()));
    }

    @GetMapping("/{batchId}")
    BatchResponse get(@PathVariable UUID workspaceId, @PathVariable UUID batchId, Authentication authentication) {
        return project(ApiSupport.actor(authentication), workspaceId,
                workflows.getServiceRequestBatch(ApiSupport.actor(authentication), workspaceId, batchId));
    }

    @GetMapping("/{batchId}/items/{itemKey}")
    ItemResponse getItem(@PathVariable UUID workspaceId, @PathVariable UUID batchId, @PathVariable String itemKey,
            Authentication authentication) {
        return projectItem(ApiSupport.actor(authentication), workspaceId,
                workflows.getServiceRequestBatchItem(ApiSupport.actor(authentication), workspaceId, batchId, itemKey));
    }

    @PostMapping("/{batchId}/cancel")
    BatchResponse cancel(@PathVariable UUID workspaceId, @PathVariable UUID batchId,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        requireKeys(raw, Set.of("expectedVersion"));
        var version = json.convertValue(raw.get("expectedVersion"), Long.class);
        var actor = ApiSupport.actor(authentication);
        return project(actor, workspaceId, workflows.cancelServiceRequestBatch(actor, workspaceId, batchId, version));
    }

    @PostMapping("/{batchId}/items/{itemKey}/cancel")
    ItemResponse cancelItem(@PathVariable UUID workspaceId, @PathVariable UUID batchId, @PathVariable String itemKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        requireKeys(raw, Set.of("expectedVersion"));
        var version = json.convertValue(raw.get("expectedVersion"), Long.class);
        var actor = ApiSupport.actor(authentication);
        workflows.cancelServiceRequestBatchItem(actor, workspaceId, batchId, itemKey, version);
        return projectItem(actor, workspaceId, workflows.getServiceRequestBatchItem(actor, workspaceId, batchId, itemKey));
    }

    private BatchResponse project(ActorContext actor, UUID workspaceId, ServiceRequestBatchSnapshot batch) {
        workspaces.require(actor, workspaceId, "task:read");
        var items = new ArrayList<ItemResponse>();
        var categories = new TreeMap<String, Integer>();
        var outcomes = new TreeMap<String, Integer>();
        var visible = 0;
        var unavailable = 0;
        for (var item : batch.items()) {
            var detail = workflows.getServiceRequestBatchItem(actor, workspaceId, batch.id(), item.itemKey());
            var projected = projectItem(actor, workspaceId, detail);
            items.add(projected);
            if (projected.resultUnavailable()) unavailable++;
            if (projected.result() != null) {
                visible++;
                count(categories, projected.result().path("knowledge").path("category").asText(null));
                count(outcomes, projected.result().path("knowledge").path("outcome").asText(null));
                count(outcomes, projected.result().path("experience").path("outcome").asText(null));
            }
        }
        return new BatchResponse(batch.id(), batch.status(), batch.version(), batch.createdAt(), batch.deadlineAt(),
                batch.maxActiveItems(), batch.itemCount(), batch.counts(), visible, unavailable, categories, outcomes, items);
    }

    private ItemResponse projectItem(ActorContext actor, UUID workspaceId, ServiceRequestBatchSnapshot.Detail detail) {
        workspaces.require(actor, workspaceId, "task:read");
        var item = detail.item();
        var branches = new ArrayList<BranchView>();
        if (item.knowledgeTaskId() != null) branches.add(branch(actor, workspaceId, "knowledge", item.knowledgeTaskId()));
        if (item.experienceTaskId() != null) branches.add(branch(actor, workspaceId, "experience", item.experienceTaskId()));
        if (!"SUCCEEDED".equals(item.status()))
            return new ItemResponse(item.itemKey(), item.ordinal(), item.status(), item.version(), item.errorCode(),
                    detail.requestText(), detail.scenarioKey(), branches, false, null, List.of());
        if (item.knowledgeTaskId() == null || item.experienceTaskId() == null
                || !runtime.canExposeResult(actor, workspaceId, item.knowledgeTaskId())
                || !runtime.canExposeResult(actor, workspaceId, item.experienceTaskId()))
            return new ItemResponse(item.itemKey(), item.ordinal(), item.status(), item.version(), item.errorCode(),
                    detail.requestText(), detail.scenarioKey(), branches, true, null, List.of());
        var knowledge = tasks.get(actor, workspaceId, item.knowledgeTaskId());
        var experience = tasks.get(actor, workspaceId, item.experienceTaskId());
        if (knowledge.resultJson() == null || experience.resultJson() == null)
            return new ItemResponse(item.itemKey(), item.ordinal(), item.status(), item.version(), item.errorCode(),
                    detail.requestText(), detail.scenarioKey(), branches, true, null, List.of());
        var sources = new ArrayList<SourceView>();
        addSources(actor, workspaceId, "knowledge", item.knowledgeTaskId(), sources);
        addSources(actor, workspaceId, "experience", item.experienceTaskId(), sources);
        try {
            var result = json.createObjectNode();
            result.set("knowledge", json.readTree(knowledge.resultJson()));
            result.set("experience", json.readTree(experience.resultJson()));
            return new ItemResponse(item.itemKey(), item.ordinal(), item.status(), item.version(), item.errorCode(),
                    detail.requestText(), detail.scenarioKey(), branches, false, result, sources);
        } catch (Exception invalid) {
            return new ItemResponse(item.itemKey(), item.ordinal(), item.status(), item.version(), "RESULT_UNAVAILABLE",
                    detail.requestText(), detail.scenarioKey(), branches, true, null, List.of());
        }
    }

    private BranchView branch(ActorContext actor, UUID workspaceId, String role, UUID taskId) {
        var task = tasks.get(actor, workspaceId, taskId);
        return new BranchView(role, taskId, task.status().name(), task.errorCode());
    }

    private void addSources(ActorContext actor, UUID workspaceId, String role, UUID taskId, List<SourceView> target) {
        for (var source : runtime.contextSources(actor, workspaceId, taskId)) target.add(new SourceView(role, taskId,
                source.citationId(), source.sourceType(), source.documentId(), source.documentVersion(), source.chunkId(),
                source.memoryId(), source.memoryVersion(), source.contentHash()));
    }

    private void count(Map<String, Integer> counts, String value) {
        if (value != null && !value.isBlank()) counts.merge(value, 1, Integer::sum);
    }

    private void requireKeys(Map<String, Object> raw, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("批次请求包含无效或未允许字段。");
    }

    private void validateItemKeys(Object value) {
        if (!(value instanceof List<?> items)) throw EafException.invalid("items 必须是数组。");
        for (var item : items) {
            if (!(item instanceof Map<?, ?> map)) throw EafException.invalid("批次项目必须是 object。");
            if (map.keySet().stream().anyMatch(key -> !Set.of("itemKey", "requestText", "scenarioKey", "experienceRefs").contains(key)))
                throw EafException.invalid("批次项目包含未允许字段。");
            var refs = map.get("experienceRefs");
            if (refs instanceof List<?> list) for (var ref : list) {
                if (!(ref instanceof Map<?, ?> reference)
                        || reference.keySet().stream().anyMatch(key -> !Set.of("cardId", "revision").contains(key)))
                    throw EafException.invalid("经验引用只允许 cardId 与 revision。");
            }
        }
    }

    record CreateBody(Integer maxActiveItems, java.time.Instant deadlineAt, List<ItemBody> items) { }
    record ItemBody(String itemKey, String requestText, String scenarioKey, List<TeamExperienceRef> experienceRefs) { }
    record BatchResponse(UUID id, String status, long version, java.time.Instant createdAt, java.time.Instant deadlineAt,
            int maxActiveItems, int itemCount, Map<String, Integer> counts, int visibleResultCount,
            int unavailableResultCount, Map<String, Integer> categoryCounts, Map<String, Integer> outcomeCounts,
            List<ItemResponse> items) { }
    record ItemResponse(String itemKey, int ordinal, String status, long version, String errorCode, String requestText,
            String scenarioKey, List<BranchView> branches, boolean resultUnavailable, JsonNode result,
            List<SourceView> sources) { }
    record BranchView(String role, UUID taskId, String status, String errorCode) { }
    record SourceView(String role, UUID taskId, String citationId, String sourceType, UUID documentId,
            Integer documentVersion, UUID chunkId, UUID memoryId, String memoryVersion, String contentHash) { }
}
