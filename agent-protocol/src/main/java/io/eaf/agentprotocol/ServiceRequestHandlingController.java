package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.EafException;
import io.eaf.workflow.api.HumanWorkItem;
import io.eaf.workflow.api.HumanWorkItemPage;
import io.eaf.workflow.api.ServiceRequestHandlingSnapshot;
import io.eaf.workflow.api.TeamExperienceRef;
import io.eaf.workflow.api.WorkflowService;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** REST 只接受显式交接和人工结果； 来源、权限、冻结数据由 Workflow Owner 校验。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}")
public class ServiceRequestHandlingController {
    private final WorkflowService workflows;
    private final ObjectMapper json;

    public ServiceRequestHandlingController(WorkflowService workflows, ObjectMapper json) {
        this.workflows = workflows;
        this.json = json;
    }

    @PostMapping("/service-request-handlings")
    ResponseEntity<ServiceRequestHandlingSnapshot> create(@PathVariable UUID workspaceId,
            @RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        var body = convert(raw, CreateBody.class, Set.of("submissionId", "assigneeId", "sharedBrief", "deadlineAt",
                "scenarioKey", "teamExperienceRefs"));
        var snapshot = workflows.createServiceRequestHandlingWorkflow(ApiSupport.actor(authentication), workspaceId,
                body.submissionId(), body.assigneeId(), body.sharedBrief(), idempotencyKey, body.deadlineAt(),
                body.scenarioKey(), body.teamExperienceRefs());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(snapshot);
    }

    @GetMapping("/service-request-handlings/{instanceId}")
    ServiceRequestHandlingSnapshot get(@PathVariable UUID workspaceId, @PathVariable UUID instanceId,
                                       Authentication authentication) {
        return workflows.getServiceRequestHandling(ApiSupport.actor(authentication), workspaceId, instanceId);
    }

    @PostMapping("/service-request-handlings/{instanceId}/cancel")
    ServiceRequestHandlingSnapshot cancel(@PathVariable UUID workspaceId, @PathVariable UUID instanceId,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, CancelBody.class, Set.of("expectedVersion"));
        var actor = ApiSupport.actor(authentication);
        workflows.cancelInstance(actor, workspaceId, instanceId, body.expectedVersion());
        return workflows.getServiceRequestHandling(actor, workspaceId, instanceId);
    }

    @GetMapping("/work-items")
    WorkItemList list(@PathVariable UUID workspaceId, @RequestParam(required = false) String relation,
            @RequestParam(required = false) String status, @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "20") int limit, Authentication authentication) {
        var decoded = decode(cursor);
        var page = workflows.listHumanWorkItems(ApiSupport.actor(authentication), workspaceId, relation, status,
                decoded == null ? null : decoded.createdAt(), decoded == null ? null : decoded.id(), limit);
        return new WorkItemList(page.items(), encode(page));
    }

    @GetMapping("/work-items/{workItemId}")
    HumanWorkItem getWorkItem(@PathVariable UUID workspaceId, @PathVariable UUID workItemId,
                              Authentication authentication) {
        return workflows.getHumanWorkItem(ApiSupport.actor(authentication), workspaceId, workItemId);
    }

    @PatchMapping("/work-items/{workItemId}/assignee")
    HumanWorkItem reassign(@PathVariable UUID workspaceId, @PathVariable UUID workItemId,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, ReassignBody.class, Set.of("expectedVersion", "assigneeId"));
        return workflows.reassignHumanWorkItem(ApiSupport.actor(authentication), workspaceId, workItemId,
                body.expectedVersion(), body.assigneeId());
    }

    @PostMapping("/work-items/{workItemId}/complete")
    HumanWorkItem complete(@PathVariable UUID workspaceId, @PathVariable UUID workItemId,
            @RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        var body = convert(raw, CompleteBody.class, Set.of("expectedVersion", "outcome", "summary", "nextAction"));
        return workflows.completeHumanWorkItem(ApiSupport.actor(authentication), workspaceId, workItemId,
                body.expectedVersion(), idempotencyKey, body.outcome(), body.summary(), body.nextAction());
    }

    private <T> T convert(Map<String, Object> raw, Class<T> type, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("请求包含无效或未允许字段。");
        try { return json.convertValue(raw, type); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("请求字段类型无效。"); }
    }

    private Cursor decode(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            var fields = new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8).split("\n", -1);
            if (fields.length != 2) throw new IllegalArgumentException();
            return new Cursor(Instant.parse(fields[0]), UUID.fromString(fields[1]));
        } catch (RuntimeException invalid) { throw EafException.invalid("列表 cursor 无效。"); }
    }

    private String encode(HumanWorkItemPage page) {
        if (page.nextCreatedAt() == null || page.nextId() == null) return null;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
                (page.nextCreatedAt() + "\n" + page.nextId()).getBytes(StandardCharsets.UTF_8));
    }

    private record Cursor(Instant createdAt, UUID id) { }
    private record CreateBody(UUID submissionId, UUID assigneeId, String sharedBrief, Instant deadlineAt,
                              String scenarioKey, List<TeamExperienceRef> teamExperienceRefs) { }
    private record CancelBody(long expectedVersion) { }
    private record ReassignBody(long expectedVersion, UUID assigneeId) { }
    private record CompleteBody(long expectedVersion, String outcome, String summary, String nextAction) { }
    private record WorkItemList(List<HumanWorkItem> items, String nextCursor) { }
}
