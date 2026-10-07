package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.audit.api.AuditQuery;
import io.eaf.learning.api.FeedbackService;
import io.eaf.learning.api.SubmitFeedbackCommand;
import io.eaf.shared.EafException;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskPageCursor;
import io.eaf.task.api.TaskStatus;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.time.Instant;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/tasks")
public class TaskController {
    private final TaskService tasks;
    private final RuntimeQuery runtime;
    private final AuditQuery audit;
    private final FeedbackService feedbacks;
    private final TaskApplicationService taskApplication;
    private final WorkspaceAuthorization workspaces;
    private final ObjectMapper json;

    public TaskController(TaskService tasks, RuntimeQuery runtime, AuditQuery audit, FeedbackService feedbacks,
                          TaskApplicationService taskApplication, WorkspaceAuthorization workspaces,
                          ObjectMapper json) {
        this.tasks = tasks; this.runtime = runtime; this.audit = audit; this.feedbacks = feedbacks;
        this.taskApplication = taskApplication; this.workspaces = workspaces;
        this.json = json;
    }

    @PostMapping
    ResponseEntity<TaskResponse> create(@PathVariable UUID workspaceId, @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                         @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
                                         @RequestBody Map<String, Object> raw, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var body = convert(raw, TaskApplicationService.CreateRequest.class,
                Set.of("agentId", "agentVersion", "capabilityId", "capabilityVersion", "input", "businessEntity"));
        if (raw.get("businessEntity") instanceof Map<?, ?> entity
                && entity.keySet().stream().anyMatch(key -> !Set.of("type", "id").contains(key)))
            throw EafException.invalid("businessEntity 包含未允许字段。");
        // REST 入口类型与 Task source 均由服务端固定，Task DTO 不能声明或覆盖。
        var response = taskApplication.create(actor, workspaceId, body, idempotencyKey, traceId, "REST");
        return ResponseEntity.accepted().header("Location", "/api/v1/workspaces/%s/tasks/%s".formatted(workspaceId, response.id())).body(response);
    }

    @PostMapping("/{taskId}/experience-drafts")
    ResponseEntity<TaskResponse> createExperienceDraft(@PathVariable UUID workspaceId, @PathVariable UUID taskId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, TaskApplicationService.ExperienceDraftRequest.class, Set.of("feedbackId", "draftText"));
        var response = taskApplication.createExperienceDraft(ApiSupport.actor(authentication), workspaceId, taskId,
                body, idempotencyKey, traceId);
        return ResponseEntity.accepted().header("Location", "/api/v1/workspaces/%s/tasks/%s".formatted(workspaceId, response.id())).body(response);
    }

    @GetMapping("/{taskId}")
    TaskResponse get(@PathVariable UUID workspaceId, @PathVariable UUID taskId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        return taskApplication.get(actor, workspaceId, taskId);
    }

    @GetMapping
    TaskApplicationService.TaskPageResponse list(@PathVariable UUID workspaceId,
            @RequestParam(required = false) UUID rootTaskId,
            @RequestParam(required = false) Set<TaskStatus> status,
            @RequestParam(required = false) Instant statusUpdatedAfter,
            @RequestParam(required = false) Instant cursorUpdatedAt,
            @RequestParam(required = false) UUID cursorTaskId,
            @RequestParam(defaultValue = "20") int limit, Authentication authentication) {
        if (limit < 1 || limit > 50 || (cursorUpdatedAt == null) != (cursorTaskId == null))
            throw EafException.invalid("Task 列表 limit 或游标无效。");
        var cursor = cursorUpdatedAt == null ? null : new TaskPageCursor(cursorUpdatedAt, cursorTaskId);
        return taskApplication.list(ApiSupport.actor(authentication), workspaceId, rootTaskId, status,
                statusUpdatedAfter, cursor, limit);
    }

    @GetMapping("/{taskId}/sources")
    io.eaf.agentruntime.api.TaskSources sources(@PathVariable UUID workspaceId, @PathVariable UUID taskId,
                                                Authentication authentication) {
        return taskApplication.sources(ApiSupport.actor(authentication), workspaceId, taskId);
    }

    @GetMapping("/{taskId}/usage")
    TaskApplicationService.TaskUsageResponse usage(@PathVariable UUID workspaceId, @PathVariable UUID taskId,
                                                    Authentication authentication) {
        return taskApplication.usage(ApiSupport.actor(authentication), workspaceId, taskId);
    }

    @PostMapping("/{taskId}/followups")
    ResponseEntity<TaskApplicationService.FollowupResponse> confirmFollowup(@PathVariable UUID workspaceId,
            @PathVariable UUID taskId, @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, FollowupBody.class, Set.of("expectedVersion", "summary"));
        var response = taskApplication.confirmFollowup(ApiSupport.actor(authentication), workspaceId, taskId,
                body.expectedVersion(), body.summary(), idempotencyKey);
        return ResponseEntity.accepted().body(response);
    }

    @GetMapping("/{taskId}/followups")
    TaskApplicationService.FollowupPageResponse followups(@PathVariable UUID workspaceId,
            @PathVariable UUID taskId, @RequestParam(required = false) Instant cursorCreatedAt,
            @RequestParam(required = false) UUID cursorId, @RequestParam(defaultValue = "20") int limit,
            Authentication authentication) {
        return taskApplication.followups(ApiSupport.actor(authentication), workspaceId, taskId,
                cursorCreatedAt, cursorId, limit);
    }

    @PostMapping("/{taskId}/service-requests")
    ResponseEntity<TaskApplicationService.ServiceRequestResponse> confirmServiceRequest(
            @PathVariable UUID workspaceId, @PathVariable UUID taskId,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, VersionBody.class, Set.of("expectedVersion"));
        var response = taskApplication.confirmServiceRequest(ApiSupport.actor(authentication), workspaceId,
                taskId, body.expectedVersion(), idempotencyKey);
        return ResponseEntity.accepted().body(response);
    }

    @GetMapping("/{taskId}/service-requests")
    TaskApplicationService.ServiceRequestResponse serviceRequest(@PathVariable UUID workspaceId,
            @PathVariable UUID taskId, Authentication authentication) {
        return taskApplication.serviceRequest(ApiSupport.actor(authentication), workspaceId, taskId);
    }

    @PostMapping("/{taskId}/cancel")
    TaskResponse cancel(@PathVariable UUID workspaceId, @PathVariable UUID taskId, @RequestBody Map<String, Object> raw, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var body = convert(raw, VersionBody.class, Set.of("expectedVersion"));
        return taskApplication.cancel(actor, workspaceId, taskId, body.expectedVersion());
    }

    @PostMapping("/{taskId}/retry")
    TaskResponse retry(@PathVariable UUID workspaceId, @PathVariable UUID taskId, @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                       @RequestBody Map<String, Object> raw, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var body = convert(raw, VersionBody.class, Set.of("expectedVersion"));
        return taskApplication.retry(actor, workspaceId, taskId, body.expectedVersion(), idempotencyKey);
    }

    @PostMapping("/{taskId}/resume")
    TaskResponse resume(@PathVariable UUID workspaceId, @PathVariable UUID taskId,
                        @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                        @RequestBody Map<String, Object> raw, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var body = convert(raw, VersionBody.class, Set.of("expectedVersion"));
        return taskApplication.resume(actor, workspaceId, taskId, body.expectedVersion(), idempotencyKey);
    }

    @GetMapping("/{taskId}/audit-events")
    AuditResponse audit(@PathVariable UUID workspaceId, @PathVariable UUID taskId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var task = tasks.get(actor, workspaceId, taskId);
        workspaces.require(actor, workspaceId, "audit:read");
        var items = audit.byTask(actor.tenantId(), task.id()).stream().map(e -> new AuditResponse.Item(e.id(), e.action(), e.result(), e.payloadJson(), e.traceId(), e.occurredAt())).toList();
        return new AuditResponse(items, null);
    }

    @GetMapping("/{taskId}/replay")
    ReplayResponse replay(@PathVariable UUID workspaceId, @PathVariable UUID taskId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        tasks.get(actor, workspaceId, taskId);
        var result = runtime.replay(actor, workspaceId, taskId);
        return new ReplayResponse(result.taskId(), result.originalStatus().name(), result.replayStatus().name(),
                result.modelCalled(), result.matched(), result.resultJson(), result.errorCode());
    }

    @PostMapping("/{taskId}/feedback")
    ResponseEntity<io.eaf.learning.api.Feedback> feedback(@PathVariable UUID workspaceId, @PathVariable UUID taskId,
                                                           @RequestHeader("Idempotency-Key") String idempotencyKey,
                                                           @RequestBody Map<String, Object> raw, Authentication authentication) {
        // 来源字段不接受客户端输入；Learning 根据认证身份与原 Task 快照重新派生。
        var body = convert(raw, FeedbackBody.class, Set.of("correction", "evidence", "executionId"));
        var result = feedbacks.submit(new SubmitFeedbackCommand(ApiSupport.actor(authentication), workspaceId, taskId,
                body.correction(), body.evidence(), body.executionId(), idempotencyKey));
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.feedback());
    }

    @GetMapping("/{taskId}/feedback")
    List<io.eaf.learning.api.Feedback> feedback(@PathVariable UUID workspaceId, @PathVariable UUID taskId,
                                                Authentication authentication) {
        return feedbacks.list(ApiSupport.actor(authentication), workspaceId, taskId);
    }

    private <T> T convert(Map<String, Object> raw, Class<T> type, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("Task 请求包含未允许字段。");
        // Jackson 默认忽略额外字段；协议入口先按业务白名单拒绝身份和控制字段。
        try { return json.convertValue(raw, type); }
        catch (IllegalArgumentException e) { throw EafException.invalid("Task 请求字段类型无效。"); }
    }

    record VersionBody(long expectedVersion) { }
    record FollowupBody(long expectedVersion, String summary) { }
    record FeedbackBody(String correction, String evidence, UUID executionId) { }
    record AuditResponse(List<Item> items, String nextCursor) { record Item(UUID id, String action, String result, String payload, String traceId, java.time.Instant occurredAt) { } }
    record ReplayResponse(UUID taskId, String originalStatus, String replayStatus, boolean modelCalled,
                          boolean matched, String resultJson, String errorCode) { }
}
// 本文件负责实现 EAF 的 TaskController.java 相关代码。
