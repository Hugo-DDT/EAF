package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.EafException;
import io.eaf.workflow.api.P27BusinessWorkflowService;
import io.eaf.workflow.api.WorkflowInstance;
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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** P27 只暴露固定 OA 查询、外部状态查询和显式服务台结果同步。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}")
public class P27BusinessConnectionController {
    private final P27BusinessWorkflowService workflows;
    private final ObjectMapper json;

    public P27BusinessConnectionController(P27BusinessWorkflowService workflows, ObjectMapper json) {
        this.workflows = workflows;
        this.json = json;
    }

    @PostMapping("/oa/todo-queries")
    ResponseEntity<QueryCreated> createTodoList(@PathVariable UUID workspaceId,
            @RequestHeader("Idempotency-Key") String key, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        requireKeys(raw, Set.of("status", "cursor", "limit"));
        var body = convert(raw, OaListBody.class, Set.of("status", "cursor", "limit"));
        return ResponseEntity.accepted().body(queryCreated(workflows.createOaTodoListQuery(
                ApiSupport.actor(authentication), workspaceId, body.status(), body.cursor(), body.limit(), key)));
    }

    @PostMapping("/oa/todo-queries/item")
    ResponseEntity<QueryCreated> createTodoItem(@PathVariable UUID workspaceId,
            @RequestHeader("Idempotency-Key") String key, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        requireKeys(raw, Set.of("todoId"));
        var body = convert(raw, OaItemBody.class, Set.of("todoId"));
        return ResponseEntity.accepted().body(queryCreated(workflows.createOaTodoItemQuery(
                ApiSupport.actor(authentication), workspaceId, body.todoId(), key)));
    }

    @GetMapping("/oa/todo-queries/{queryId}")
    P27BusinessWorkflowService.QuerySnapshot getTodoQuery(@PathVariable UUID workspaceId,
            @PathVariable UUID queryId, Authentication authentication) {
        return workflows.getQuery(ApiSupport.actor(authentication), workspaceId, queryId);
    }

    @PostMapping("/work-items/{workItemId}/external-state-queries")
    ResponseEntity<QueryCreated> createStateQuery(@PathVariable UUID workspaceId, @PathVariable UUID workItemId,
            @RequestHeader("Idempotency-Key") String key, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        requireKeys(raw, Set.of());
        return ResponseEntity.accepted().body(queryCreated(workflows.createServiceRequestStateQuery(
                ApiSupport.actor(authentication), workspaceId, workItemId, key)));
    }

    @GetMapping("/work-items/{workItemId}/external-state-queries/{queryId}")
    P27BusinessWorkflowService.QuerySnapshot getStateQuery(@PathVariable UUID workspaceId,
            @PathVariable UUID workItemId, @PathVariable UUID queryId, Authentication authentication) {
        return workflows.getServiceRequestStateQuery(ApiSupport.actor(authentication), workspaceId, workItemId, queryId);
    }

    @PostMapping("/work-items/{workItemId}/result-syncs")
    ResponseEntity<P27BusinessWorkflowService.ResultSyncSnapshot> createResultSync(@PathVariable UUID workspaceId,
            @PathVariable UUID workItemId, @RequestHeader("Idempotency-Key") String key,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        requireKeys(raw, Set.of("expectedWorkItemVersion", "stateQueryId"));
        var body = convert(raw, ResultSyncBody.class, Set.of("expectedWorkItemVersion", "stateQueryId"));
        if (body.expectedWorkItemVersion() == null || body.stateQueryId() == null)
            throw EafException.invalid("expectedWorkItemVersion 和 stateQueryId 必填。");
        return ResponseEntity.accepted().body(workflows.createResultSync(ApiSupport.actor(authentication),
                workspaceId, workItemId, body.expectedWorkItemVersion(), body.stateQueryId(), key));
    }

    @GetMapping("/work-items/{workItemId}/result-syncs")
    ResultSyncList listResultSyncs(@PathVariable UUID workspaceId, @PathVariable UUID workItemId,
            @RequestParam(required = false) String cursor, @RequestParam(defaultValue = "20") int limit,
            Authentication authentication) {
        var decoded = decode(cursor);
        var page = workflows.listResultSyncs(ApiSupport.actor(authentication), workspaceId, workItemId,
                decoded == null ? null : decoded.createdAt(), decoded == null ? null : decoded.id(), limit);
        return new ResultSyncList(page.items(), encode(page));
    }

    @GetMapping("/work-items/{workItemId}/result-syncs/{syncId}")
    P27BusinessWorkflowService.ResultSyncSnapshot getResultSync(@PathVariable UUID workspaceId,
            @PathVariable UUID workItemId, @PathVariable UUID syncId, Authentication authentication) {
        return workflows.getResultSync(ApiSupport.actor(authentication), workspaceId, workItemId, syncId);
    }

    @PostMapping("/work-items/{workItemId}/result-syncs/{syncId}/verify")
    P27BusinessWorkflowService.ResultSyncSnapshot verifyResultSync(@PathVariable UUID workspaceId,
            @PathVariable UUID workItemId, @PathVariable UUID syncId,
            @RequestHeader("Idempotency-Key") String key, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        requireKeys(raw, Set.of("reason"));
        var body = convert(raw, VerifyBody.class, Set.of("reason"));
        if (body.reason() == null || body.reason().isBlank() || body.reason().length() > 500)
            throw EafException.invalid("reason 必填且不能超过 500 个字符。");
        return workflows.verifyResultSync(ApiSupport.actor(authentication), workspaceId, workItemId, syncId, key,
                body.reason());
    }

    private QueryCreated queryCreated(WorkflowInstance instance) {
        var taskId = instance.rootTaskId();
        return new QueryCreated(instance.id(), instance.id(), taskId, instance.status());
    }

    private void requireKeys(Map<String, Object> raw, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("请求包含无效或未允许字段。");
    }

    private <T> T convert(Map<String, Object> raw, Class<T> type, Set<String> allowed) {
        requireKeys(raw, allowed);
        try { return json.convertValue(raw, type); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("请求字段类型无效。"); }
    }

    private Cursor decode(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            var fields = new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8).split("\n", -1);
            if (fields.length != 2) throw new IllegalArgumentException();
            return new Cursor(Instant.parse(fields[0]), UUID.fromString(fields[1]));
        } catch (RuntimeException invalid) { throw EafException.invalid("同步记录 cursor 无效。"); }
    }

    private String encode(P27BusinessWorkflowService.ResultSyncPage page) {
        if (page.nextCreatedAt() == null || page.nextSyncId() == null) return null;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
                (page.nextCreatedAt() + "\n" + page.nextSyncId()).getBytes(StandardCharsets.UTF_8));
    }

    private record Cursor(Instant createdAt, UUID id) { }
    private record OaListBody(String status, String cursor, Integer limit) { }
    private record OaItemBody(String todoId) { }
    private record ResultSyncBody(Long expectedWorkItemVersion, UUID stateQueryId) { }
    private record VerifyBody(String reason) { }
    private record QueryCreated(UUID queryId, UUID workflowInstanceId, UUID taskId, String status) { }
    private record ResultSyncList(List<P27BusinessWorkflowService.ResultSyncSnapshot> items, String nextCursor) {
        private ResultSyncList { items = List.copyOf(items); }
    }
}
