package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.EafException;
import java.time.Instant;
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

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}")
public class CustomerFollowupController {
    private final CustomerFollowupApplicationService application;
    private final ObjectMapper json;

    public CustomerFollowupController(CustomerFollowupApplicationService application, ObjectMapper json) {
        this.application = application;
        this.json = json;
    }

    @PostMapping("/customer-followups")
    ResponseEntity<CustomerFollowupApplicationService.CreateResponse> create(@PathVariable UUID workspaceId,
            @RequestHeader(value = "Idempotency-Key", required = false) String requestKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var request = convert(raw, CustomerFollowupApplicationService.CreateRequest.class,
                Set.of("conversationId", "sourceTaskId", "expectedTaskVersion", "expectedBriefRevision",
                        "summary", "assigneeId", "dueAt"));
        var response = application.create(ApiSupport.actor(authentication), workspaceId, request, requestKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/customer-followups")
    io.eaf.task.api.CustomerFollowupService.FollowupPage list(@PathVariable UUID workspaceId,
            @RequestParam(defaultValue = "mine") String scope, @RequestParam(required = false) String customerId,
            @RequestParam(required = false) UUID assigneeId, @RequestParam(required = false) String status,
            @RequestParam(required = false) Boolean overdue, @RequestParam(required = false) Instant cursorUpdatedAt,
            @RequestParam(required = false) UUID cursorId, @RequestParam(defaultValue = "20") int limit,
            Authentication authentication) {
        return application.list(ApiSupport.actor(authentication), workspaceId, scope, customerId, assigneeId,
                status, overdue, cursorUpdatedAt, cursorId, limit);
    }

    @GetMapping("/customer-followups/{followupId}")
    io.eaf.task.api.CustomerFollowupService.FollowupDetail get(@PathVariable UUID workspaceId,
            @PathVariable UUID followupId, Authentication authentication) {
        return application.get(ApiSupport.actor(authentication), workspaceId, followupId);
    }

    @GetMapping("/customer-followups/{followupId}/results")
    io.eaf.task.api.CustomerFollowupService.ResultPage results(@PathVariable UUID workspaceId,
            @PathVariable UUID followupId, @RequestParam(required = false) Integer beforeResultNo,
            @RequestParam(defaultValue = "20") int limit, Authentication authentication) {
        return application.results(ApiSupport.actor(authentication), workspaceId, followupId, beforeResultNo, limit);
    }

    @PatchMapping("/customer-followups/{followupId}")
    io.eaf.task.api.CustomerFollowupService.CardSnapshot update(@PathVariable UUID workspaceId,
            @PathVariable UUID followupId, @RequestBody Map<String, Object> raw, Authentication authentication) {
        if (raw == null || !raw.containsKey("expectedVersion"))
            throw EafException.invalid("expectedVersion 必填。");
        var request = convert(raw, PatchRequest.class, Set.of("expectedVersion", "assigneeId", "dueAt"));
        return application.update(ApiSupport.actor(authentication), workspaceId, followupId, request.expectedVersion(),
                request.assigneeId(), raw.containsKey("assigneeId"), request.dueAt(), raw.containsKey("dueAt"));
    }

    @PostMapping("/customer-followups/{followupId}/results")
    ResponseEntity<io.eaf.task.api.CustomerFollowupService.ResultReceipt> appendResult(@PathVariable UUID workspaceId,
            @PathVariable UUID followupId, @RequestHeader(value = "Idempotency-Key", required = false) String requestKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var request = convert(raw, CustomerFollowupApplicationService.ResultRequest.class,
                Set.of("expectedVersion", "outcomeCode", "summary", "nextAction", "nextContactAt",
                        "disposition", "correctsResultId"));
        var response = application.appendResult(ApiSupport.actor(authentication), workspaceId, followupId, request, requestKey);
        return ResponseEntity.status(response.created() ? HttpStatus.CREATED : HttpStatus.OK).body(response);
    }

    @PostMapping("/customer-followups/{followupId}/results/{resultId}/sync")
    ResponseEntity<CustomerFollowupApplicationService.SyncProjection> startSync(@PathVariable UUID workspaceId,
            @PathVariable UUID followupId, @PathVariable UUID resultId,
            @RequestHeader(value = "Idempotency-Key", required = false) String requestKey,
            @RequestBody(required = false) Map<String, Object> raw, Authentication authentication) {
        if (raw != null && !raw.isEmpty()) throw EafException.invalid("结果同步请求体必须为空对象。");
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(application.startSync(ApiSupport.actor(authentication),
                workspaceId, followupId, resultId, requestKey));
    }

    @GetMapping("/customer-followups/{followupId}/results/{resultId}/sync")
    CustomerFollowupApplicationService.SyncProjection syncStatus(@PathVariable UUID workspaceId,
            @PathVariable UUID followupId, @PathVariable UUID resultId, Authentication authentication) {
        return application.syncStatus(ApiSupport.actor(authentication), workspaceId, followupId, resultId);
    }

    @PostMapping("/customer-followups/{followupId}/results/{resultId}/verify")
    CustomerFollowupApplicationService.SyncProjection verifySync(@PathVariable UUID workspaceId,
            @PathVariable UUID followupId, @PathVariable UUID resultId,
            @RequestHeader(value = "Idempotency-Key", required = false) String requestKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var request = convert(raw, VerifyRequest.class, Set.of("reason"));
        return application.verifySync(ApiSupport.actor(authentication), workspaceId, followupId, resultId,
                requestKey, request.reason());
    }

    @GetMapping("/customer-followup-assignees")
    java.util.List<io.eaf.task.api.CustomerFollowupService.Assignee> assignees(@PathVariable UUID workspaceId,
            @RequestParam String customerId, @RequestParam(defaultValue = "20") int limit,
            Authentication authentication) {
        return application.assignees(ApiSupport.actor(authentication), workspaceId, customerId, limit);
    }

    private <T> T convert(Map<String, Object> raw, Class<T> type, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("请求包含未允许字段。");
        try { return json.convertValue(raw, type); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("请求字段格式无效。"); }
    }

    public record PatchRequest(long expectedVersion, UUID assigneeId, Instant dueAt) { }
    public record VerifyRequest(String reason) { }
}
