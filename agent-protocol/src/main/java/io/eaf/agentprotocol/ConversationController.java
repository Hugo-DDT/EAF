package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.EafException;
import io.eaf.task.api.TaskService;
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
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/conversations")
public class ConversationController {
    private final ConversationApplicationService application;
    private final ObjectMapper json;

    public ConversationController(ConversationApplicationService application, ObjectMapper json) {
        this.application = application;
        this.json = json;
    }

    @PostMapping
    ResponseEntity<ConversationApplicationService.ConversationResponse> create(
            @PathVariable UUID workspaceId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var request = convert(raw, ConversationApplicationService.CreateRequest.class,
                Set.of("mode", "title", "customerId"));
        var response = application.create(ApiSupport.actor(authentication), workspaceId, request, idempotencyKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping
    TaskService.ConversationPage list(@PathVariable UUID workspaceId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Instant cursorUpdatedAt,
            @RequestParam(required = false) UUID cursorId,
            @RequestParam(defaultValue = "20") int limit, Authentication authentication) {
        if (limit < 1 || limit > 50 || (cursorUpdatedAt == null) != (cursorId == null))
            throw EafException.invalid("会话列表 limit 或游标无效。");
        return application.list(ApiSupport.actor(authentication), workspaceId, status, cursorUpdatedAt, cursorId, limit);
    }

    @GetMapping("/{conversationId}")
    ConversationApplicationService.ConversationResponse get(@PathVariable UUID workspaceId,
            @PathVariable UUID conversationId, Authentication authentication) {
        return application.get(ApiSupport.actor(authentication), workspaceId, conversationId);
    }

    @PatchMapping("/{conversationId}")
    ConversationApplicationService.ConversationResponse update(@PathVariable UUID workspaceId,
            @PathVariable UUID conversationId, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        var request = convert(raw, ConversationApplicationService.UpdateRequest.class,
                Set.of("expectedVersion", "title", "status"));
        return application.update(ApiSupport.actor(authentication), workspaceId, conversationId, request);
    }

    @GetMapping("/{conversationId}/turns")
    ConversationApplicationService.TurnPageResponse turns(@PathVariable UUID workspaceId,
            @PathVariable UUID conversationId, @RequestParam(required = false) Integer beforeTurnNo,
            @RequestParam(defaultValue = "20") int limit, Authentication authentication) {
        return application.turns(ApiSupport.actor(authentication), workspaceId, conversationId, beforeTurnNo,
                limit);
    }

    @PostMapping("/{conversationId}/turns")
    ResponseEntity<ConversationApplicationService.SendResponse> send(@PathVariable UUID workspaceId,
            @PathVariable UUID conversationId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var request = convert(raw, ConversationApplicationService.SendRequest.class, Set.of("input", "followupResultIds"));
        var response = application.send(ApiSupport.actor(authentication), workspaceId, conversationId, request,
                idempotencyKey);
        return ResponseEntity.accepted().header("Location", "/api/v1/workspaces/%s/conversations/%s/turns"
                .formatted(workspaceId, conversationId)).body(response);
    }

    @GetMapping("/{conversationId}/brief")
    TaskService.ConversationBrief brief(@PathVariable UUID workspaceId, @PathVariable UUID conversationId,
                                        Authentication authentication) {
        return application.brief(ApiSupport.actor(authentication), workspaceId, conversationId);
    }

    @PutMapping("/{conversationId}/brief")
    TaskService.ConversationBriefSave saveBrief(@PathVariable UUID workspaceId,
            @PathVariable UUID conversationId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var request = convert(raw, ConversationApplicationService.SaveBriefRequest.class,
                Set.of("expectedRevision", "content", "suggestionTaskId"));
        return application.saveBrief(ApiSupport.actor(authentication), workspaceId, conversationId, request,
                idempotencyKey);
    }

    @GetMapping("/{conversationId}/analysis-comparison")
    ConversationApplicationService.AnalysisComparisonResponse comparison(@PathVariable UUID workspaceId,
            @PathVariable UUID conversationId, Authentication authentication) {
        return application.comparison(ApiSupport.actor(authentication), workspaceId, conversationId);
    }

    @GetMapping("/{conversationId}/usage")
    ConversationApplicationService.ConversationUsageResponse usage(@PathVariable UUID workspaceId,
            @PathVariable UUID conversationId, Authentication authentication) {
        return application.usage(ApiSupport.actor(authentication), workspaceId, conversationId);
    }

    @PostMapping("/{conversationId}/followups")
    ResponseEntity<TaskApplicationService.FollowupResponse> confirmFollowup(@PathVariable UUID workspaceId,
            @PathVariable UUID conversationId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var request = convert(raw, ConversationApplicationService.ConfirmFollowupRequest.class,
                Set.of("sourceTaskId", "expectedTaskVersion", "expectedBriefRevision", "summary"));
        var response = application.confirmFollowup(ApiSupport.actor(authentication), workspaceId, conversationId,
                request, idempotencyKey);
        return ResponseEntity.accepted().body(response);
    }

    @GetMapping("/{conversationId}/followups")
    java.util.List<TaskApplicationService.FollowupResponse> followups(@PathVariable UUID workspaceId,
            @PathVariable UUID conversationId, Authentication authentication) {
        return application.followups(ApiSupport.actor(authentication), workspaceId, conversationId);
    }

    private <T> T convert(Map<String, Object> raw, Class<T> type, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("请求包含缺失或未允许字段。");
        try { return json.convertValue(raw, type); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("请求字段格式无效。"); }
    }
}
