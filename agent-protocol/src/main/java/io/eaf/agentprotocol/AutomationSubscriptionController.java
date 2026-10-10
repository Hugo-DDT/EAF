package io.eaf.agentprotocol;

import io.eaf.shared.EafException;
import io.eaf.workflow.api.AutomationRun;
import io.eaf.workflow.api.AutomationRunPage;
import io.eaf.workflow.api.AutomationSubscription;
import io.eaf.workflow.api.AutomationSubscriptionPage;
import io.eaf.workflow.api.CreateAutomationSubscriptionCommand;
import io.eaf.workflow.api.WorkflowAutomationService;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/automation-subscriptions")
public class AutomationSubscriptionController {
    private static final Set<String> COMMON = Set.of("name", "triggerKind", "expiresAt", "maxRuns");
    private final WorkflowAutomationService automations;

    public AutomationSubscriptionController(WorkflowAutomationService automations) {
        this.automations = automations;
    }

    @PostMapping
    ResponseEntity<AutomationSubscription> create(@PathVariable UUID workspaceId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        if (body == null || !body.keySet().containsAll(COMMON)) throw EafException.invalid("订阅请求字段缺失或格式无效。");
        var trigger = string(body.get("triggerKind"), "triggerKind");
        DayOfWeek day = null;
        LocalTime time = null;
        String zone = null;
        UUID workItem = null;
        int maxItems = 0;
        Set<String> allowed;
        if ("WEEKLY".equals(trigger)) {
            allowed = Set.of("name", "triggerKind", "dayOfWeek", "localTime", "timeZone", "maxItems", "expiresAt", "maxRuns");
            if (!body.keySet().equals(allowed)) throw EafException.invalid("周订阅必须且只能包含计划与范围字段。");
            try {
                day = DayOfWeek.valueOf(string(body.get("dayOfWeek"), "dayOfWeek").toUpperCase(java.util.Locale.ROOT));
                time = LocalTime.parse(string(body.get("localTime"), "localTime"));
            } catch (RuntimeException invalid) { throw EafException.invalid("dayOfWeek 或 localTime 格式无效。"); }
            zone = string(body.get("timeZone"), "timeZone");
            maxItems = integer(body.get("maxItems"), "maxItems");
        } else if ("P16_WORK_ITEM_CHANGED".equals(trigger)) {
            allowed = body.containsKey("maxItems") ? Set.of("name", "triggerKind", "workItemId", "maxItems", "expiresAt", "maxRuns")
                    : Set.of("name", "triggerKind", "workItemId", "expiresAt", "maxRuns");
            if (!body.keySet().equals(allowed)) throw EafException.invalid("事件订阅必须且只能包含一个工作项来源。");
            workItem = uuid(body.get("workItemId"), "workItemId");
            if (body.containsKey("maxItems")) maxItems = integer(body.get("maxItems"), "maxItems");
        } else throw EafException.invalid("仅支持 WEEKLY 或 P16_WORK_ITEM_CHANGED。");
        var result = automations.create(new CreateAutomationSubscriptionCommand(ApiSupport.actor(authentication), workspaceId,
                string(body.get("name"), "name"), trigger, day, time, zone, workItem, maxItems,
                instant(body.get("expiresAt"), "expiresAt"), integer(body.get("maxRuns"), "maxRuns"), idempotencyKey));
        return ResponseEntity.status(HttpStatus.CREATED).body(result);
    }

    @GetMapping
    AutomationSubscriptionPage list(@PathVariable UUID workspaceId,
            @RequestParam(required = false) Instant cursorCreatedAt,
            @RequestParam(required = false) UUID cursorId,
            @RequestParam(defaultValue = "20") int limit,
            Authentication authentication) {
        return automations.list(ApiSupport.actor(authentication), workspaceId, cursorCreatedAt, cursorId, limit);
    }

    @GetMapping("/{subscriptionId}")
    AutomationSubscription get(@PathVariable UUID workspaceId, @PathVariable UUID subscriptionId,
            Authentication authentication) {
        return automations.get(ApiSupport.actor(authentication), workspaceId, subscriptionId);
    }

    @PostMapping("/{subscriptionId}/pause")
    AutomationSubscription pause(@PathVariable UUID workspaceId, @PathVariable UUID subscriptionId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        return automations.pause(ApiSupport.actor(authentication), workspaceId, subscriptionId,
                expectedVersion(body), idempotencyKey);
    }

    @PostMapping("/{subscriptionId}/resume")
    AutomationSubscription resume(@PathVariable UUID workspaceId, @PathVariable UUID subscriptionId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        return automations.resume(ApiSupport.actor(authentication), workspaceId, subscriptionId,
                expectedVersion(body), idempotencyKey);
    }

    @DeleteMapping("/{subscriptionId}")
    AutomationSubscription delete(@PathVariable UUID workspaceId, @PathVariable UUID subscriptionId,
            @RequestParam long expectedVersion,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            Authentication authentication) {
        return automations.delete(ApiSupport.actor(authentication), workspaceId, subscriptionId, expectedVersion, idempotencyKey);
    }

    @GetMapping("/{subscriptionId}/runs")
    AutomationRunPage runs(@PathVariable UUID workspaceId, @PathVariable UUID subscriptionId,
            @RequestParam(required = false) Instant cursorCreatedAt,
            @RequestParam(required = false) UUID cursorId,
            @RequestParam(defaultValue = "20") int limit,
            Authentication authentication) {
        return automations.listRuns(ApiSupport.actor(authentication), workspaceId, subscriptionId,
                cursorCreatedAt, cursorId, limit);
    }

    @GetMapping("/{subscriptionId}/runs/{runId}")
    AutomationRun run(@PathVariable UUID workspaceId, @PathVariable UUID subscriptionId,
            @PathVariable UUID runId, Authentication authentication) {
        return automations.getRun(ApiSupport.actor(authentication), workspaceId, subscriptionId, runId);
    }

    private static long expectedVersion(Map<String, Object> body) {
        if (body == null || !body.keySet().equals(Set.of("expectedVersion")))
            throw EafException.invalid("expectedVersion 必须是唯一请求字段。");
        return longNumber(body.get("expectedVersion"), "expectedVersion");
    }

    private static String string(Object value, String field) {
        if (!(value instanceof String text) || text.isBlank()) throw EafException.invalid(field + " 必须是非空字符串。");
        return text;
    }

    private static UUID uuid(Object value, String field) {
        try {
            var text = string(value, field);
            var result = UUID.fromString(text);
            if (!result.toString().equalsIgnoreCase(text)) throw new IllegalArgumentException();
            return result;
        } catch (RuntimeException invalid) { throw EafException.invalid(field + " 必须是 UUID。"); }
    }

    private static Instant instant(Object value, String field) {
        try { return Instant.parse(string(value, field)); }
        catch (RuntimeException invalid) { throw EafException.invalid(field + " 必须是 ISO-8601 时间。"); }
    }

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
}
