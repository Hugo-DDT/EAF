package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.memory.api.ExperienceCardService;
import io.eaf.shared.EafException;
import java.time.Instant;
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

/** 个人经验卡的 HTTP 适配器；身份和 Memory 生命周期均由服务端确认。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/experience-cards")
public class ExperienceCardController {
    private final ExperienceCardService cards;
    private final ExperienceCardApplicationService application;
    private final ObjectMapper json;

    public ExperienceCardController(ExperienceCardService cards, ExperienceCardApplicationService application,
                                    ObjectMapper json) {
        this.cards = cards;
        this.application = application;
        this.json = json;
    }

    @GetMapping
    ExperienceCardService.CardPage list(@PathVariable UUID workspaceId,
            @RequestParam(required = false) String applicability,
            @RequestParam(required = false) String customerId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Instant cursorUpdatedAt,
            @RequestParam(required = false) UUID cursorId,
            @RequestParam(defaultValue = "20") int limit, Authentication authentication) {
        return cards.list(ApiSupport.actor(authentication), workspaceId, applicability, customerId, status,
                cursorUpdatedAt, cursorId, limit);
    }

    @PostMapping
    ResponseEntity<ExperienceCardService.CardReceipt> create(@PathVariable UUID workspaceId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, ExperienceCardApplicationService.CreateRequest.class,
                Set.of("title", "content", "type", "applicability", "customerId", "expiresAt", "sourceTaskId", "sourceFeedbackId", "draftTaskId"));
        var receipt = application.create(ApiSupport.actor(authentication), workspaceId, body, idempotencyKey);
        return ResponseEntity.status(receipt.replayed() ? HttpStatus.OK : HttpStatus.CREATED).body(receipt);
    }

    @GetMapping("/{cardId}")
    ExperienceCardService.ExperienceCard get(@PathVariable UUID workspaceId, @PathVariable UUID cardId,
                                              Authentication authentication) {
        return cards.get(ApiSupport.actor(authentication), workspaceId, cardId);
    }

    @GetMapping("/{cardId}/versions")
    java.util.List<ExperienceCardService.ExperienceCardRevision> versions(@PathVariable UUID workspaceId,
            @PathVariable UUID cardId, @RequestParam(required = false) Integer beforeRevision,
            @RequestParam(defaultValue = "20") int limit, Authentication authentication) {
        return cards.versions(ApiSupport.actor(authentication), workspaceId, cardId, beforeRevision, limit);
    }

    @PostMapping("/{cardId}/versions")
    ExperienceCardService.CardReceipt save(@PathVariable UUID workspaceId, @PathVariable UUID cardId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, ExperienceCardApplicationService.SaveRequest.class,
                Set.of("expectedVersion", "title", "content", "type", "expiresAt", "sourceTaskId", "sourceFeedbackId", "draftTaskId"));
        return application.save(ApiSupport.actor(authentication), workspaceId, cardId, body, idempotencyKey);
    }

    @PostMapping("/{cardId}/versions/{revision}/publish")
    ExperienceCardService.CardReceipt publish(@PathVariable UUID workspaceId, @PathVariable UUID cardId,
            @PathVariable int revision, @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, VersionCheck.class, Set.of("expectedVersion"));
        return cards.publish(new ExperienceCardService.PublishExperienceCard(ApiSupport.actor(authentication),
                workspaceId, cardId, revision, body.expectedVersion(), idempotencyKey));
    }

    @PostMapping("/{cardId}/revoke")
    ExperienceCardService.CardReceipt revoke(@PathVariable UUID workspaceId, @PathVariable UUID cardId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, RevokeBody.class, Set.of("expectedVersion", "expectedActiveRevision"));
        return cards.revoke(new ExperienceCardService.RevokeExperienceCard(ApiSupport.actor(authentication),
                workspaceId, cardId, body.expectedVersion(), body.expectedActiveRevision(), idempotencyKey));
    }

    private <T> T convert(Map<String, Object> raw, Class<T> type, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("请求包含空体或未允许字段。");
        try { return json.convertValue(raw, type); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("请求字段格式无效。"); }
    }

    record VersionCheck(long expectedVersion) { }
    record RevokeBody(long expectedVersion, int expectedActiveRevision) { }
}
