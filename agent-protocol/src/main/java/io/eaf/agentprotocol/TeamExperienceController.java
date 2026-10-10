package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.memory.api.TeamExperienceService;
import io.eaf.shared.EafException;
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

/** REST 适配器；TEAM 读写始终由 Memory Owner 授权。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/team-experiences")
public class TeamExperienceController {
    private final TeamExperienceService experiences;
    private final TeamExperienceApplicationService application;
    private final ObjectMapper json;

    public TeamExperienceController(TeamExperienceService experiences, TeamExperienceApplicationService application,
                                   ObjectMapper json) {
        this.experiences = experiences;
        this.application = application;
        this.json = json;
    }

    @GetMapping
    TeamExperienceList list(@PathVariable UUID workspaceId,
            @RequestParam(required = false) String scenarioKey, @RequestParam(required = false) String view,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String cursor, @RequestParam(defaultValue = "20") int limit,
            Authentication authentication) {
        var decoded = decode(cursor);
        if (view != null && !"owned".equals(view)) throw EafException.invalid("team experiences view 只支持 owned。");
        var page = experiences.list(ApiSupport.actor(authentication), workspaceId, scenarioKey, "owned".equals(view),
                status, decoded == null ? null : decoded.updatedAt(), decoded == null ? null : decoded.id(), limit);
        return new TeamExperienceList(page.items(), encode(page));
    }

    @PostMapping("/discover")
    TeamExperienceService.TeamExperienceDiscovery discover(@PathVariable UUID workspaceId,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, DiscoverRequest.class, Set.of("scenarioKey", "keywords", "limit"));
        return experiences.discover(ApiSupport.actor(authentication), workspaceId, body.scenarioKey(), body.keywords(),
                body.limit() == null ? 10 : body.limit());
    }

    @GetMapping("/{cardId}")
    TeamExperienceService.TeamExperience get(@PathVariable UUID workspaceId, @PathVariable UUID cardId,
                                               Authentication authentication) {
        return experiences.get(ApiSupport.actor(authentication), workspaceId, cardId);
    }

    @GetMapping("/{cardId}/versions")
    List<TeamExperienceService.TeamExperienceRevision> versions(@PathVariable UUID workspaceId,
            @PathVariable UUID cardId, @RequestParam(required = false) Integer beforeRevision,
            @RequestParam(defaultValue = "20") int limit, Authentication authentication) {
        return experiences.versions(ApiSupport.actor(authentication), workspaceId, cardId, beforeRevision, limit);
    }

    @PostMapping
    ResponseEntity<TeamExperienceService.CardReceipt> create(@PathVariable UUID workspaceId,
            @RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        var body = convert(raw, TeamExperienceApplicationService.CreateRequest.class,
                Set.of("scenarioKey", "title", "appliesWhen", "content", "expiresAt", "sourceWorkItemId"));
        var receipt = application.create(ApiSupport.actor(authentication), workspaceId, body, idempotencyKey);
        return ResponseEntity.status(receipt.replayed() ? HttpStatus.OK : HttpStatus.CREATED).body(receipt);
    }

    @PostMapping("/{cardId}/versions")
    TeamExperienceService.CardReceipt save(@PathVariable UUID workspaceId, @PathVariable UUID cardId,
            @RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        var body = convert(raw, TeamExperienceApplicationService.SaveRequest.class,
                Set.of("expectedVersion", "title", "appliesWhen", "content", "expiresAt", "sourceWorkItemId"));
        return application.save(ApiSupport.actor(authentication), workspaceId, cardId, body, idempotencyKey);
    }

    @PostMapping("/{cardId}/versions/{revision}/publish")
    TeamExperienceService.CardReceipt publish(@PathVariable UUID workspaceId, @PathVariable UUID cardId,
            @PathVariable int revision, @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, VersionCheck.class, Set.of("expectedVersion"));
        return application.publish(ApiSupport.actor(authentication), workspaceId, cardId, revision,
                body.expectedVersion(), idempotencyKey);
    }

    @PostMapping("/{cardId}/revoke")
    TeamExperienceService.CardReceipt revoke(@PathVariable UUID workspaceId, @PathVariable UUID cardId,
            @RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        var body = convert(raw, TeamExperienceApplicationService.RevokeRequest.class,
                Set.of("expectedVersion", "expectedActiveRevision"));
        return application.revoke(ApiSupport.actor(authentication), workspaceId, cardId, body, idempotencyKey);
    }

    private <T> T convert(Map<String, Object> raw, Class<T> type, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("团队经验请求包含空体或未允许字段。");
        try { return json.convertValue(raw, type); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("团队经验请求字段格式无效。"); }
    }

    private Cursor decode(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            var fields = new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8).split("\n", -1);
            if (fields.length != 2) throw new IllegalArgumentException();
            return new Cursor(Instant.parse(fields[0]), UUID.fromString(fields[1]));
        } catch (RuntimeException invalid) { throw EafException.invalid("团队经验 cursor 无效。"); }
    }

    private String encode(TeamExperienceService.TeamExperiencePage page) {
        if (page.nextCursorUpdatedAt() == null || page.nextCursorId() == null) return null;
        return Base64.getUrlEncoder().withoutPadding().encodeToString((page.nextCursorUpdatedAt() + "\n" + page.nextCursorId())
                .getBytes(StandardCharsets.UTF_8));
    }

    private record Cursor(Instant updatedAt, UUID id) { }
    private record DiscoverRequest(String scenarioKey, List<String> keywords, Integer limit) { }
    private record VersionCheck(long expectedVersion) { }
    private record TeamExperienceList(List<TeamExperienceService.TeamExperience> items, String nextCursor) { }
}
