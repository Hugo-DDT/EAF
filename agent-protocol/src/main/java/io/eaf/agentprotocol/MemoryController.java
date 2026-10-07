package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.memory.api.CreateMemoryCommand;
import io.eaf.memory.api.CreateMemoryVersionCommand;
import io.eaf.memory.api.MemoryDefinition;
import io.eaf.memory.api.MemoryService;
import io.eaf.shared.EafException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/memories")
public class MemoryController {
    private final MemoryService memories;
    private final ObjectMapper json;

    public MemoryController(MemoryService memories, ObjectMapper json) {
        this.memories = memories;
        this.json = json;
    }

    @GetMapping
    MemoryList list(@PathVariable UUID workspaceId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        return new MemoryList(memories.list(actor, workspaceId).stream().map(this::view).toList(), null);
    }

    @PostMapping
    ResponseEntity<MemoryView> create(@PathVariable UUID workspaceId, @RequestBody Map<String, Object> rawBody,
                                      Authentication authentication) {
        var body = convert(rawBody, CreateBody.class,
                Set.of("logicalKey", "version", "type", "scope", "content", "confidence", "expiresAt", "sourceRef", "evidenceRefs", "businessEntityType", "businessEntityId"));
        var actor = ApiSupport.actor(authentication);
        var memory = memories.create(new CreateMemoryCommand(actor, workspaceId, body.logicalKey(), body.version(),
                body.type(), body.scope(), body.content(), body.confidence(), body.expiresAt(), body.sourceRef(), body.evidenceRefs(),
                body.businessEntityType(), body.businessEntityId()));
        return ResponseEntity.status(201).body(view(memory));
    }

    @PostMapping("/{memoryId}/versions")
    MemoryView addVersion(@PathVariable UUID workspaceId, @PathVariable UUID memoryId,
                          @RequestBody Map<String, Object> rawBody, Authentication authentication) {
        var body = convert(rawBody, VersionBody.class,
                Set.of("version", "type", "scope", "content", "confidence", "expiresAt", "sourceRef", "evidenceRefs", "businessEntityType", "businessEntityId"));
        return view(memories.addVersion(ApiSupport.actor(authentication), workspaceId, memoryId,
                new CreateMemoryVersionCommand(body.version(), body.type(), body.scope(), body.content(), body.confidence(),
                        body.expiresAt(), body.sourceRef(), body.evidenceRefs(), body.businessEntityType(), body.businessEntityId())));
    }

    @GetMapping("/{memoryId}/versions/{version}")
    MemoryView get(@PathVariable UUID workspaceId, @PathVariable UUID memoryId, @PathVariable String version,
                   Authentication authentication) {
        return view(memories.get(ApiSupport.actor(authentication), workspaceId, memoryId, version));
    }

    @PostMapping("/{memoryId}/versions/{version}/publish")
    MemoryView publish(@PathVariable UUID workspaceId, @PathVariable UUID memoryId, @PathVariable String version,
                       @RequestParam long expectedVersion, Authentication authentication) {
        return view(memories.publish(ApiSupport.actor(authentication), workspaceId, memoryId, version, expectedVersion));
    }

    @PostMapping("/{memoryId}/versions/{version}/revoke")
    MemoryView revoke(@PathVariable UUID workspaceId, @PathVariable UUID memoryId, @PathVariable String version,
                      @RequestParam long expectedVersion, Authentication authentication) {
        return view(memories.revoke(ApiSupport.actor(authentication), workspaceId, memoryId, version, expectedVersion));
    }

    private <T> T convert(Map<String, Object> rawBody, Class<T> type, Set<String> allowed) {
        if (rawBody == null || rawBody.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("请求包含空体或未允许字段。");
        try { return json.convertValue(rawBody, type); }
        catch (IllegalArgumentException e) { throw EafException.invalid("请求字段类型无效。"); }
    }

    private MemoryView view(MemoryDefinition memory) {
        return new MemoryView(memory.id(), memory.ownerId(), memory.logicalKey(), memory.version(), memory.type(),
                memory.scope(), memory.businessEntityType(), memory.businessEntityId(), memory.content(), memory.confidence(), memory.expiresAt(), memory.sourceType(),
                memory.sourceRef(), memory.evidenceRefs(), memory.status(), memory.rowVersion(), memory.contentHash());
    }

    record MemoryList(List<MemoryView> items, String nextCursor) { }
    record CreateBody(String logicalKey, String version, String type, String scope, String content,
                      Double confidence, Instant expiresAt, String sourceRef, List<String> evidenceRefs,
                      String businessEntityType, String businessEntityId) { }
    record VersionBody(String version, String type, String scope, String content, Double confidence,
                       Instant expiresAt, String sourceRef, List<String> evidenceRefs,
                       String businessEntityType, String businessEntityId) { }
    record MemoryView(UUID id, UUID ownerId, String logicalKey, String version, String type, String scope,
                      String businessEntityType, String businessEntityId,
                      String content, double confidence, Instant expiresAt, String sourceType, String sourceRef,
                      List<String> evidenceRefs, String status, long rowVersion, String contentHash) { }
}
// REST 适配只接收 Owner 可声明的草稿内容；身份、来源类型、发布状态和权限由 Memory 模块控制。
