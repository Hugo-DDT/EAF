package io.eaf.agentprotocol;

import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.api.ManagedKnowledgeSource;
import io.eaf.knowledge.api.KnowledgeDocument;
import io.eaf.shared.EafException;
import java.math.BigInteger;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 仅适配受管 Knowledge REST；Owner、Workspace 和来源事实由 Knowledge 本域复核。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/knowledge")
public class ManagedKnowledgeController {
    private final KnowledgeService knowledge;

    public ManagedKnowledgeController(KnowledgeService knowledge) {
        this.knowledge = knowledge;
    }

    @PostMapping("/sources")
    ResponseEntity<ManagedKnowledgeSource> createSource(@PathVariable UUID workspaceId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        fields(body, Set.of("name", "type"), Set.of("name", "type"));
        var source = knowledge.createManagedSource(new ManagedKnowledgeSource.CreateCommand(ApiSupport.actor(authentication),
                workspaceId, string(body, "name"), string(body, "type"), idempotencyKey));
        return ResponseEntity.status(201).body(source);
    }

    @GetMapping("/sources")
    List<ManagedKnowledgeSource> listSources(@PathVariable UUID workspaceId,
            @RequestParam MultiValueMap<String, String> query, Authentication authentication) {
        queryFields(query, Set.of("limit", "offset"));
        return knowledge.listManagedSources(ApiSupport.actor(authentication), workspaceId,
                optionalInt(query, "limit", 20), optionalInt(query, "offset", 0));
    }

    @GetMapping("/sources/{sourceId}")
    ManagedKnowledgeSource getSource(@PathVariable UUID workspaceId, @PathVariable UUID sourceId,
                                     Authentication authentication) {
        return knowledge.getManagedSource(ApiSupport.actor(authentication), workspaceId, sourceId);
    }

    @PostMapping("/sources/{sourceId}/state")
    ManagedKnowledgeSource.StateReceipt changeSourceState(@PathVariable UUID workspaceId,
            @PathVariable UUID sourceId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        fields(body, Set.of("expectedSourceRevision", "status"), Set.of("expectedSourceRevision", "status"));
        return knowledge.changeManagedSourceState(ApiSupport.actor(authentication), workspaceId, sourceId,
                longValue(body.get("expectedSourceRevision"), "expectedSourceRevision"),
                string(body, "status"), idempotencyKey);
    }

    @PostMapping("/sources/{sourceId}/syncs")
    ManagedKnowledgeSource.SyncReceipt syncSource(@PathVariable UUID workspaceId, @PathVariable UUID sourceId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        fields(body, Set.of("expectedSourceRevision", "changes"), Set.of("expectedSourceRevision", "changes"));
        if (!(body.get("changes") instanceof List<?> rows)) throw EafException.invalid("changes 必须是数组。");
        var changes = rows.stream().map(this::change).toList();
        return knowledge.applyManagedSourceSync(new ManagedKnowledgeSource.BatchCommand(ApiSupport.actor(authentication),
                workspaceId, sourceId, longValue(body.get("expectedSourceRevision"), "expectedSourceRevision"),
                changes, idempotencyKey));
    }

    @GetMapping("/sources/{sourceId}/syncs/{syncId}")
    ManagedKnowledgeSource.SyncReceipt getSync(@PathVariable UUID workspaceId, @PathVariable UUID sourceId,
            @PathVariable UUID syncId, Authentication authentication) {
        return knowledge.getManagedSourceSync(ApiSupport.actor(authentication), workspaceId, sourceId, syncId);
    }

    @GetMapping("/sources/{sourceId}/items")
    List<ManagedKnowledgeSource.ItemSummary> listItems(@PathVariable UUID workspaceId,
            @PathVariable UUID sourceId, @RequestParam MultiValueMap<String, String> query,
            Authentication authentication) {
        queryFields(query, Set.of("limit", "offset"));
        return knowledge.listManagedSourceItems(ApiSupport.actor(authentication), workspaceId, sourceId,
                optionalInt(query, "limit", 20), optionalInt(query, "offset", 0));
    }

    @GetMapping("/documents/{documentId}/versions/{documentVersion}/source")
    ManagedKnowledgeSource.SourceVersion sourceVersion(@PathVariable UUID workspaceId,
            @PathVariable UUID documentId, @PathVariable int documentVersion, Authentication authentication) {
        return knowledge.getManagedSourceVersion(ApiSupport.actor(authentication), workspaceId, documentId, documentVersion);
    }

    @GetMapping("/sources/{sourceId}/items/{itemId}/versions/{documentVersion}")
    KnowledgeDocument sourceVersionForOwner(@PathVariable UUID workspaceId, @PathVariable UUID sourceId,
            @PathVariable String itemId, @PathVariable int documentVersion, Authentication authentication) {
        return knowledge.getManagedSourceVersionForOwner(ApiSupport.actor(authentication), workspaceId,
                sourceId, itemId, documentVersion);
    }

    @PostMapping("/neighborhoods")
    ManagedKnowledgeSource.Neighborhood neighborhood(@PathVariable UUID workspaceId,
            @RequestBody Map<String, Object> body, Authentication authentication) {
        fields(body, Set.of("documentId", "documentVersion", "buildId", "chunkId", "tokenBudget"),
                Set.of("documentId", "documentVersion", "buildId", "chunkId", "tokenBudget"));
        return knowledge.neighborhood(new ManagedKnowledgeSource.NeighborhoodRequest(ApiSupport.actor(authentication),
                workspaceId, uuid(body.get("documentId"), "documentId"),
                intValue(body.get("documentVersion"), "documentVersion"), uuid(body.get("buildId"), "buildId"),
                uuid(body.get("chunkId"), "chunkId"), intValue(body.get("tokenBudget"), "tokenBudget")));
    }

    private ManagedKnowledgeSource.Change change(Object row) {
        if (!(row instanceof Map<?, ?> raw)) throw EafException.invalid("changes 项必须是对象。");
        var change = new java.util.LinkedHashMap<String, Object>();
        raw.forEach((key, value) -> {
            if (!(key instanceof String text)) throw EafException.invalid("changes 项字段名无效。");
            change.put(text, value);
        });
        var type = string(change, "changeType");
        var itemId = string(change, "itemId");
        return switch (type) {
            case "UPSERT" -> {
                fields(change, Set.of("changeType", "itemId", "sourceVersion", "title", "format", "content", "aclVersion", "readActorIds"),
                        Set.of("changeType", "itemId", "sourceVersion", "title", "format", "content", "aclVersion", "readActorIds"));
                yield new ManagedKnowledgeSource.Change(type, itemId, string(change, "sourceVersion"),
                        string(change, "title"), string(change, "format"), string(change, "content"),
                        string(change, "aclVersion"), uuidSet(change.get("readActorIds")), null);
            }
            case "DELETE" -> {
                fields(change, Set.of("changeType", "itemId", "sourceVersion"), Set.of("changeType", "itemId", "sourceVersion"));
                yield new ManagedKnowledgeSource.Change(type, itemId, string(change, "sourceVersion"),
                        null, null, null, null, null, null);
            }
            case "ACCESS" -> {
                fields(change, Set.of("changeType", "itemId", "aclVersion", "readActorIds"),
                        Set.of("changeType", "itemId", "aclVersion", "readActorIds"));
                yield new ManagedKnowledgeSource.Change(type, itemId, null, null, null, null,
                        string(change, "aclVersion"), uuidSet(change.get("readActorIds")), null);
            }
            case "UNAVAILABLE" -> {
                fields(change, Set.of("changeType", "itemId", "reasonCode"), Set.of("changeType", "itemId", "reasonCode"));
                yield new ManagedKnowledgeSource.Change(type, itemId, null, null, null, null,
                        null, null, string(change, "reasonCode"));
            }
            default -> throw EafException.invalid("不支持的来源变更类型。");
        };
    }

    private Set<UUID> uuidSet(Object value) {
        if (!(value instanceof List<?> rows)) throw EafException.invalid("readActorIds 必须是 UUID 数组。");
        var ids = new LinkedHashSet<UUID>();
        for (var row : rows) if (!ids.add(uuid(row, "readActorIds")))
            throw EafException.invalid("readActorIds 不能包含重复主体。");
        return Set.copyOf(ids);
    }

    private static void fields(Map<?, ?> body, Set<String> allowed, Set<String> required) {
        if (body == null || body.keySet().stream().anyMatch(key -> !(key instanceof String text) || !allowed.contains(text))
                || !body.keySet().containsAll(required))
            throw EafException.invalid("请求包含缺失或未允许字段。");
    }

    private static String string(Map<?, ?> body, String name) {
        if (!(body.get(name) instanceof String value)) throw EafException.invalid(name + " 必须是字符串。");
        return value;
    }

    private static UUID uuid(Object value, String name) {
        if (!(value instanceof String text)) throw EafException.invalid(name + " 必须是 UUID。");
        try { return UUID.fromString(text); }
        catch (IllegalArgumentException malformed) { throw EafException.invalid(name + " 必须是 UUID。"); }
    }

    private static int intValue(Object value, String name) {
        long parsed = longValue(value, name);
        if (parsed < Integer.MIN_VALUE || parsed > Integer.MAX_VALUE) throw EafException.invalid(name + " 超出整数范围。");
        return (int) parsed;
    }

    private static long longValue(Object value, String name) {
        if (!(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof BigInteger)) throw EafException.invalid(name + " 必须是整数。");
        try { return value instanceof BigInteger big ? big.longValueExact() : Long.parseLong(value.toString()); }
        catch (ArithmeticException | NumberFormatException malformed) { throw EafException.invalid(name + " 必须是有效整数。"); }
    }

    private static void queryFields(MultiValueMap<String, String> query, Set<String> allowed) {
        if (query.keySet().stream().anyMatch(key -> !allowed.contains(key))) throw EafException.invalid("查询包含未允许字段。");
    }

    private static int optionalInt(MultiValueMap<String, String> query, String name, int fallback) {
        var values = query.get(name);
        if (values == null) return fallback;
        if (values.size() != 1) throw EafException.invalid(name + " 只能提供一次。");
        try { return Integer.parseInt(values.getFirst()); }
        catch (NumberFormatException malformed) { throw EafException.invalid(name + " 必须是整数。"); }
    }
}
