package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.knowledge.api.CreateKnowledgeDocumentCommand;
import io.eaf.knowledge.api.CreateKnowledgeVersionCommand;
import io.eaf.knowledge.api.KnowledgeDocument;
import io.eaf.knowledge.api.KnowledgeIndexBuild;
import io.eaf.knowledge.api.KnowledgePublication;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.api.ContextShare;
import io.eaf.knowledge.api.ContextSharePage;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.util.MultiValueMap;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/knowledge/documents")
public class KnowledgeController {
    private final KnowledgeService knowledge;
    private final ObjectMapper mapper;

    public KnowledgeController(KnowledgeService knowledge, ObjectMapper mapper) {
        this.knowledge = knowledge;
        this.mapper = mapper;
    }

    @PutMapping("/{documentId}/context-shares/{recipientId}")
    ContextShare shareContext(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                              @PathVariable UUID recipientId, @RequestBody Map<String, Object> body,
                              Authentication authentication) {
        if (body == null || body.keySet().stream().anyMatch(key -> !Set.of("documentVersion", "expectedVersion").contains(key))
                || !body.containsKey("documentVersion") || !body.containsKey("expectedVersion"))
            throw io.eaf.shared.EafException.invalid("正式知识共享请求只能包含 documentVersion 和 expectedVersion。");
        var version = PlatformAccessApplicationService.optionalInteger(body, "documentVersion");
        var expected = longValue(body.get("expectedVersion"), "expectedVersion");
        if (version == null) throw io.eaf.shared.EafException.invalid("documentVersion 必须是整数。");
        return knowledge.shareContext(ApiSupport.actor(authentication), workspaceId, documentId, recipientId, version, expected);
    }

    @GetMapping("/{documentId}/context-shares")
    ContextSharePage contextShares(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                   @RequestParam MultiValueMap<String, String> query, Authentication authentication) {
        if (query.keySet().stream().anyMatch(key -> !Set.of("limit", "offset").contains(key)))
            throw io.eaf.shared.EafException.invalid("共享列表查询包含未允许字段。");
        var limit = PlatformAccessApplicationService.optionalQueryInteger(single(query, "limit"), "limit");
        var offset = PlatformAccessApplicationService.optionalQueryInteger(single(query, "offset"), "offset");
        return knowledge.listContextShares(ApiSupport.actor(authentication), workspaceId, documentId,
                limit == null ? 20 : limit, offset == null ? 0 : offset);
    }

    @PostMapping("/{documentId}/context-shares/{recipientId}/revoke")
    ContextShare revokeContextShare(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                    @PathVariable UUID recipientId, @RequestBody Map<String, Object> body,
                                    Authentication authentication) {
        if (body == null || body.keySet().stream().anyMatch(key -> !Set.of("expectedVersion").contains(key))
                || !body.containsKey("expectedVersion"))
            throw io.eaf.shared.EafException.invalid("共享撤回请求只能包含 expectedVersion。");
        return knowledge.revokeContextShare(ApiSupport.actor(authentication), workspaceId, documentId, recipientId,
                longValue(body.get("expectedVersion"), "expectedVersion"));
    }

    @GetMapping
    io.eaf.knowledge.api.KnowledgeDocumentPage list(@PathVariable UUID workspaceId,
            @RequestParam(required = false) java.time.Instant cursorCreatedAt,
            @RequestParam(required = false) UUID cursorId,
            @RequestParam(defaultValue = "20") int limit, Authentication authentication) {
        if (limit < 1 || limit > 50 || (cursorCreatedAt == null) != (cursorId == null))
            throw io.eaf.shared.EafException.invalid("Knowledge 文档列表 limit 或游标无效。");
        return knowledge.listDocuments(ApiSupport.actor(authentication), workspaceId, cursorCreatedAt, cursorId, limit);
    }

    @PostMapping
    ResponseEntity<KnowledgeDocument> create(@PathVariable UUID workspaceId,
                                              @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                              @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
                                              @RequestBody Map<String, Object> rawBody, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        if (rawBody == null) throw io.eaf.shared.EafException.invalid("请求体不能为空。");
        var unknown = rawBody == null ? null : rawBody.keySet().stream()
                .filter(key -> !Set.of("title", "sourceRef", "content", "metadata").contains(key))
                .findFirst().orElse(null);
        if (unknown != null) throw io.eaf.shared.EafException.invalid("请求包含未允许字段。");
        var body = mapper.convertValue(rawBody, CreateBody.class);
        var document = knowledge.create(new CreateKnowledgeDocumentCommand(actor, workspaceId, body.title(), body.sourceRef(),
                body.content(), stringMetadata(body.metadata()), idempotencyKey,
                traceId == null || traceId.isBlank() ? UUID.randomUUID().toString() : traceId));
        return ResponseEntity.status(201).body(document);
    }

    // 新正文先成为独立草稿版本，索引 READY 前不会改变普通检索。
    @PostMapping("/{documentId}/versions")
    ResponseEntity<KnowledgeDocument> createVersion(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                                     @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                                     @RequestBody Map<String, Object> rawBody, Authentication authentication) {
        if (rawBody == null || rawBody.keySet().stream().anyMatch(key -> !Set.of("expectedRowVersion", "content").contains(key)))
            throw io.eaf.shared.EafException.invalid("请求包含空体或未允许字段。");
        var body = mapper.convertValue(rawBody, VersionBody.class);
        var version = knowledge.createVersion(new CreateKnowledgeVersionCommand(ApiSupport.actor(authentication), workspaceId,
                documentId, body.expectedRowVersion(), body.content(), idempotencyKey));
        return ResponseEntity.status(201).body(version);
    }

    @GetMapping("/{documentId}")
    KnowledgeDocument get(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                           Authentication authentication) {
        return knowledge.get(ApiSupport.actor(authentication), workspaceId, documentId);
    }

    // 详情路径显式指定版本，避免把草稿或旧版误当成当前指针。
    @GetMapping("/{documentId}/versions/{assetVersion}")
    KnowledgeDocument getVersion(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                 @PathVariable int assetVersion, Authentication authentication) {
        return knowledge.getVersion(ApiSupport.actor(authentication), workspaceId, documentId, assetVersion);
    }

    // 切块是 knowledge 的受授权派生操作，控制器不自行读取或拆分原文。
    @PostMapping("/{documentId}/chunks")
    java.util.List<io.eaf.knowledge.api.KnowledgeChunk> chunk(@PathVariable UUID workspaceId,
                                                               @PathVariable UUID documentId,
                                                               @RequestParam(defaultValue = "1") int assetVersion,
                                                               @RequestParam(defaultValue = "p3-plain-1") String chunkingVersion,
                                                               Authentication authentication) {
        return knowledge.chunk(ApiSupport.actor(authentication), workspaceId, documentId, assetVersion, chunkingVersion);
    }

    // 索引接口只返回作业状态；模型调用、批次校验和向量落库由 knowledge/model 边界处理。
    @PostMapping("/{documentId}/index-builds")
    ResponseEntity<KnowledgeIndexBuild> buildIndex(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                                    @RequestParam(defaultValue = "1") int assetVersion,
                                                    @RequestParam(defaultValue = "p3-plain-1") String chunkingVersion,
                                                    @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                                    Authentication authentication) {
        return ResponseEntity.ok(knowledge.buildIndex(ApiSupport.actor(authentication), workspaceId, documentId,
                assetVersion, chunkingVersion, idempotencyKey));
    }

    @GetMapping("/{documentId}/index-builds/{buildId}")
    KnowledgeIndexBuild getIndexBuild(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                      @PathVariable UUID buildId, Authentication authentication) {
        return knowledge.getIndexBuild(ApiSupport.actor(authentication), workspaceId, documentId, buildId);
    }

    @PostMapping("/{documentId}/index-builds/{buildId}/retry")
    KnowledgeIndexBuild retryIndex(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                   @PathVariable UUID buildId, Authentication authentication) {
        return knowledge.retryIndex(ApiSupport.actor(authentication), workspaceId, documentId, buildId);
    }

    // 发布和撤回共享 expected row version；策略由 knowledge:publish 和文档级授权共同决定。
    @PostMapping("/{documentId}/publish")
    KnowledgePublication publish(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                  @RequestParam long expectedVersion, @RequestParam(required = false) Integer baseVersion,
                                  @RequestParam UUID buildId,
                                  @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                  Authentication authentication) {
        return knowledge.publish(ApiSupport.actor(authentication), workspaceId, documentId, expectedVersion,
                baseVersion, buildId, idempotencyKey);
    }

    @PostMapping("/{documentId}/revoke")
    KnowledgePublication revoke(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                @RequestParam long expectedVersion,
                                @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                Authentication authentication) {
        return knowledge.revoke(ApiSupport.actor(authentication), workspaceId, documentId, expectedVersion, idempotencyKey);
    }

    // 版本撤回应精确指出目标；撤回非当前版本不移动当前发布指针。
    @PostMapping("/{documentId}/versions/{assetVersion}/revoke")
    KnowledgePublication revokeVersion(@PathVariable UUID workspaceId, @PathVariable UUID documentId,
                                       @PathVariable int assetVersion, @RequestParam long expectedVersion,
                                       @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                       Authentication authentication) {
        return knowledge.revokeVersion(ApiSupport.actor(authentication), workspaceId, documentId, assetVersion,
                expectedVersion, idempotencyKey);
    }

    private static long longValue(Object value, String name) {
        if (!(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof java.math.BigInteger)) throw io.eaf.shared.EafException.invalid(name + " 必须是整数。");
        try {
            if (value instanceof java.math.BigInteger big) return big.longValueExact();
            return Long.parseLong(value.toString());
        } catch (ArithmeticException | NumberFormatException malformed) {
            throw io.eaf.shared.EafException.invalid(name + " 必须是有效整数。");
        }
    }

    private static String single(MultiValueMap<String, String> query, String name) {
        var values = query.get(name);
        if (values == null) return null;
        if (values.size() != 1) throw io.eaf.shared.EafException.invalid(name + " 只能提供一次。");
        return values.getFirst();
    }

    private Map<String, String> stringMetadata(Map<String, Object> metadata) {
        if (metadata == null) return Map.of();
        var result = new java.util.LinkedHashMap<String, String>();
        metadata.forEach((key, value) -> {
            if (!(value instanceof String string)) throw io.eaf.shared.EafException.invalid("metadata 只能包含字符串值。");
            result.put(key, string);
        });
        return result;
    }

    record CreateBody(String title, String sourceRef, String content, Map<String, Object> metadata) { }
    record VersionBody(long expectedRowVersion, String content) { }
}
// 本控制器只做 REST 适配；权限、owner 注入和幂等规则仍由 knowledge 模块决定。
