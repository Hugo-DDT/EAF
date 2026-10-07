package io.eaf.agentprotocol;

import io.eaf.context.api.EnterpriseContext;
import io.eaf.context.api.ScopedContextQueryService;
import io.eaf.context.api.ScopedContextResult;
import io.eaf.workspace.api.ContextSourceSelection;
import io.eaf.workspace.api.WorkspaceCatalog;
import io.eaf.shared.EafException;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.util.MultiValueMap;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/context")
public class ContextController {
    private final PlatformAccessApplicationService access;
    private final WorkspaceCatalog workspaces;
    private final ScopedContextQueryService scopedContexts;

    public ContextController(PlatformAccessApplicationService access, WorkspaceCatalog workspaces,
                             ScopedContextQueryService scopedContexts) {
        this.access = access;
        this.workspaces = workspaces;
        this.scopedContexts = scopedContexts;
    }

    // REST 层只限制字段形状；片段授权、Embedding 和完整 Chunk 预算由 context/knowledge 负责。
    @PostMapping("/queries")
    EnterpriseContext query(@PathVariable UUID workspaceId, @RequestBody Map<String, Object> rawBody,
                             Authentication authentication) {
        var request = PlatformAccessApplicationService.parseContextRequest(rawBody);
        return access.queryContext(ApiSupport.actor(authentication), workspaceId, request);
    }

    @GetMapping("/sources")
    ContextSourceSelection sources(@PathVariable UUID workspaceId, @RequestParam MultiValueMap<String, String> query,
                                   Authentication authentication) {
        if (query.keySet().stream().anyMatch(key -> !Set.of("limit", "offset").contains(key)))
            throw EafException.invalid("来源列表查询包含未允许字段。");
        var limit = PlatformAccessApplicationService.optionalQueryInteger(single(query, "limit"), "limit");
        var offset = PlatformAccessApplicationService.optionalQueryInteger(single(query, "offset"), "offset");
        return workspaces.listContextSources(ApiSupport.actor(authentication), workspaceId,
                limit == null ? 20 : limit, offset == null ? 0 : offset);
    }

    @PutMapping("/sources")
    ContextSourceSelection replaceSources(@PathVariable UUID workspaceId, @RequestBody Map<String, Object> body,
                                          Authentication authentication) {
        if (body == null || body.keySet().stream().anyMatch(key -> !Set.of("sourceWorkspaceIds").contains(key))
                || !(body.get("sourceWorkspaceIds") instanceof List<?> values))
            throw EafException.invalid("来源偏好请求只能包含 sourceWorkspaceIds 数组。");
        return workspaces.replaceContextSources(ApiSupport.actor(authentication), workspaceId, uuidList(values));
    }

    @PostMapping("/scoped-queries")
    ScopedContextResult scopedQuery(@PathVariable UUID workspaceId, @RequestBody Map<String, Object> body,
                                    Authentication authentication) {
        var allowed = Set.of("query", "topK", "tokenBudget", "sourceWorkspaceIds");
        if (body == null || body.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("多来源上下文请求包含未允许字段。");
        if (!(body.get("query") instanceof String queryText)) throw EafException.invalid("query 必须是字符串。");
        var sourceIds = body.containsKey("sourceWorkspaceIds") && body.get("sourceWorkspaceIds") != null
                ? uuidList(body.get("sourceWorkspaceIds")) : null;
        return scopedContexts.query(ApiSupport.actor(authentication), workspaceId, queryText,
                PlatformAccessApplicationService.optionalInteger(body, "topK"),
                PlatformAccessApplicationService.optionalInteger(body, "tokenBudget"), sourceIds);
    }

    private static List<UUID> uuidList(Object value) {
        if (!(value instanceof List<?> values) || values.size() > 3) throw EafException.invalid("sourceWorkspaceIds 必须是最多三个 UUID 的数组。");
        var result = new java.util.ArrayList<UUID>();
        for (var item : values) {
            if (!(item instanceof String text)) throw EafException.invalid("sourceWorkspaceIds 只能包含 UUID 字符串。");
            try { result.add(UUID.fromString(text)); }
            catch (IllegalArgumentException malformed) { throw EafException.invalid("sourceWorkspaceIds 只能包含 UUID 字符串。"); }
        }
        return List.copyOf(result);
    }

    private static String single(MultiValueMap<String, String> query, String name) {
        var values = query.get(name);
        if (values == null) return null;
        if (values.size() != 1) throw EafException.invalid(name + " 只能提供一次。");
        return values.getFirst();
    }
}
// 控制器不缓存或拼接历史上下文，避免把旧授权结果误当作当前正式知识。
