package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.knowledge.api.KnowledgeSearchQuery;
import io.eaf.knowledge.api.KnowledgeSearchResult;
import io.eaf.knowledge.api.KnowledgeService;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.security.core.Authentication;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/knowledge")
public class KnowledgeSearchController {
    private final KnowledgeService knowledge;
    private final ObjectMapper mapper;

    public KnowledgeSearchController(KnowledgeService knowledge, ObjectMapper mapper) {
        this.knowledge = knowledge;
        this.mapper = mapper;
    }

    // 检索请求只允许问题和小范围 topK；租户、权限和正式状态由 knowledge 服务端从认证上下文决定。
    @PostMapping("/search")
    KnowledgeSearchResult search(@PathVariable UUID workspaceId, @RequestBody Map<String, Object> rawBody,
                                 Authentication authentication) {
        if (rawBody == null) throw io.eaf.shared.EafException.invalid("检索请求体不能为空。");
        var unknown = rawBody.keySet().stream().filter(key -> !Set.of("query", "topK", "mode").contains(key)).findFirst().orElse(null);
        if (unknown != null) throw io.eaf.shared.EafException.invalid("检索请求包含未允许字段。");
        var body = mapper.convertValue(rawBody, KnowledgeSearchQuery.class);
        var mode = rawBody.get("mode") == null ? "VECTOR" : rawBody.get("mode").toString();
        return knowledge.search(ApiSupport.actor(authentication), workspaceId, body.query(), body.topK() == null ? 5 : body.topK(), null, mode);
    }
}
// 控制器只做 JSON 适配，不直接连接 pgvector 或处理命中权限。
