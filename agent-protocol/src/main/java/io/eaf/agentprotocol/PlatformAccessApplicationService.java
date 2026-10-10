package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.context.api.ContextQuery;
import io.eaf.context.api.ContextService;
import io.eaf.context.api.EnterpriseContext;
import io.eaf.identity.api.IdentityService;
import io.eaf.identity.api.McpReadonlyDelegationScope;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.skill.api.SkillDefinition;
import io.eaf.skill.api.SkillService;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** 面向使用方的轻量投影；访问权和资产状态仍由 Skill、Capability、Context Owner 决定。 */
@Component
public final class PlatformAccessApplicationService {
    private static final int DEFAULT_LIMIT = 20;
    private static final int MAX_LIMIT = 50;
    private static final int MAX_QUERY_LENGTH = 200;
    private static final int DEFAULT_CONTEXT_TOP_K = 5;
    private static final int DEFAULT_CONTEXT_TOKEN_BUDGET = 2_000;

    private final SkillService skills;
    private final CapabilityService capabilities;
    private final ContextService contexts;
    private final ObjectMapper json;
    private final IdentityService identities;

    public PlatformAccessApplicationService(SkillService skills, CapabilityService capabilities,
                                            ContextService contexts, ObjectMapper json, IdentityService identities) {
        this.skills = skills;
        this.capabilities = capabilities;
        this.contexts = contexts;
        this.json = json;
        this.identities = identities;
    }

    public DiscoveryPage search(ActorContext actor, UUID workspaceId, DiscoveryRequest request) {
        if (request == null || request.kind() == null) throw EafException.invalid("kind 必须是 SKILL 或 CAPABILITY。");
        var kind = switch (request.kind()) {
            case "SKILL" -> AssetKind.SKILL;
            case "CAPABILITY" -> AssetKind.CAPABILITY;
            default -> throw EafException.invalid("kind 必须是 SKILL 或 CAPABILITY。");
        };
        var rawQuery = request.query() == null ? "" : request.query();
        if (rawQuery.length() > MAX_QUERY_LENGTH) throw EafException.invalid("query 最多 200 个字符。");
        var query = rawQuery.trim();
        var delegatedScope = mcpReadonlyScope(actor, workspaceId);
        var limit = request.limit() == null ? DEFAULT_LIMIT : request.limit();
        var offset = request.offset() == null ? 0 : request.offset();
        if (limit < 1 || limit > MAX_LIMIT) throw EafException.invalid("limit 必须在 1-50 之间。");
        if (offset < 0) throw EafException.invalid("offset 必须是非负整数。");

        // 先经 Owner API 授权并只取选中的资产类型，再从发布结果构造目录，避免摘要入口成为草稿读取旁路。
        var allowedCapability = delegatedScope == null ? null : requireBoundCapability(actor, workspaceId, delegatedScope);
        var items = kind == AssetKind.SKILL
                ? skills.list(actor, workspaceId).stream().filter(asset -> "PUBLISHED".equals(asset.status()))
                        .filter(asset -> allowedCapability == null || allowedCapability.skillId().equals(asset.id())
                                && allowedCapability.skillVersion().equals(asset.version()))
                        .filter(asset -> matches(asset.name(), asset.description(), query))
                        .map(PlatformAccessApplicationService::summary).toList()
                : capabilities.list(actor, workspaceId).stream().filter(asset -> "PUBLISHED".equals(asset.status()))
                        .filter(asset -> delegatedScope == null || delegatedScope.capabilityId().equals(asset.id())
                                && delegatedScope.capabilityVersion().equals(asset.version()))
                        .filter(asset -> matches(asset.name(), asset.description(), query))
                        .map(PlatformAccessApplicationService::summary).toList();
        var ordered = items.stream().sorted(Comparator.comparing(DiscoveryItem::name)
                .thenComparing(item -> item.id().toString()).thenComparing(DiscoveryItem::version)).toList();
        var start = Math.min((long) offset, ordered.size());
        var end = Math.min(start + limit, ordered.size());
        var page = ordered.subList((int) start, (int) end);
        Integer nextOffset = end < ordered.size() ? (int) end : null;
        return new DiscoveryPage(page, nextOffset);
    }

    public SkillUsageDetail getSkill(ActorContext actor, UUID workspaceId, UUID skillId, String version) {
        var scope = mcpReadonlyScope(actor, workspaceId);
        if (scope != null) {
            var capability = requireBoundCapability(actor, workspaceId, scope);
            if (!capability.skillId().equals(skillId) || !capability.skillVersion().equals(version))
                throw EafException.notFound();
        }
        var skill = skills.get(actor, workspaceId, skillId, version);
        // get 允许 Owner 查看草稿用于管理；使用方投影必须对 Owner 同样隐藏非发布版本。
        if (!"PUBLISHED".equals(skill.status())) throw EafException.notFound();
        return new SkillUsageDetail("SKILL", skill.id(), skill.name(), skill.description(), skill.version(),
                skill.contentHash(), schema(skill.inputSchema()), schema(skill.outputSchema()),
                skill.toolDependencies() == null ? List.of() : skill.toolDependencies().stream()
                        .map(dependency -> new ToolVersion(dependency.name(), dependency.version())).toList(),
                List.of("用途见 description；inputSchema 和 outputSchema 描述技能数据约定。",
                        "需要由 EAF 执行任务时，请选择已发布的 Capability 并调用 eaf.tasks.create。",
                        "依赖列表只描述声明的工具名称和版本，不授予工具权限；执行时仍重新检查权限与依赖。"));
    }

    public CapabilityUsageDetail getCapability(ActorContext actor, UUID workspaceId, UUID capabilityId, String version) {
        var scope = mcpReadonlyScope(actor, workspaceId);
        if (scope != null && (!scope.capabilityId().equals(capabilityId)
                || !scope.capabilityVersion().equals(version))) throw EafException.notFound();
        var capability = scope == null ? capabilities.get(actor, workspaceId, capabilityId, version)
                : requireBoundCapability(actor, workspaceId, scope);
        // 不使用 requirePublished：详情不扩展执行资格检查，但发布状态必须当前有效。
        if (!"PUBLISHED".equals(capability.status())) throw EafException.notFound();
        return new CapabilityUsageDetail("CAPABILITY", capability.id(), capability.name(), capability.description(),
                capability.version(), capability.contentHash(),
                new SkillVersionReference(capability.skillId(), capability.skillVersion(), capability.skillContentHash()),
                new TaskInput("string", 8_000),
                List.of("Task input 是最多 8,000 字符的字符串；Skill Schema 是资产元数据，不是 Task 请求外壳。",
                        "创建任务使用已选定的 capabilityId 与 capabilityVersion，并提供稳定的 idempotencyKey；重试沿用原键。",
                        "仅在能力要求绑定业务实体时提供 businessEntity。会话、经验整理、跟进确认和结果同步继续使用各自专用入口。",
                        "发现和读取不保证当前可执行；创建 Task 时会重新检查能力版本、依赖、权限和业务来源。"));
    }

    public EnterpriseContext queryContext(ActorContext actor, UUID workspaceId, ContextRequest request) {
        if (request == null || request.query() == null || request.query().isBlank())
            throw EafException.invalid("query 不能为空。");
        var topK = request.topK() == null ? DEFAULT_CONTEXT_TOP_K : request.topK();
        var tokenBudget = request.tokenBudget() == null ? DEFAULT_CONTEXT_TOKEN_BUDGET : request.tokenBudget();
        if (topK < 1 || topK > 10) throw EafException.invalid("topK 必须在 1-10 之间。");
        if (tokenBudget < 1 || tokenBudget > DEFAULT_CONTEXT_TOKEN_BUDGET)
            throw EafException.invalid("tokenBudget 必须在 1-2000 之间。");
        return contexts.query(actor, workspaceId, new ContextQuery(request.query(), topK, tokenBudget));
    }

    private McpReadonlyDelegationScope mcpReadonlyScope(ActorContext actor, UUID workspaceId) {
        if (actor == null || !actor.delegated()) return null;
        if (!IdentityService.MCP_AUDIENCE.equals(actor.delegationAudience()))
            throw EafException.forbidden("MCP 不接受 REST audience 委托。");
        return identities.mcpReadonlyScope(actor).filter(scope -> scope.workspaceId().equals(workspaceId))
                .orElseThrow(() -> EafException.forbidden("MCP 只读委托已失效或 Workspace 不匹配。"));
    }

    private CapabilityDefinition requireBoundCapability(ActorContext actor, UUID workspaceId,
                                                         McpReadonlyDelegationScope scope) {
        var capability = capabilities.requirePublished(actor, workspaceId, scope.capabilityId(), scope.capabilityVersion());
        if (!scope.capabilityHash().equals(capability.contentHash()))
            throw EafException.notFound();
        return capability;
    }

    static ContextRequest parseContextRequest(Map<String, Object> rawBody) {
        if (rawBody == null) throw EafException.invalid("context 请求体不能为空。");
        if (rawBody.keySet().stream().anyMatch(key -> !Set.of("query", "topK", "tokenBudget").contains(key)))
            throw EafException.invalid("context 请求包含未允许字段。");
        var query = rawBody.get("query");
        if (!(query instanceof String text)) throw EafException.invalid("query 必须是字符串。");
        return new ContextRequest(text, optionalInteger(rawBody, "topK"), optionalInteger(rawBody, "tokenBudget"));
    }

    static Integer optionalInteger(Map<String, Object> values, String name) {
        var raw = values.get(name);
        if (raw == null) return null;
        if (!(raw instanceof Byte || raw instanceof Short || raw instanceof Integer || raw instanceof Long
                || raw instanceof BigInteger)) throw EafException.invalid(name + " 必须是整数。");
        try {
            if (raw instanceof BigInteger big) return big.intValueExact();
            var value = Long.parseLong(raw.toString());
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) throw new ArithmeticException("overflow");
            return (int) value;
        } catch (ArithmeticException | NumberFormatException malformed) {
            throw EafException.invalid(name + " 必须是有效整数。");
        }
    }

    static Integer optionalQueryInteger(String raw, String name) {
        if (raw == null) return null;
        if (!raw.matches("[0-9]+")) throw EafException.invalid(name + " 必须是整数。");
        try { return Integer.valueOf(raw); }
        catch (NumberFormatException malformed) { throw EafException.invalid(name + " 必须是有效整数。"); }
    }

    private JsonNode schema(String serialized) {
        try { return json.readTree(serialized); }
        catch (IOException malformedStoredSchema) {
            // Owner 保存的 Schema 无效属于内部数据问题，不能静默删字段后当成正常详情返回。
            throw new IllegalStateException("已发布资产的 JSON Schema 无法解析。", malformedStoredSchema);
        }
    }

    private static boolean matches(String name, String description, String query) {
        return query.isEmpty() || contains(name, query) || contains(description, query);
    }

    private static boolean contains(String value, String query) {
        return value != null && value.toLowerCase(Locale.ROOT).contains(query.toLowerCase(Locale.ROOT));
    }

    private static DiscoveryItem summary(SkillDefinition skill) {
        return new DiscoveryItem("SKILL", skill.id(), skill.name(), skill.description(), skill.version());
    }

    private static DiscoveryItem summary(CapabilityDefinition capability) {
        return new DiscoveryItem("CAPABILITY", capability.id(), capability.name(), capability.description(), capability.version());
    }

    public enum AssetKind { SKILL, CAPABILITY }
    public record DiscoveryRequest(String kind, String query, Integer limit, Integer offset) { }
    public record DiscoveryItem(String kind, UUID id, String name, String description, String version) { }
    public record DiscoveryPage(List<DiscoveryItem> items, Integer nextOffset) { }
    public record ToolVersion(String name, String version) { }
    public record SkillUsageDetail(String kind, UUID id, String name, String description, String version,
                                   String contentHash, JsonNode inputSchema, JsonNode outputSchema,
                                   List<ToolVersion> toolDependencies, List<String> usageNotes) { }
    public record SkillVersionReference(UUID id, String version, String contentHash) { }
    public record TaskInput(String type, int maxLength) { }
    public record CapabilityUsageDetail(String kind, UUID id, String name, String description, String version,
                                        String contentHash, SkillVersionReference skill, TaskInput taskInput,
                                        List<String> usageNotes) { }
    public record ContextRequest(String query, Integer topK, Integer tokenBudget) { }
}
