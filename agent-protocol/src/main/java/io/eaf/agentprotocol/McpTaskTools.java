package io.eaf.agentprotocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.capability.api.CapabilityService;
import io.eaf.context.api.ScopedContextQueryService;
import io.eaf.workspace.api.WorkspaceCatalog;
import io.eaf.workspace.api.ContextSourceSelection;
import io.eaf.agentprotocol.PlatformAccessApplicationService.CapabilityUsageDetail;
import io.eaf.agentprotocol.PlatformAccessApplicationService.DiscoveryRequest;
import io.eaf.agentprotocol.PlatformAccessApplicationService.SkillUsageDetail;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** MCP 工具只做协议字段校验和错误映射，Task 业务仍由共享应用服务与领域 API 执行。 */
@Component
public class McpTaskTools {
    private static final String CAPABILITIES_LIST = "eaf.capabilities.list";
    private static final String TASKS_CREATE = "eaf.tasks.create";
    private static final String TASKS_GET = "eaf.tasks.get";
    private static final String TASKS_CANCEL = "eaf.tasks.cancel";
    private static final String CATALOG_SEARCH = "eaf.catalog.search";
    private static final String SKILLS_GET = "eaf.skills.get";
    private static final String CAPABILITIES_GET = "eaf.capabilities.get";
    private static final String CONTEXT_QUERY = "eaf.context.query";
    private static final String CONTEXT_SOURCES_LIST = "eaf.context.sources.list";
    private static final String CONTEXT_SCOPED_QUERY = "eaf.context.scoped-query";

    private final TaskApplicationService tasks;
    private final CapabilityService capabilities;
    private final PlatformAccessApplicationService access;
    private final ObjectMapper json;
    private final WorkspaceCatalog workspaces;
    private final ScopedContextQueryService scopedContexts;

    public McpTaskTools(TaskApplicationService tasks, CapabilityService capabilities,
                        PlatformAccessApplicationService access, ObjectMapper json,
                        WorkspaceCatalog workspaces, ScopedContextQueryService scopedContexts) {
        this.tasks = tasks;
        this.capabilities = capabilities;
        this.access = access;
        this.json = json;
        this.workspaces = workspaces;
        this.scopedContexts = scopedContexts;
    }

    List<SyncToolSpecification> specifications() {
        return List.of(
                specification(CAPABILITIES_LIST, "列出当前身份可读的已发布 Capability。",
                        input(Map.of("workspaceId", uuidSchema()), "workspaceId"), capabilityOutput(),
                        (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId"));
                            var actor = actor(exchange);
                            var items = capabilities.list(actor, uuid(args, "workspaceId")).stream()
                                    .map(capability -> new CapabilityView(capability.id(), capability.name(),
                                            capability.description(), capability.version())).toList();
                            return new CapabilityList(items);
                        })),
                specification(TASKS_CREATE, "按已发布 Capability 创建异步 Task；调用方使用稳定幂等键重试。",
                        input(Map.of("workspaceId", uuidSchema(), "capabilityId", uuidSchema(),
                                "capabilityVersion", stringSchema(), "input", stringSchema(),
                                "idempotencyKey", stringSchema(), "businessEntity", businessEntitySchema()),
                                "workspaceId", "capabilityId", "capabilityVersion", "input", "idempotencyKey"),
                        taskOutput(), (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId", "capabilityId", "capabilityVersion",
                                    "input", "idempotencyKey", "businessEntity"));
                            var entity = businessEntity(args.get("businessEntity"));
                            return tasks.create(actor(exchange), uuid(args, "workspaceId"),
                                    new TaskApplicationService.CreateRequest(null, null,
                                            uuid(args, "capabilityId"), string(args, "capabilityVersion"),
                                            string(args, "input"), entity), string(args, "idempotencyKey"), null, "MCP");
                        })),
                specification(TASKS_GET, "读取当前身份可见的 Task 及安全投影。",
                        input(Map.of("workspaceId", uuidSchema(), "taskId", uuidSchema()), "workspaceId", "taskId"),
                        taskOutput(), (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId", "taskId"));
                            return tasks.get(actor(exchange), uuid(args, "workspaceId"), uuid(args, "taskId"));
                        })),
                specification(TASKS_CANCEL, "按当前版本取消尚可取消的 Task。",
                        input(Map.of("workspaceId", uuidSchema(), "taskId", uuidSchema(),
                                "expectedVersion", Map.of("type", "integer", "minimum", 1)),
                                "workspaceId", "taskId", "expectedVersion"), taskOutput(),
                        (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId", "taskId", "expectedVersion"));
                            return tasks.cancel(actor(exchange), uuid(args, "workspaceId"), uuid(args, "taskId"),
                                    integer(args, "expectedVersion"));
                        })),
                specification(CATALOG_SEARCH, "按类别和关键词发现当前身份可读取的已发布 Skill 或 Capability 摘要。",
                        catalogInput(), catalogOutput(), (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId", "kind", "query", "limit", "offset"));
                            return access.search(actor(exchange), uuid(args, "workspaceId"),
                                    new DiscoveryRequest(string(args, "kind"), optionalString(args, "query"),
                                            PlatformAccessApplicationService.optionalInteger(args, "limit"),
                                            PlatformAccessApplicationService.optionalInteger(args, "offset")));
                        })),
                specification(SKILLS_GET, "读取指定已发布 Skill 版本的使用说明与数据 Schema，不返回内部 Prompt。",
                        input(Map.of("workspaceId", uuidSchema(), "skillId", uuidSchema(), "version", stringSchema()),
                                "workspaceId", "skillId", "version"), skillOutput(), (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId", "skillId", "version"));
                            SkillUsageDetail detail = access.getSkill(actor(exchange), uuid(args, "workspaceId"),
                                    uuid(args, "skillId"), string(args, "version"));
                            return detail;
                        })),
                specification(CAPABILITIES_GET, "读取指定已发布 Capability 版本及 Task 调用合同，不返回内部 Prompt 或模型配置。",
                        input(Map.of("workspaceId", uuidSchema(), "capabilityId", uuidSchema(), "version", stringSchema()),
                                "workspaceId", "capabilityId", "version"), capabilityDetailOutput(),
                        (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId", "capabilityId", "version"));
                            CapabilityUsageDetail detail = access.getCapability(actor(exchange), uuid(args, "workspaceId"),
                                    uuid(args, "capabilityId"), string(args, "version"));
                            return detail;
                        })),
                specification(CONTEXT_QUERY, "按当前身份查询授权上下文和来源；不生成回答，可能调用既有 Embedding 检索路径。",
                        input(Map.of("workspaceId", uuidSchema(), "query", stringSchema(),
                                "topK", Map.of("type", List.of("integer", "null"), "minimum", 1, "maximum", 10),
                                "tokenBudget", Map.of("type", List.of("integer", "null"), "minimum", 1, "maximum", 2_000)),
                                "workspaceId", "query"), contextOutput(), (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId", "query", "topK", "tokenBudget"));
                            var publicBody = new java.util.HashMap<String, Object>(args);
                            publicBody.remove("workspaceId");
                            return access.queryContext(actor(exchange), uuid(args, "workspaceId"),
                                    PlatformAccessApplicationService.parseContextRequest(publicBody));
                        })),
                specification(CONTEXT_SOURCES_LIST, "列出当前 HUMAN 身份有权选择的上下文来源及本人默认选择。",
                        input(Map.of("workspaceId", uuidSchema(), "limit", nullableIntegerSchema(),
                                "offset", nullableIntegerSchema()), "workspaceId"), sourceSelectionOutput(),
                        (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId", "limit", "offset"));
                            var limit = PlatformAccessApplicationService.optionalInteger(args, "limit");
                            var offset = PlatformAccessApplicationService.optionalInteger(args, "offset");
                            return workspaces.listContextSources(actor(exchange), uuid(args, "workspaceId"),
                                    limit == null ? 20 : limit, offset == null ? 0 : offset);
                        })),
                specification(CONTEXT_SCOPED_QUERY, "按明确来源选择执行有出处、统一预算的多来源上下文读取；不生成回答。",
                        input(Map.of("workspaceId", uuidSchema(), "query", stringSchema(),
                                "topK", nullableIntegerSchema(), "tokenBudget", nullableIntegerSchema(),
                                "sourceWorkspaceIds", Map.of("type", List.of("array", "null"), "maxItems", 3,
                                        "items", uuidSchema())), "workspaceId", "query"), scopedContextOutput(),
                        (exchange, request) -> execute(() -> {
                            var args = arguments(request, Set.of("workspaceId", "query", "topK", "tokenBudget", "sourceWorkspaceIds"));
                            var sourceIds = optionalUuidList(args.get("sourceWorkspaceIds"));
                            return scopedContexts.query(actor(exchange), uuid(args, "workspaceId"), string(args, "query"),
                                    PlatformAccessApplicationService.optionalInteger(args, "topK"),
                                    PlatformAccessApplicationService.optionalInteger(args, "tokenBudget"), sourceIds);
                        })));
    }

    private static SyncToolSpecification specification(String name, String description,
            Map<String, Object> inputSchema, Map<String, Object> outputSchema,
            java.util.function.BiFunction<McpSyncServerExchange, CallToolRequest, CallToolResult> handler) {
        return SyncToolSpecification.builder()
                .tool(Tool.builder(name, inputSchema).description(description).outputSchema(outputSchema).build())
                .callHandler(handler).build();
    }

    private CallToolResult execute(java.util.function.Supplier<?> operation) {
        try {
            var result = operation.get();
            var text = json.writeValueAsString(result);
            // 结构化结果复用 JSON 投影，Instant 等领域类型先按应用序列化规则转为协议值再交给 SDK 校验。
            var structured = json.readValue(text, new TypeReference<Map<String, Object>>() { });
            return CallToolResult.builder().addContent(new TextContent(text)).structuredContent(structured).build();
        } catch (EafException e) {
            return CallToolResult.builder().addTextContent(errorJson(e.code(), e.getMessage(), e.retryable())).isError(true).build();
        } catch (JsonProcessingException e) {
            return CallToolResult.builder().addTextContent(errorJson("INTERNAL_ERROR", "服务未能完成请求。"))
                    .isError(true).build();
        }
    }

    private String errorJson(String code, String message) { return errorJson(code, message, false); }

    private String errorJson(String code, String message, boolean retryable) {
        try { return json.writeValueAsString(Map.of("code", code, "message", message, "retryable", retryable)); }
        catch (JsonProcessingException impossible) { return "{\"code\":\"INTERNAL_ERROR\",\"message\":\"服务未能完成请求。\"}"; }
    }

    private static ActorContext actor(McpSyncServerExchange exchange) {
        Object actor = exchange.transportContext().get(McpTransportConfiguration.ACTOR_CONTEXT_KEY);
        if (!(actor instanceof ActorContext context)) throw EafException.unauthenticated();
        //  的委托 audience 固定为 eaf:rest；MCP 不复用该受托身份跨协议授权。
        if (context.delegated()) throw EafException.forbidden("当前 MCP 入口不接受绑定 REST audience 的委托身份。");
        return context;
    }

    private static Map<String, Object> arguments(CallToolRequest request, Set<String> allowed) {
        var args = request.arguments();
        if (args == null || args.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("MCP 工具参数包含未允许字段。");
        return args;
    }

    private static UUID uuid(Map<String, Object> args, String name) {
        var value = string(args, name);
        try { return UUID.fromString(value); }
        catch (IllegalArgumentException malformed) { throw EafException.invalid(name + " 必须是 UUID。"); }
    }

    private static String string(Map<String, Object> args, String name) {
        if (!(args.get(name) instanceof String value) || value.isBlank())
            throw EafException.invalid(name + " 不能为空。");
        if ("input".equals(name) && value.length() > 8_000) throw EafException.invalid("input 超过 8,000 字符限制。");
        if ("idempotencyKey".equals(name) && value.length() > 200) throw EafException.invalid("idempotencyKey 超过 200 字符限制。");
        return value;
    }

    private static String optionalString(Map<String, Object> args, String name) {
        var value = args.get(name);
        if (value == null) return null;
        if (!(value instanceof String text)) throw EafException.invalid(name + " 必须是字符串。");
        return text;
    }

    private static long integer(Map<String, Object> args, String name) {
        if (!(args.get(name) instanceof Number number)) throw EafException.invalid(name + " 必须是整数。");
        try { return Long.parseLong(number.toString()); }
        catch (NumberFormatException malformed) { throw EafException.invalid(name + " 必须是整数。"); }
    }

    private static TaskApplicationService.BusinessEntity businessEntity(Object value) {
        if (value == null) return null;
        if (!(value instanceof Map<?, ?> body)
                || body.keySet().stream().anyMatch(key -> !Set.of("type", "id").contains(key)))
            throw EafException.invalid("businessEntity 字段无效。");
        var type = body.get("type");
        var id = body.get("id");
        if (!(type instanceof String entityType) || !(id instanceof String entityId))
            throw EafException.invalid("businessEntity.type 与 businessEntity.id 必须是字符串。");
        return new TaskApplicationService.BusinessEntity(entityType, entityId);
    }

    private static Map<String, Object> input(Map<String, Object> properties, String... required) {
        return Map.of("type", "object", "properties", properties, "required", List.of(required),
                "additionalProperties", false);
    }

    private static Map<String, Object> stringSchema() { return Map.of("type", "string"); }
    private static Map<String, Object> uuidSchema() { return Map.of("type", "string", "format", "uuid"); }

    private static Map<String, Object> businessEntitySchema() {
        return input(Map.of("type", stringSchema(), "id", stringSchema()), "type", "id");
    }

    private static Map<String, Object> capabilityOutput() {
        var item = Map.of("type", "object", "properties", Map.of("id", uuidSchema(), "name", stringSchema(),
                "description", Map.of("type", List.of("string", "null")), "version", stringSchema()),
                "required", List.of("id", "name", "version"));
        return Map.of("type", "object", "properties", Map.of("items", Map.of("type", "array", "items", item)),
                "required", List.of("items"));
    }

    private static Map<String, Object> catalogInput() {
        var properties = new java.util.HashMap<String, Object>();
        properties.put("workspaceId", uuidSchema());
        properties.put("kind", Map.of("type", "string", "enum", List.of("SKILL", "CAPABILITY")));
        properties.put("query", Map.of("type", "string", "maxLength", 200));
        properties.put("limit", Map.of("type", "integer", "minimum", 1, "maximum", 50));
        properties.put("offset", Map.of("type", "integer", "minimum", 0, "maximum", Integer.MAX_VALUE));
        return input(properties, "workspaceId", "kind");
    }

    private static Map<String, Object> catalogOutput() {
        var item = objectSchema(Map.of("kind", stringSchema(), "id", uuidSchema(), "name", stringSchema(),
                "description", nullableStringSchema(), "version", stringSchema()),
                "kind", "id", "name", "description", "version");
        return objectSchema(Map.of("items", Map.of("type", "array", "items", item),
                "nextOffset", nullableIntegerSchema()), "items", "nextOffset");
    }

    private static Map<String, Object> skillOutput() {
        var dependency = objectSchema(Map.of("name", stringSchema(), "version", stringSchema()), "name", "version");
        return objectSchema(Map.of("kind", stringSchema(), "id", uuidSchema(), "name", stringSchema(),
                "description", nullableStringSchema(), "version", stringSchema(), "contentHash", stringSchema(),
                "inputSchema", Map.of("type", "object"), "outputSchema", Map.of("type", "object"),
                "toolDependencies", Map.of("type", "array", "items", dependency),
                "usageNotes", stringArraySchema()), "kind", "id", "name", "description", "version", "contentHash",
                "inputSchema", "outputSchema", "toolDependencies", "usageNotes");
    }

    private static Map<String, Object> capabilityDetailOutput() {
        var skill = objectSchema(Map.of("id", uuidSchema(), "version", stringSchema(), "contentHash", nullableStringSchema()),
                "id", "version", "contentHash");
        var taskInput = objectSchema(Map.of("type", stringSchema(), "maxLength", Map.of("type", "integer")),
                "type", "maxLength");
        return objectSchema(Map.of("kind", stringSchema(), "id", uuidSchema(), "name", stringSchema(),
                "description", nullableStringSchema(), "version", stringSchema(), "contentHash", stringSchema(),
                "skill", skill, "taskInput", taskInput, "usageNotes", stringArraySchema()),
                "kind", "id", "name", "description", "version", "contentHash", "skill", "taskInput", "usageNotes");
    }

    private static Map<String, Object> contextOutput() {
        return objectSchema(Map.of("status", stringSchema(), "topK", Map.of("type", "integer"),
                "tokenBudget", Map.of("type", "integer"), "usedTokens", Map.of("type", "integer"),
                "omittedCount", Map.of("type", "integer"), "truncationReason", nullableStringSchema(),
                "items", Map.of("type", "array", "items", Map.of("type", "object")),
                "experienceUsage", Map.of("type", List.of("object", "null")),
                "teamExperienceUsage", Map.of("type", List.of("object", "null"))),
                "status", "topK", "tokenBudget", "usedTokens", "omittedCount", "truncationReason", "items",
                "experienceUsage", "teamExperienceUsage");
    }

    private static Map<String, Object> objectSchema(Map<String, Object> properties, String... required) {
        return Map.of("type", "object", "properties", properties, "required", List.of(required),
                "additionalProperties", false);
    }

    private static Map<String, Object> nullableStringSchema() {
        return Map.of("type", List.of("string", "null"));
    }

    private static Map<String, Object> nullableIntegerSchema() {
        return Map.of("type", List.of("integer", "null"));
    }

    private static List<UUID> optionalUuidList(Object value) {
        if (value == null) return null;
        if (!(value instanceof List<?> values) || values.size() > 3)
            throw EafException.invalid("sourceWorkspaceIds 必须是最多三个 UUID 的数组。");
        var result = new java.util.ArrayList<UUID>();
        for (var item : values) {
            if (!(item instanceof String text)) throw EafException.invalid("sourceWorkspaceIds 只能包含 UUID 字符串。");
            try { result.add(UUID.fromString(text)); }
            catch (IllegalArgumentException malformed) { throw EafException.invalid("sourceWorkspaceIds 只能包含 UUID 字符串。"); }
        }
        return List.copyOf(result);
    }

    private static Map<String, Object> sourceSelectionOutput() {
        return objectSchema(Map.of("sources", Map.of("type", "array", "items", Map.of("type", "object")),
                "selectedSourceWorkspaceIds", Map.of("type", "array", "items", uuidSchema()),
                "unavailableSourceCount", Map.of("type", "integer"), "nextOffset", nullableIntegerSchema()),
                "sources", "selectedSourceWorkspaceIds", "unavailableSourceCount", "nextOffset");
    }

    private static Map<String, Object> scopedContextOutput() {
        return objectSchema(Map.of("status", stringSchema(), "topK", Map.of("type", "integer"),
                "tokenBudget", Map.of("type", "integer"), "usedTokens", Map.of("type", "integer"),
                "items", Map.of("type", "array", "items", Map.of("type", "object")),
                "sources", Map.of("type", "array", "items", Map.of("type", "object")),
                "omitted", Map.of("type", "object"), "unavailableSourceCount", Map.of("type", "integer")),
                "status", "topK", "tokenBudget", "usedTokens", "items", "sources", "omitted", "unavailableSourceCount");
    }

    private static Map<String, Object> stringArraySchema() {
        return Map.of("type", "array", "items", stringSchema());
    }

    private static Map<String, Object> taskOutput() {
        return Map.of("type", "object", "properties", Map.of("id", uuidSchema(), "status", stringSchema(),
                "attempt", Map.of("type", "integer"), "version", Map.of("type", "integer"),
                "result", Map.of(), "errorCode", Map.of("type", List.of("string", "null")),
                "errorDetail", Map.of("type", List.of("string", "null")), "entryProtocol", stringSchema()),
                "required", List.of("id", "status", "attempt", "version", "entryProtocol"));
    }

    private record CapabilityList(List<CapabilityView> items) { }
    private record CapabilityView(UUID id, String name, String description, String version) { }
}
