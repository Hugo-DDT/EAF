package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.task.api.TaskPageCursor;
import io.eaf.task.api.TaskStatus;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.UUID;
import org.a2aproject.sdk.spec.A2AErrorCodes;
import org.a2aproject.sdk.spec.TaskState;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** A2A 1.0 JSON-RPC 入口；身份与业务操作仍交给认证过滤器和 TaskApplicationService。 */
@RestController
@ConditionalOnProperty(name = "eaf.agent-protocol.a2a-enabled", havingValue = "true", matchIfMissing = true)
public class A2aServerController {
    private static final int MAX_BODY_BYTES = 32_768;
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final String VERSION = "1.0";
    private final A2aProtocolMapping mapping;
    private final TaskApplicationService tasks;
    private final ObjectMapper json;

    public A2aServerController(A2aProtocolMapping mapping, TaskApplicationService tasks, ObjectMapper json) {
        this.mapping = mapping;
        this.tasks = tasks;
        this.json = json;
    }

    @GetMapping("/.well-known/agent-card.json")
    public JsonNode agentCard(@RequestParam(required = false) UUID workspaceId, Authentication authentication) {
        // 静态公开 Card 不包含 Workspace 能力；只有认证身份才可请求带能力的 Workspace Card。
        if (workspaceId != null && authentication != null
                && authentication.getPrincipal() instanceof ActorContext actor)
            return mapping.cardJson(actor, workspaceId);
        return mapping.publicCardJson();
    }

    @PostMapping(path = "/a2a/{workspaceId}", consumes = {"application/json", "application/a2a+json"},
            produces = {"application/json", "application/a2a+json"})
    public JsonNode jsonRpc(@PathVariable UUID workspaceId,
                            @RequestHeader(value = "A2A-Version", required = false) String version,
                            @RequestBody byte[] body, Authentication authentication) {
        JsonNode request;
        try {
            if (body == null || body.length > MAX_BODY_BYTES) return error(null, -32600, "A2A 请求体无效。");
            request = json.readTree(body);
        } catch (java.io.IOException malformed) {
            return error(null, A2AErrorCodes.JSON_PARSE.code(), "A2A JSON 无法解析。");
        }
        var id = request != null && request.isObject() ? request.get("id") : null;
        if (id != null && !id.isTextual() && !id.isNumber()) id = null;
        if (!validEnvelope(request)) return error(id, A2AErrorCodes.INVALID_REQUEST.code(), "JSON-RPC 请求无效。");
        if (!VERSION.equals(version))
            return error(id, A2AErrorCodes.VERSION_NOT_SUPPORTED.code(), "仅支持 A2A-Version 1.0；缺少版本头按 0.3 处理。");

        try {
            var actor = ApiSupport.actor(authentication);
            var method = request.path("method").asText();
            var result = dispatch(actor, workspaceId, method, request.path("params"));
            return success(id, result);
        } catch (EafException failure) {
            var method = request.path("method").asText();
            return error(id, failure, method);
        } catch (UnknownMethodException unknown) {
            return error(id, A2AErrorCodes.METHOD_NOT_FOUND.code(), "A2A 方法不受支持。");
        } catch (RuntimeException failure) {
            return error(id, A2AErrorCodes.INTERNAL.code(), "服务未能完成 A2A 请求。");
        }
    }

    private JsonNode dispatch(ActorContext actor, UUID workspaceId, String method, JsonNode params) {
        if (!params.isObject()) throw EafException.invalid("A2A params 必须是对象。");
        return switch (method) {
            case "SendMessage" -> sendMessage(actor, workspaceId, params);
            case "GetTask" -> getTask(actor, workspaceId, params);
            case "ListTasks" -> listTasks(actor, workspaceId, params);
            case "CancelTask" -> cancelTask(actor, workspaceId, params);
            default -> throw new UnknownMethodException();
        };
    }

    private JsonNode sendMessage(ActorContext actor, UUID workspaceId, JsonNode params) {
        requireOnly(params, Set.of("message", "metadata"));
        var message = mapping.parseUserMessage(params.path("message"));
        var metadata = params.path("metadata");
        if (!metadata.isObject() || !only(metadata, Set.of("skillId"))
                || !metadata.path("skillId").isTextual())
            throw EafException.invalid("SendMessage 必须提供唯一的 metadata.skillId。");
        var task = tasks.createForSkill(actor, workspaceId, metadata.path("skillId").asText(),
                message.text(), message.messageId());
        var result = json.createObjectNode();
        result.set("task", mapping.taskJson(task, true));
        return result;
    }

    private JsonNode getTask(ActorContext actor, UUID workspaceId, JsonNode params) {
        requireOnly(params, Set.of("id", "historyLength"));
        requireNoHistory(params);
        var task = tasks.get(actor, workspaceId, uuid(params.path("id"), "id"));
        return mapping.taskJson(task, true);
    }

    private JsonNode listTasks(ActorContext actor, UUID workspaceId, JsonNode params) {
        requireOnly(params, Set.of("contextId", "status", "pageSize", "pageToken", "historyLength",
                "statusTimestampAfter", "includeArtifacts"));
        requireNoHistory(params);
        var contextId = params.has("contextId") ? uuid(params.path("contextId"), "contextId") : null;
        var statuses = params.has("status") ? statuses(params.path("status")) : null;
        var updatedAfter = params.has("statusTimestampAfter")
                ? instant(params.path("statusTimestampAfter"), "statusTimestampAfter") : null;
        var pageSize = pageSize(params.path("pageSize"));
        var cursor = decodeCursor(params.path("pageToken"));
        var includeArtifacts = params.path("includeArtifacts").asBoolean(false);
        if (params.has("includeArtifacts") && !params.path("includeArtifacts").isBoolean())
            throw EafException.invalid("includeArtifacts 必须是布尔值。");

        var page = tasks.list(actor, workspaceId, contextId, statuses, updatedAfter, cursor, pageSize);
        var items = json.createArrayNode();
        page.items().forEach(task -> items.add(mapping.taskJson(task, includeArtifacts)));
        var result = json.createObjectNode();
        result.set("tasks", items);
        result.put("nextPageToken", encodeCursor(page.nextCursor()));
        result.put("pageSize", pageSize);
        result.put("totalSize", page.totalSize());
        return result;
    }

    private JsonNode cancelTask(ActorContext actor, UUID workspaceId, JsonNode params) {
        requireOnly(params, Set.of("id", "metadata"));
        if (params.has("metadata") && (!params.path("metadata").isObject() || !params.path("metadata").isEmpty()))
            throw EafException.invalid("CancelTask 不接受额外 metadata。");
        return mapping.taskJson(tasks.cancel(actor, workspaceId, uuid(params.path("id"), "id")), true);
    }

    private Set<TaskStatus> statuses(JsonNode value) {
        if (!value.isTextual()) throw EafException.invalid("status 必须是 A2A TaskState 字符串。");
        TaskState state;
        try { state = TaskState.valueOf(value.asText()); }
        catch (IllegalArgumentException unknown) { throw EafException.invalid("status 不是受支持的 A2A TaskState。"); }
        return switch (state) {
            case TASK_STATE_SUBMITTED -> Set.of(TaskStatus.QUEUED);
            // 两种等待在 A2A 中都属于 WORKING，具体授权/等待原因仍只由 EAF Task API 判断。
            case TASK_STATE_WORKING -> Set.of(TaskStatus.RUNNING, TaskStatus.WAITING_VERIFICATION, TaskStatus.WAITING_REMOTE);
            case TASK_STATE_INPUT_REQUIRED -> Set.of(TaskStatus.WAITING_APPROVAL);
            case TASK_STATE_COMPLETED -> Set.of(TaskStatus.SUCCEEDED);
            case TASK_STATE_FAILED -> Set.of(TaskStatus.FAILED, TaskStatus.TIMED_OUT);
            case TASK_STATE_CANCELED -> Set.of(TaskStatus.CANCELLED);
            case TASK_STATE_AUTH_REQUIRED, TASK_STATE_REJECTED, TASK_STATE_UNSPECIFIED -> Set.of();
        };
    }

    private TaskPageCursor decodeCursor(JsonNode token) {
        if (token.isMissingNode() || token.asText().isEmpty()) return null;
        if (!token.isTextual() || token.asText().length() > 256)
            throw EafException.invalid("pageToken 无效。");
        try {
            var decoded = new String(Base64.getUrlDecoder().decode(token.asText()), StandardCharsets.UTF_8);
            var fields = decoded.split("\\n", -1);
            if (fields.length != 2) throw new IllegalArgumentException();
            return new TaskPageCursor(Instant.parse(fields[0]), UUID.fromString(fields[1]));
        } catch (IllegalArgumentException malformed) {
            throw EafException.invalid("pageToken 无效。");
        }
    }

    private String encodeCursor(TaskPageCursor cursor) {
        if (cursor == null) return "";
        var value = cursor.updatedAt() + "\n" + cursor.taskId();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private int pageSize(JsonNode value) {
        if (value.isMissingNode()) return DEFAULT_PAGE_SIZE;
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.asInt() < 1 || value.asInt() > 100)
            throw EafException.invalid("pageSize 必须在 1 到 100 之间。");
        return value.asInt();
    }

    private void requireNoHistory(JsonNode params) {
        if (!params.has("historyLength")) return;
        var history = params.path("historyLength");
        if (!history.isIntegralNumber() || !history.canConvertToInt() || history.asInt() < 0 || history.asInt() > 100)
            throw EafException.invalid("historyLength 必须是 0 到 100 的整数。");
        // Task 域尚未持久化 A2A Message 历史；请求有限历史时以空 history 满足上限。
    }

    private Instant instant(JsonNode value, String field) {
        if (!value.isTextual()) throw EafException.invalid(field + " 必须是 ISO-8601 时间。");
        try { return Instant.parse(value.asText()); }
        catch (RuntimeException malformed) { throw EafException.invalid(field + " 必须是 ISO-8601 时间。"); }
    }

    private UUID uuid(JsonNode value, String field) {
        if (!value.isTextual()) throw EafException.invalid(field + " 必须是 UUID 字符串。");
        try { return UUID.fromString(value.asText()); }
        catch (IllegalArgumentException malformed) { throw EafException.invalid(field + " 必须是 UUID 字符串。"); }
    }

    private static boolean validEnvelope(JsonNode request) {
        if (request == null || !request.isObject() || !only(request, Set.of("jsonrpc", "id", "method", "params")))
            return false;
        var id = request.path("id");
        return "2.0".equals(request.path("jsonrpc").asText())
                && (id.isTextual() || id.isNumber()) && request.path("method").isTextual()
                && request.path("params").isObject();
    }

    private static boolean only(JsonNode value, Set<String> allowed) {
        Iterator<String> fields = value.fieldNames();
        while (fields.hasNext()) if (!allowed.contains(fields.next())) return false;
        return true;
    }

    private void requireOnly(JsonNode value, Set<String> allowed) {
        if (!only(value, allowed)) throw EafException.invalid("A2A params 包含未支持字段。");
    }

    private ObjectNode success(JsonNode id, JsonNode result) {
        var response = json.createObjectNode().put("jsonrpc", "2.0");
        response.set("id", id.deepCopy());
        response.set("result", result);
        return response;
    }

    private ObjectNode error(JsonNode id, EafException failure, String method) {
        if (List.of("TASK_CAPACITY_EXCEEDED", "TASK_EXECUTION_BUSY", "MODEL_CAPACITY_EXCEEDED")
                .contains(failure.code())) {
            var detail = json.createObjectNode().put("code", -32000).put("message", "服务暂时无法接纳此请求。");
            detail.set("data", json.createObjectNode().put("code", failure.code()).put("retryable", failure.retryable()));
            var response = json.createObjectNode().put("jsonrpc", "2.0");
            response.set("id", id == null ? json.nullNode() : id.deepCopy());
            response.set("error", detail);
            return response;
        }
        if ("RESOURCE_NOT_FOUND".equals(failure.code()))
            return error(id, A2AErrorCodes.TASK_NOT_FOUND.code(), "Task 不存在或当前身份不可见。");
        if ("IDEMPOTENCY_CONFLICT".equals(failure.code()))
            return error(id, A2AErrorCodes.INVALID_PARAMS.code(), "messageId 已用于不同内容。");
        if ("POLICY_DENIED".equals(failure.code()) || "UNAUTHENTICATED".equals(failure.code()))
            return error(id, -32000, "当前身份不能执行此 A2A 操作。");
        if ("CancelTask".equals(method) && failure.status() == 409)
            return error(id, A2AErrorCodes.TASK_NOT_CANCELABLE.code(), "Task 当前状态不能取消。");
        if (failure.status() == 400) return error(id, A2AErrorCodes.INVALID_PARAMS.code(), failure.getMessage());
        return error(id, A2AErrorCodes.INTERNAL.code(), "服务未能完成 A2A 请求。");
    }

    private ObjectNode error(JsonNode id, int code, String message) {
        var detail = json.createObjectNode().put("code", code).put("message", message);
        var response = json.createObjectNode().put("jsonrpc", "2.0");
        response.set("id", id == null ? json.nullNode() : id.deepCopy());
        response.set("error", detail);
        return response;
    }

    private static final class UnknownMethodException extends RuntimeException { }
}
