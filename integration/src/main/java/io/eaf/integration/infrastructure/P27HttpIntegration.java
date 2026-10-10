package io.eaf.integration.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.connector.api.ConnectorDefinition;
import io.eaf.connector.api.P27BusinessConnectionIntegrationPort;
import io.eaf.credential.api.CredentialRequest;
import io.eaf.credential.api.CredentialResolutionPort;
import io.eaf.shared.EafException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;

/** P27 本地合成适配器；固定路径、主体映射和凭据用途由 Connector Owner 校验。 */
@Service
public class P27HttpIntegration implements P27BusinessConnectionIntegrationPort {
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._:-]{1,160}");
    private static final int MAX_REQUEST_BYTES = 16_384;
    private static final int MAX_RESPONSE_BYTES = 16_384;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NEVER).build();
    private final ObjectMapper json;
    private final CredentialResolutionPort credentials;
    private final OutboundTargetPolicy targets;
    private final Semaphore permits = new Semaphore(16);

    public P27HttpIntegration(ObjectMapper json, CredentialResolutionPort credentials, OutboundTargetPolicy targets) {
        this.json = json;
        this.credentials = credentials;
        this.targets = targets;
    }

    @Override
    public OaTodoPage listTodos(ConnectorDefinition connector, String externalSubjectId, String status,
                                String cursor, int limit, Instant deadline) {
        requireId(externalSubjectId, "员工映射");
        if (status != null && !Set.of("OPEN", "IN_PROGRESS", "DONE").contains(status)
                || limit < 1 || limit > 50 || cursor != null && cursor.length() > 512)
            throw EafException.invalid("OA 待办查询字段无效。");
        var query = "?limit=" + limit + (status == null ? "" : "&status=" + status)
                + (cursor == null ? "" : "&cursor=" + URLEncoder.encode(cursor, StandardCharsets.UTF_8));
        var uri = targets.p27OaFixtureEndpoint(connector, "/users/" + externalSubjectId + "/todos" + query);
        var response = send(HttpRequest.newBuilder(uri).timeout(timeout(deadline))
                .header("Authorization", "Bearer " + oaCredential(connector, "oa.todo.read", "oa.todo.read"))
                .header("X-External-Subject", externalSubjectId).GET().build(), deadline);
        if (response.statusCode() == 401 || response.statusCode() == 403 || response.statusCode() == 404)
            throw EafException.notFound();
        if (response.statusCode() != 200 || response.oversized())
            throw EafException.conflict("OA_READ_FAILED", "OA 待办读取失败或响应超过上限。");
        try {
            var root = json.readTree(response.bodyText());
            if (!"EAF-OA-TODO-V1".equals(root.path("sourceId").asText()) || !root.path("items").isArray()
                    || root.path("items").size() > limit)
                throw new IllegalArgumentException("OA page");
            var fetchedAt = Instant.now();
            var items = new ArrayList<OaTodo>();
            for (var item : root.path("items")) items.add(parseTodo(item, externalSubjectId, fetchedAt));
            var next = root.hasNonNull("nextCursor") ? bounded(root.path("nextCursor").asText(), 512) : null;
            return new OaTodoPage(items, next, fetchedAt);
        } catch (Exception malformed) {
            throw EafException.conflict("INVALID_TOOL_RESULT", "OA 返回字段无效。");
        }
    }

    @Override
    public Optional<OaTodo> getTodo(ConnectorDefinition connector, String externalSubjectId,
                                    String todoId, Instant deadline) {
        requireId(externalSubjectId, "员工映射"); requireId(todoId, "todoId");
        var uri = targets.p27OaFixtureEndpoint(connector, "/users/" + externalSubjectId + "/todos/" + todoId);
        var response = send(HttpRequest.newBuilder(uri).timeout(timeout(deadline))
                .header("Authorization", "Bearer " + oaCredential(connector, "oa.todo.read", "oa.todo.read"))
                .header("X-External-Subject", externalSubjectId).GET().build(), deadline);
        if (response.statusCode() == 404 || response.statusCode() == 403) return Optional.empty();
        if (response.statusCode() != 200 || response.oversized())
            throw EafException.conflict("OA_READ_FAILED", "OA 单项读取失败或响应超过上限。");
        try { return Optional.of(parseTodo(json.readTree(response.bodyText()), externalSubjectId, Instant.now())); }
        catch (Exception malformed) { throw EafException.conflict("INVALID_TOOL_RESULT", "OA 返回字段无效。"); }
    }

    @Override
    public boolean canReadTodos(ConnectorDefinition connector, String externalSubjectId, String todoId, Instant deadline) {
        requireId(externalSubjectId, "员工映射");
        var path = todoId == null ? "/users/" + externalSubjectId + "/todos"
                : "/users/" + externalSubjectId + "/todos/" + requireId(todoId, "todoId");
        var response = send(HttpRequest.newBuilder(targets.p27OaFixtureEndpoint(connector, path))
                .timeout(timeout(deadline)).header("Authorization", "Bearer "
                        + oaCredential(connector, "oa.todo.read", "oa.todo.read"))
                .header("X-External-Subject", externalSubjectId).method("HEAD", HttpRequest.BodyPublishers.noBody()).build(), deadline);
        if (response.statusCode() == 200) return true;
        if (response.statusCode() == 404 || response.statusCode() == 403) return false;
        throw EafException.conflict("OA_ACCESS_UNAVAILABLE", "OA 当前无法确认待办读取权限。");
    }

    @Override
    public ServiceRequestCurrentState readCurrentState(ConnectorDefinition connector, String externalSubjectId,
                                                        String requestId, Instant deadline) {
        requireId(externalSubjectId, "员工映射"); requireId(requestId, "requestId");
        var response = send(HttpRequest.newBuilder(targets.p27ServiceDeskFixtureEndpoint(connector,
                        "/requests/" + requestId + "/state")).timeout(timeout(deadline))
                .header("Authorization", "Bearer " + serviceCredential(connector,
                        "service.request.status.read", "service.request.status.read"))
                .header("X-External-Subject", externalSubjectId).GET().build(), deadline);
        if (response.statusCode() == 404 || response.statusCode() == 403) throw EafException.notFound();
        if (response.statusCode() != 200 || response.oversized())
            throw EafException.conflict("SERVICE_REQUEST_STATUS_FAILED", "服务台当前状态读取失败。");
        try {
            var root = json.readTree(response.bodyText());
            var state = new ServiceRequestCurrentState(root.path("sourceId").asText(null),
                    root.path("requestId").asText(null), root.path("registrationOperationId").asText(null),
                    root.path("status").asText(null), root.path("externalVersion").asText(null),
                    root.hasNonNull("updatedAt") ? Instant.parse(root.path("updatedAt").asText()) : null, Instant.now());
            if (!"EAF-SERVICE-DESK-HANDLING-V1".equals(root.path("contractVersion").asText())
                    || !"P15_INTERNAL_SERVICE_DESK_FIXTURE".equals(state.sourceId())
                    || !requestId.equals(state.requestId()) || !ID.matcher(state.registrationOperationId() == null ? "" : state.registrationOperationId()).matches()
                    || !Set.of("REGISTERED", "IN_PROGRESS", "RESOLVED").contains(state.status())
                    || !validVersion(state.externalVersion())) throw new IllegalArgumentException("state");
            return state;
        } catch (Exception malformed) { throw EafException.conflict("INVALID_TOOL_RESULT", "服务台当前状态回执无效。"); }
    }

    @Override
    public boolean canReadServiceRequest(ConnectorDefinition connector, String externalSubjectId,
                                         String requestId, Instant deadline) {
        requireId(externalSubjectId, "员工映射"); requireId(requestId, "requestId");
        var response = send(HttpRequest.newBuilder(targets.p27ServiceDeskFixtureEndpoint(connector,
                        "/requests/" + requestId + "/state")).timeout(timeout(deadline))
                .header("Authorization", "Bearer " + serviceCredential(connector,
                        "service.request.status.read", "service.request.status.read"))
                .header("X-External-Subject", externalSubjectId).method("HEAD", HttpRequest.BodyPublishers.noBody()).build(), deadline);
        if (response.statusCode() == 200) return true;
        if (response.statusCode() == 404 || response.statusCode() == 403) return false;
        throw EafException.conflict("SERVICE_REQUEST_ACCESS_UNAVAILABLE", "服务台当前无法确认请求读取权限。");
    }

    @Override
    public ServiceRequestResultWriteResult recordHandlingResult(ConnectorDefinition connector, String externalSubjectId,
            ServiceRequestHandlingPayload payload, Instant deadline) {
        if (payload == null || !validPayload(payload, externalSubjectId))
            return ServiceRequestResultWriteResult.rejected("SERVICE_REQUEST_RESULT_INVALID", "处理结果写回参数无效。");
        final byte[] body;
        try {
            var node = json.createObjectNode().put("contractVersion", "EAF-SERVICE-DESK-HANDLING-V1")
                    .put("operationId", payload.operationId()).put("requestId", payload.requestId())
                    .put("registrationOperationId", payload.registrationOperationId()).put("workItemId", payload.workItemId())
                    .put("workItemVersion", payload.workItemVersion()).put("sourceResultHash", payload.sourceResultHash())
                    .put("externalSubjectId", payload.externalSubjectId()).put("completedBy", payload.completedBy())
                    .put("completedAt", payload.completedAt().toString()).put("outcome", payload.outcome())
                    .put("summary", payload.summary()).put("nextAction", payload.nextAction());
            body = json.writeValueAsBytes(node);
        } catch (Exception invalid) { return ServiceRequestResultWriteResult.rejected("SERVICE_REQUEST_RESULT_INVALID", "处理结果写回参数无法编码。"); }
        if (body.length > MAX_REQUEST_BYTES)
            return ServiceRequestResultWriteResult.rejected("SERVICE_REQUEST_RESULT_TOO_LARGE", "处理结果超过载荷上限。");
        var response = send(HttpRequest.newBuilder(targets.p27ServiceDeskFixtureEndpoint(connector,
                        "/requests/" + payload.requestId() + "/handling-results")).timeout(timeout(deadline))
                .header("Authorization", "Bearer " + serviceCredential(connector,
                        "service.request.result.write", "service.request.result.write"))
                .header("X-External-Subject", externalSubjectId).header("Content-Type", "application/json")
                .header("Idempotency-Key", payload.operationId()).header("If-Match", payload.expectedExternalVersion())
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(), deadline);
        if (response.oversized() || response.statusCode() >= 500)
            return ServiceRequestResultWriteResult.unknown("SERVICE_REQUEST_RESULT_UNKNOWN", "服务台可能已接收处理结果，需核验原 operationId。");
        if (response.statusCode() == 409 || response.statusCode() == 412)
            return ServiceRequestResultWriteResult.rejected("SERVICE_REQUEST_VERSION_CONFLICT", "服务台版本已变化，请重新查询并申请审批。");
        if (response.statusCode() == 401 || response.statusCode() == 403 || response.statusCode() == 404)
            return ServiceRequestResultWriteResult.rejected("SERVICE_REQUEST_RESULT_REJECTED", "服务台拒绝处理结果写回。");
        if (response.statusCode() < 200 || response.statusCode() >= 300)
            return ServiceRequestResultWriteResult.rejected("SERVICE_REQUEST_RESULT_REJECTED", "服务台未接受处理结果。");
        try { return ServiceRequestResultWriteResult.accepted(parseReceipt(response.bodyText(), payload.operationId())); }
        catch (Exception malformed) { return ServiceRequestResultWriteResult.unknown("SERVICE_REQUEST_RESULT_RECEIPT_INVALID", "服务台可能已写入，但回执无法核验。"); }
    }

    @Override
    public Optional<ServiceRequestHandlingReceipt> findHandlingResult(ConnectorDefinition connector,
            String externalSubjectId, String operationId, Instant deadline) {
        requireId(externalSubjectId, "员工映射"); requireId(operationId, "operationId");
        var response = send(HttpRequest.newBuilder(targets.p27ServiceDeskFixtureEndpoint(connector,
                        "/handling-results/by-operation/" + operationId)).timeout(timeout(deadline))
                .header("Authorization", "Bearer " + serviceCredential(connector,
                        "service.request.result.verify", "service.request.result.read"))
                .header("X-External-Subject", externalSubjectId).GET().build(), deadline);
        if (response.statusCode() == 404) return Optional.empty();
        if (response.statusCode() != 200 || response.oversized())
            throw EafException.conflict("SERVICE_REQUEST_VERIFY_UNAVAILABLE", "服务台结果核验接口不可用。");
        try { return Optional.of(parseReceipt(response.bodyText(), operationId)); }
        catch (Exception malformed) { throw EafException.conflict("SERVICE_REQUEST_VERIFY_UNAVAILABLE", "服务台结果回执无效。"); }
    }

    private OaTodo parseTodo(JsonNode node, String externalSubjectId, Instant fetchedAt) {
        var todo = new OaTodo(node.path("sourceId").asText(null), node.path("todoId").asText(null),
                node.path("title").asText(null), node.path("status").asText(null),
                node.hasNonNull("dueAt") ? bounded(node.path("dueAt").asText(), 50) : null,
                node.path("sourceVersion").asText(null), node.hasNonNull("updatedAt")
                ? Instant.parse(node.path("updatedAt").asText()) : null, fetchedAt);
        if (!"EAF-OA-TODO-V1".equals(todo.sourceId()) || !ID.matcher(todo.todoId() == null ? "" : todo.todoId()).matches()
                || !validText(todo.title(), 240) || !Set.of("OPEN", "IN_PROGRESS", "DONE").contains(todo.status())
                || !validVersion(todo.sourceVersion()) || node.hasNonNull("externalSubjectId")
                && !externalSubjectId.equals(node.path("externalSubjectId").asText()))
            throw new IllegalArgumentException("todo");
        return todo;
    }

    private ServiceRequestHandlingReceipt parseReceipt(String body, String operationId) throws Exception {
        var node = json.readTree(body);
        var receipt = new ServiceRequestHandlingReceipt(node.path("operationId").asText(null),
                node.path("requestId").asText(null), node.path("registrationOperationId").asText(null),
                node.path("resultId").asText(null),
                node.path("workItemId").asText(null), node.path("workItemVersion").asLong(-1),
                node.path("sourceResultHash").asText(null), node.path("externalSubjectId").asText(null),
                node.path("completedBy").asText(null), Instant.parse(node.path("completedAt").asText()),
                node.path("outcome").asText(null), node.path("summary").asText(null),
                node.path("nextAction").asText(null), node.path("recordState").asText(null),
                node.path("previousExternalVersion").asText(null), node.path("resultingExternalVersion").asText(null),
                node.path("resultingStatus").asText(null), Instant.parse(node.path("acceptedAt").asText()));
        if (!"EAF-SERVICE-DESK-HANDLING-V1".equals(node.path("contractVersion").asText())
                || !operationId.equals(receipt.operationId()) || !ID.matcher(receipt.requestId() == null ? "" : receipt.requestId()).matches()
                || !ID.matcher(receipt.registrationOperationId() == null ? "" : receipt.registrationOperationId()).matches()
                || !ID.matcher(receipt.resultId() == null ? "" : receipt.resultId()).matches()
                || !ID.matcher(receipt.workItemId() == null ? "" : receipt.workItemId()).matches()
                || receipt.workItemVersion() < 1 || !hash(receipt.sourceResultHash())
                || !ID.matcher(receipt.externalSubjectId() == null ? "" : receipt.externalSubjectId()).matches()
                || !validText(receipt.completedBy(), 36) || !Set.of("COMPLETED", "BLOCKED", "NEEDS_FOLLOWUP").contains(receipt.outcome())
                || !validText(receipt.summary(), 2_000) || receipt.nextAction() != null && receipt.nextAction().length() > 1_000
                || !"RECORDED".equals(receipt.recordState()) || !validVersion(receipt.previousExternalVersion())
                || !validVersion(receipt.resultingExternalVersion()) || !Set.of("IN_PROGRESS", "RESOLVED").contains(receipt.resultingStatus())
                || receipt.acceptedAt() == null)
            throw new IllegalArgumentException("handling receipt");
        return receipt;
    }

    private boolean validPayload(ServiceRequestHandlingPayload payload, String externalSubjectId) {
        return ID.matcher(payload.operationId() == null ? "" : payload.operationId()).matches()
                && ID.matcher(payload.requestId() == null ? "" : payload.requestId()).matches()
                && ID.matcher(payload.registrationOperationId() == null ? "" : payload.registrationOperationId()).matches()
                && ID.matcher(payload.workItemId() == null ? "" : payload.workItemId()).matches()
                && payload.workItemVersion() > 0 && hash(payload.sourceResultHash())
                && externalSubjectId.equals(payload.externalSubjectId()) && validText(payload.completedBy(), 36)
                && payload.completedAt() != null && Set.of("COMPLETED", "BLOCKED", "NEEDS_FOLLOWUP").contains(payload.outcome())
                && validText(payload.summary(), 2_000) && (payload.nextAction() == null || payload.nextAction().length() <= 1_000)
                && validVersion(payload.expectedExternalVersion());
    }

    private String oaCredential(ConnectorDefinition connector, String use, String permission) {
        if (connector == null || !"P27_OA_TODO_FIXTURE".equals(connector.provider())
                || !"p27-oa".equals(connector.credentialRef()) || !"eaf:p27-oa".equals(connector.audience())
                || !connector.allowedUses().equals(Set.of("oa.todo.read"))
                || !connector.permissions().equals(Set.of("oa.todo.read")))
            throw EafException.conflict("CREDENTIAL_SCOPE_DENIED", "P27 OA 凭据未登记固定只读用途。");
        return credentials.resolve(new CredentialRequest(connector.tenantId(), connector.workspaceId(),
                connector.credentialRef(), connector.audience(), use, permission)).valueForOutboundRequest();
    }

    private String serviceCredential(ConnectorDefinition connector, String use, String permission) {
        if (connector == null || !"P27_SERVICE_DESK_RESULT_FIXTURE".equals(connector.provider())
                || !"p27-service-desk-result".equals(connector.credentialRef())
                || !"eaf:p27-service-desk-result".equals(connector.audience())
                || !connector.allowedUses().equals(Set.of("service.request.status.read", "service.request.result.write", "service.request.result.verify"))
                || !connector.permissions().equals(Set.of("service.request.status.read", "service.request.result.write", "service.request.result.read"))
                || !("service.request.status.read".equals(use) && "service.request.status.read".equals(permission)
                || "service.request.result.write".equals(use) && "service.request.result.write".equals(permission)
                || "service.request.result.verify".equals(use) && "service.request.result.read".equals(permission)))
            throw EafException.conflict("CREDENTIAL_SCOPE_DENIED", "P27 服务台凭据未登记固定用途。");
        return credentials.resolve(new CredentialRequest(connector.tenantId(), connector.workspaceId(),
                connector.credentialRef(), connector.audience(), use, permission)).valueForOutboundRequest();
    }

    private BoundedHttpResponse send(HttpRequest request, Instant deadline) {
        if (!permits.tryAcquire()) throw EafException.conflict("P27_OUTBOUND_CAPACITY", "P27 业务连接出站并发已满。");
        try {
            return BoundedHttpResponse.send(client, request, MAX_RESPONSE_BYTES, deadline, Duration.ofSeconds(10));
        } catch (java.net.http.HttpTimeoutException timeout) {
            throw EafException.conflict("P27_OUTBOUND_TIMEOUT", "P27 外部读取超时。");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw EafException.conflict("P27_OUTBOUND_INTERRUPTED", "P27 外部请求被中断。");
        } catch (EafException error) { throw error;
        } catch (Exception failure) { throw EafException.conflict("P27_UPSTREAM_FAILURE", "P27 外部请求失败。");
        } finally { permits.release(); }
    }

    private Duration timeout(Instant deadline) {
        return Duration.ofMillis(Math.min(10_000, Math.max(1, Duration.between(Instant.now(), deadline).toMillis())));
    }
    private String requireId(String value, String field) {
        if (!ID.matcher(value == null ? "" : value).matches()) throw EafException.invalid(field + " 标识无效。");
        return value;
    }
    private String bounded(String value, int max) {
        if (value == null || value.length() > max || value.chars().anyMatch(Character::isISOControl))
            throw new IllegalArgumentException("bounded text");
        return value;
    }
    private boolean validText(String value, int max) {
        return value != null && !value.isBlank() && value.length() <= max
                && value.chars().noneMatch(Character::isISOControl);
    }
    private boolean validVersion(String value) { return validText(value, 120); }
    private boolean hash(String value) { return value != null && value.matches("[0-9a-f]{64}"); }
}
