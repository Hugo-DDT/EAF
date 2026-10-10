package io.eaf.integration.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.connector.api.ConnectorDefinition;
import io.eaf.connector.api.CustomerRecord;
import io.eaf.connector.api.ExternalWriteResult;
import io.eaf.connector.api.FollowupRecord;
import io.eaf.connector.api.FollowupOutcomeRecord;
import io.eaf.connector.api.ExternalOutcomeWriteResult;
import io.eaf.connector.api.IntegrationPort;
import io.eaf.connector.api.ServiceRequestIntegrationPort;
import io.eaf.connector.api.ServiceRequestPayload;
import io.eaf.connector.api.ServiceRequestReceipt;
import io.eaf.connector.api.ServiceRequestWriteResult;
import io.eaf.credential.api.CredentialRequest;
import io.eaf.credential.api.CredentialResolutionPort;
import io.eaf.shared.EafException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;

@Service
public class TestCrmHttpIntegration implements IntegrationPort, ServiceRequestIntegrationPort {
    private static final Pattern CUSTOMER_ID = Pattern.compile("[A-Za-z0-9._:-]{1,160}");
    private static final Pattern OPERATION_ID = Pattern.compile("[A-Za-z0-9._:-]{1,160}");
    // 测试 CRM 也按字节限制流读取，并共享固定出站并发额度。
    private static final int MAX_REQUEST_BYTES = 16_384;
    private static final int MAX_RESPONSE_BYTES = 16_384;
    private static final int MAX_CONCURRENT_REQUESTS = 16;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NEVER).build();
    private final ObjectMapper json;
    private final CredentialResolutionPort credentials;
    private final OutboundTargetPolicy targets;
    private final Semaphore outboundPermits = new Semaphore(MAX_CONCURRENT_REQUESTS);

    public TestCrmHttpIntegration(ObjectMapper json, CredentialResolutionPort credentials, OutboundTargetPolicy targets) {
        this.json = json;
        this.credentials = credentials;
        this.targets = targets;
    }

    @Override
    public CustomerRecord readCustomer(ConnectorDefinition connector, String customerId, Instant deadline) {
        if (customerId == null || !CUSTOMER_ID.matcher(customerId).matches()
                || ".".equals(customerId) || "..".equals(customerId))
            throw EafException.invalid("customerId 包含不允许的字符。");
        try {
            var p7ContractFixture = connector != null && Set.of("P7_CRM_READ_CONTRACT_FIXTURE", "P7_CRM_WRITE_CONTRACT_FIXTURE").contains(connector.provider());
            var uri = p7ContractFixture
                    ? targets.crmContractFixtureEndpoint(connector, "/customers/" + customerId)
                    : targets.testCrmEndpoint(connector, "/customers/" + customerId);
            // Connector 的用途快照与 Credential 权威绑定须同时允许本次读取。
            var token = crmCredential(connector, "customer.read", "crm.customer.read");
            var remaining = Math.max(1, Duration.between(Instant.now(), deadline).toMillis());
            var request = HttpRequest.newBuilder(uri).timeout(Duration.ofMillis(Math.min(10_000, remaining)))
                    .header("Authorization", "Bearer " + token).GET().build();
            var response = sendBounded(request, deadline);
            if (response.statusCode() == 401 || response.statusCode() == 403 || response.statusCode() == 404)
                throw EafException.conflict("CRM_CUSTOMER_UNAVAILABLE", "CRM 未提供所请求客户。");
            if (response.statusCode() == 409) throw EafException.conflict("CRM_CONFLICT", "CRM 拒绝了冲突的客户读取。");
            if (response.statusCode() == 429) throw EafException.conflict("CRM_RATE_LIMITED", "CRM 暂时限制了读取请求。");
            if (response.statusCode() >= 500) throw EafException.conflict("CRM_UPSTREAM_FAILURE", "CRM 暂时不可用。");
            // HttpClient 不跟随重定向；将其保留为既有契约错误，避免暗中切换出站目标。
            if (response.statusCode() >= 300 && response.statusCode() < 400)
                throw EafException.conflict("INVALID_TOOL_RESULT", "CRM 返回未跟随的重定向响应。");
            if (response.statusCode() != 200) throw EafException.conflict("CRM_REQUEST_REJECTED", "CRM 未接受客户读取请求。");
            if (response.oversized()) throw EafException.conflict("INVALID_TOOL_RESULT", "CRM 响应超出大小上限。");
            var root = json.readTree(response.bodyText());
            if (p7ContractFixture && !"EAF-CRM-READ-V1".equals(root.path("contractVersion").asText(null)))
                throw EafException.conflict("INVALID_TOOL_RESULT", "CRM 响应契约版本不匹配。");
            var result = new CustomerRecord(root.path("customerId").asText(null), root.path("renewalStatus").asText(null),
                    root.path("lastContactDate").asText(null), root.path("complaintSummary").asText(null),
                    root.path("sourceId").asText(null), root.path("externalVersion").asText(null), Instant.now());
            if (result.customerId() == null || result.renewalStatus() == null || result.lastContactDate() == null
                    || result.complaintSummary() == null || result.sourceId() == null
                    || p7ContractFixture && !validExternalVersion(result.externalVersion()))
                throw EafException.conflict("INVALID_TOOL_RESULT", "CRM 缺少必需字段或外部版本无效。");
            return result;
        } catch (EafException e) {
            throw e;
        } catch (java.net.http.HttpTimeoutException e) {
            throw EafException.conflict("CRM_TIMEOUT", "测试 CRM 读取超时。");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw EafException.conflict("CRM_INTERRUPTED", "测试 CRM 读取被中断。");
        } catch (Exception e) {
            throw EafException.conflict("CRM_UPSTREAM_FAILURE", "CRM 读取失败。");
        }
    }

    @Override
    public ExternalWriteResult createFollowup(ConnectorDefinition connector, String operationId, String customerId,
                                              String summary, String ownerId, Instant deadline) {
        if (!OPERATION_ID.matcher(operationId == null ? "" : operationId).matches()
                || !CUSTOMER_ID.matcher(customerId == null ? "" : customerId).matches())
            return ExternalWriteResult.rejected("CRM_REQUEST_INVALID", "测试 CRM 跟进标识无效。");
        var p7WriteContract = connector != null && "P7_CRM_WRITE_CONTRACT_FIXTURE".equals(connector.provider());
        var uri = p7WriteContract ? targets.crmContractFixtureEndpoint(connector, "/followups")
                : targets.testCrmEndpoint(connector, "/followups");
        byte[] requestBody;
        try {
            var body = json.createObjectNode();
            if (p7WriteContract) body.put("contractVersion", "EAF-CRM-WRITE-V1");
            body.put("operationId", operationId);
            body.put("customerId", customerId);
            body.put("summary", summary);
            body.put("ownerId", ownerId);
            requestBody = json.writeValueAsBytes(body);
        } catch (Exception invalid) {
            return ExternalWriteResult.rejected("CRM_REQUEST_INVALID", "测试 CRM 跟进请求无效。");
        }
        if (requestBody.length > MAX_REQUEST_BYTES)
            return ExternalWriteResult.rejected("CRM_REQUEST_TOO_LARGE", "测试 CRM 跟进请求超出上限。");
        var token = crmCredential(connector, "followup.create", "crm.followup.create");
        try {
            var request = HttpRequest.newBuilder(uri).timeout(timeout(deadline))
                    .header("Content-Type", "application/json").header("Idempotency-Key", operationId)
                    .header("Authorization", "Bearer " + token)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody)).build();
            var response = sendBounded(request, deadline);
            if (response.oversized()) return ExternalWriteResult.unknown("CRM_RESPONSE_TOO_LARGE", "测试 CRM 写入响应超出上限，外部事实待核验。");
            if (response.statusCode() == 409) return ExternalWriteResult.rejected("CRM_IDEMPOTENCY_CONFLICT", "测试 CRM 拒绝了不一致的重复操作。");
            if (response.statusCode() == 401 || response.statusCode() == 403) return ExternalWriteResult.rejected("CRM_FORBIDDEN", "测试 CRM 拒绝了写入。");
            if (response.statusCode() >= 500) return ExternalWriteResult.unknown("CRM_UPSTREAM_FAILURE", "测试 CRM 在写入后返回了暂时性错误。");
            if (response.statusCode() >= 300 && response.statusCode() < 400)
                return ExternalWriteResult.rejected("INVALID_TOOL_RESULT", "CRM 返回未跟随的重定向响应。");
            if (response.statusCode() != 200 && response.statusCode() != 201 && response.statusCode() != 202)
                return ExternalWriteResult.rejected("CRM_WRITE_REJECTED", "测试 CRM 拒绝了跟进写入。");
            //  写入合同的响应字段全部必需；畸形响应仍可能已提交外部事实，因此不能降级成可重试拒绝。
            return ExternalWriteResult.accepted(p7WriteContract
                    ? parseP7Followup(response.bodyText(), operationId)
                    : parseFollowup(response.bodyText(), operationId, customerId, summary, ownerId));
        } catch (java.net.http.HttpTimeoutException e) {
            return ExternalWriteResult.unknown("CRM_TIMEOUT", "测试 CRM 写入响应超时，外部事实待核验。");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ExternalWriteResult.unknown("CRM_INTERRUPTED", "测试 CRM 写入被中断，外部事实待核验。");
        } catch (Exception e) {
            return ExternalWriteResult.unknown("CRM_UPSTREAM_FAILURE", "测试 CRM 写入结果不明。");
        }
    }

    @Override
    public java.util.Optional<FollowupRecord> findFollowup(ConnectorDefinition connector, String operationId, Instant deadline) {
        if (!OPERATION_ID.matcher(operationId == null ? "" : operationId).matches())
            throw EafException.invalid("operationId 无效。");
        var p7WriteContract = connector != null && "P7_CRM_WRITE_CONTRACT_FIXTURE".equals(connector.provider());
        var uri = p7WriteContract ? targets.crmContractFixtureEndpoint(connector, "/followups/by-operation/" + operationId)
                : targets.testCrmEndpoint(connector, "/followups/by-operation/" + operationId);
        var token = crmCredential(connector, "followup.verify", "crm.followup.read");
        try {
            var response = sendBounded(HttpRequest.newBuilder(uri).header("Authorization", "Bearer " + token)
                    .timeout(timeout(deadline)).GET().build(), deadline);
            if (response.statusCode() == 404) return java.util.Optional.empty();
            if (response.oversized() || response.statusCode() != 200) throw EafException.conflict("CRM_VERIFY_UNAVAILABLE", "测试 CRM 核验接口不可用或响应过大。");
            var root = json.readTree(response.bodyText());
            var record = p7WriteContract ? parseP7Followup(response.bodyText(), operationId)
                    : new FollowupRecord(root.path("operationId").asText(operationId), root.path("externalId").asText(null),
                    root.path("customerId").asText(null), root.path("summary").asText(null), root.path("ownerId").asText(null),
                    root.path("status").asText(null), Instant.parse(root.path("acceptedAt").asText()));
            return java.util.Optional.of(record);
        } catch (EafException e) { throw e;
        } catch (java.net.http.HttpTimeoutException e) { throw EafException.conflict("CRM_VERIFY_TIMEOUT", "测试 CRM 核验超时。");
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw EafException.conflict("CRM_VERIFY_INTERRUPTED", "测试 CRM 核验被中断。");
        } catch (Exception e) { throw EafException.conflict("CRM_VERIFY_UNAVAILABLE", "测试 CRM 核验失败。"); }
    }

    @Override
    public ExternalOutcomeWriteResult recordFollowupOutcome(ConnectorDefinition connector, String operationId,
            FollowupOutcomeRecord outcome, Instant deadline) {
        if (connector == null || !"P12_CRM_OUTCOME_FIXTURE".equals(connector.provider())
                || !OPERATION_ID.matcher(operationId == null ? "" : operationId).matches()
                || !validOutcome(outcome) || !operationId.equals(outcome.operationId()))
            return ExternalOutcomeWriteResult.rejected("CRM_OUTCOME_REQUEST_INVALID", "客户结果写入参数无效。");
        final byte[] requestBody;
        try {
            var body = json.createObjectNode().put("contractVersion", "EAF-CRM-FOLLOWUP-OUTCOME-V1")
                    .put("operationId", operationId).put("externalId", outcome.externalId())
                    .put("customerId", outcome.customerId()).put("followupId", outcome.followupId().toString())
                    .put("resultId", outcome.resultId().toString()).put("resultNo", outcome.resultNo())
                    .put("recordedBy", outcome.recordedBy().toString()).put("outcomeCode", outcome.outcomeCode())
                    .put("summary", outcome.summary()).put("disposition", outcome.disposition());
            if (outcome.nextAction() != null) body.put("nextAction", outcome.nextAction());
            if (outcome.nextContactAt() != null) body.put("nextContactAt", outcome.nextContactAt().toString());
            requestBody = json.writeValueAsBytes(body);
        } catch (Exception invalid) {
            return ExternalOutcomeWriteResult.rejected("CRM_OUTCOME_REQUEST_INVALID", "客户结果写入请求无法编码。");
        }
        if (requestBody.length > MAX_REQUEST_BYTES)
            return ExternalOutcomeWriteResult.rejected("CRM_REQUEST_TOO_LARGE", "CRM 结果写入请求超出上限。");
        var uri = targets.crmOutcomeFixtureEndpoint(connector, "/followups/" + outcome.externalId() + "/outcomes");
        var token = crmCredential(connector, "followup.result.create", "crm.followup.result");
        try {
            var request = HttpRequest.newBuilder(uri).timeout(timeout(deadline))
                    .header("Content-Type", "application/json").header("Idempotency-Key", operationId)
                    .header("Authorization", "Bearer " + token)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody)).build();
            var response = sendBounded(request, deadline);
            if (response.oversized()) return ExternalOutcomeWriteResult.unknown("CRM_RESPONSE_TOO_LARGE", "CRM 结果回执超出上限，外部状态待核验。");
            if (response.statusCode() == 409) return ExternalOutcomeWriteResult.rejected("CRM_IDEMPOTENCY_CONFLICT", "CRM 拒绝了不一致的重复结果操作。");
            if (response.statusCode() == 401 || response.statusCode() == 403 || response.statusCode() == 404)
                return ExternalOutcomeWriteResult.rejected("CRM_OUTCOME_REJECTED", "CRM 未接受客户结果写入。");
            if (response.statusCode() >= 500) return ExternalOutcomeWriteResult.unknown("CRM_UPSTREAM_FAILURE", "CRM 写入后返回暂时性错误，外部状态待核验。");
            if (response.statusCode() >= 300 && response.statusCode() < 400)
                return ExternalOutcomeWriteResult.rejected("INVALID_TOOL_RESULT", "CRM 返回未跟随的重定向响应。");
            if (response.statusCode() != 200 && response.statusCode() != 201 && response.statusCode() != 202)
                return ExternalOutcomeWriteResult.rejected("CRM_OUTCOME_REJECTED", "CRM 拒绝了客户结果写入。");
            return ExternalOutcomeWriteResult.accepted(parseOutcome(response.bodyText(), operationId));
        } catch (java.net.http.HttpTimeoutException timeout) {
            return ExternalOutcomeWriteResult.unknown("CRM_TIMEOUT", "CRM 结果写入响应超时，外部状态待核验。");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return ExternalOutcomeWriteResult.unknown("CRM_INTERRUPTED", "CRM 结果写入被中断，外部状态待核验。");
        } catch (Exception failure) {
            return ExternalOutcomeWriteResult.unknown("CRM_UPSTREAM_FAILURE", "CRM 结果写入回执无效或响应丢失，外部状态待核验。");
        }
    }

    @Override
    public java.util.Optional<FollowupOutcomeRecord> findFollowupOutcome(ConnectorDefinition connector,
            String operationId, Instant deadline) {
        if (connector == null || !"P12_CRM_OUTCOME_FIXTURE".equals(connector.provider())
                || !OPERATION_ID.matcher(operationId == null ? "" : operationId).matches())
            throw EafException.invalid("结果 operationId 无效。");
        var uri = targets.crmOutcomeFixtureEndpoint(connector, "/followup-outcomes/by-operation/" + operationId);
        var token = crmCredential(connector, "followup.result.verify", "crm.followup.read");
        try {
            var response = sendBounded(HttpRequest.newBuilder(uri).timeout(timeout(deadline))
                    .header("Authorization", "Bearer " + token).GET().build(), deadline);
            if (response.statusCode() == 404) return java.util.Optional.empty();
            if (response.oversized() || response.statusCode() != 200)
                throw EafException.conflict("CRM_VERIFY_UNAVAILABLE", "CRM 结果核验接口不可用或响应过大。");
            return java.util.Optional.of(parseOutcome(response.bodyText(), operationId));
        } catch (EafException e) { throw e;
        } catch (java.net.http.HttpTimeoutException e) { throw EafException.conflict("CRM_VERIFY_TIMEOUT", "CRM 结果核验超时。");
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw EafException.conflict("CRM_VERIFY_INTERRUPTED", "CRM 结果核验被中断。");
        } catch (Exception e) { throw EafException.conflict("CRM_VERIFY_UNAVAILABLE", "CRM 结果核验回执无效。"); }
    }

    @Override
    public ServiceRequestWriteResult register(ConnectorDefinition connector, ServiceRequestPayload payload, Instant deadline) {
        if (connector == null || !"P15_INTERNAL_SERVICE_DESK_FIXTURE".equals(connector.provider())
                || !validServiceRequest(payload))
            return ServiceRequestWriteResult.rejected("SERVICE_REQUEST_INVALID", "服务请求登记参数无效。");
        final byte[] body;
        try {
            var request = json.createObjectNode().put("operationId", payload.operationId())
                    .put("requesterId", payload.requesterId()).put("category", payload.category())
                    .put("title", payload.title()).put("summary", payload.summary())
                    .put("handlingSuggestion", payload.handlingSuggestion())
                    .put("sourceTaskId", payload.sourceTaskId().toString()).put("sourceResultHash", payload.sourceResultHash());
            body = json.writeValueAsBytes(request);
        } catch (Exception invalid) { return ServiceRequestWriteResult.rejected("SERVICE_REQUEST_INVALID", "服务请求登记载荷无法编码。"); }
        if (body.length > MAX_REQUEST_BYTES)
            return ServiceRequestWriteResult.rejected("SERVICE_REQUEST_TOO_LARGE", "服务请求登记载荷超过上限。");
        var endpoint = targets.serviceRequestFixtureEndpoint(connector, "/requests");
        var token = serviceRequestCredential(connector, "service.request.register", "service.request.register");
        try {
            var request = HttpRequest.newBuilder(endpoint).timeout(timeout(deadline))
                    .header("Content-Type", "application/json").header("Idempotency-Key", payload.operationId())
                    .header("Authorization", "Bearer " + token).POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
            var response = sendBounded(request, deadline);
            if (response.oversized()) return ServiceRequestWriteResult.unknown("SERVICE_REQUEST_RESPONSE_TOO_LARGE", "服务台响应超过上限，外部状态待核验。");
            if (response.statusCode() == 409) return ServiceRequestWriteResult.rejected("SERVICE_REQUEST_IDEMPOTENCY_CONFLICT", "服务台拒绝了内容不一致的重复操作。");
            if (response.statusCode() == 401 || response.statusCode() == 403 || response.statusCode() == 404)
                return ServiceRequestWriteResult.rejected("SERVICE_REQUEST_REJECTED", "服务台拒绝了登记请求。");
            if (response.statusCode() >= 500) return ServiceRequestWriteResult.unknown("SERVICE_REQUEST_UPSTREAM_FAILURE", "服务台写入后返回暂时性错误，外部状态待核验。");
            if (response.statusCode() >= 300 && response.statusCode() < 400)
                return ServiceRequestWriteResult.rejected("INVALID_TOOL_RESULT", "服务台返回未跟随的重定向响应。");
            if (response.statusCode() != 200 && response.statusCode() != 201 && response.statusCode() != 202)
                return ServiceRequestWriteResult.rejected("SERVICE_REQUEST_REJECTED", "服务台未接受登记请求。");
            try { return ServiceRequestWriteResult.accepted(parseServiceRequest(response.bodyText(), payload.operationId())); }
            catch (Exception malformed) { return ServiceRequestWriteResult.unknown("SERVICE_REQUEST_RECEIPT_INVALID", "服务台可能已接收请求，但回执无法核验。"); }
        } catch (java.net.http.HttpTimeoutException e) {
            return ServiceRequestWriteResult.unknown("SERVICE_REQUEST_TIMEOUT", "服务台登记超时，外部事实待核验。");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ServiceRequestWriteResult.unknown("SERVICE_REQUEST_INTERRUPTED", "服务台登记中断，外部事实待核验。");
        } catch (Exception e) {
            return ServiceRequestWriteResult.unknown("SERVICE_REQUEST_UPSTREAM_FAILURE", "服务台登记结果不明。");
        }
    }

    @Override
    public java.util.Optional<ServiceRequestReceipt> find(ConnectorDefinition connector, String operationId, Instant deadline) {
        if (connector == null || !"P15_INTERNAL_SERVICE_DESK_FIXTURE".equals(connector.provider())
                || !OPERATION_ID.matcher(operationId == null ? "" : operationId).matches())
            throw EafException.invalid("服务请求 operationId 无效。");
        var endpoint = targets.serviceRequestFixtureEndpoint(connector, "/requests/by-operation/" + operationId);
        var token = serviceRequestCredential(connector, "service.request.verify", "service.request.read");
        try {
            var request = HttpRequest.newBuilder(endpoint).timeout(timeout(deadline))
                    .header("Authorization", "Bearer " + token).GET().build();
            var response = sendBounded(request, deadline);
            if (response.statusCode() == 404) return java.util.Optional.empty();
            if (response.oversized() || response.statusCode() != 200)
                throw EafException.conflict("SERVICE_REQUEST_VERIFY_UNAVAILABLE", "服务台核验接口不可用或响应超过上限。");
            return java.util.Optional.of(parseServiceRequest(response.bodyText(), operationId));
        } catch (EafException e) { throw e;
        } catch (java.net.http.HttpTimeoutException e) { throw EafException.conflict("SERVICE_REQUEST_VERIFY_TIMEOUT", "服务台核验超时。");
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw EafException.conflict("SERVICE_REQUEST_VERIFY_INTERRUPTED", "服务台核验被中断。");
        } catch (Exception e) { throw EafException.conflict("SERVICE_REQUEST_VERIFY_UNAVAILABLE", "服务台核验回执无效。"); }
    }

    private boolean validServiceRequest(ServiceRequestPayload payload) {
        return payload != null && OPERATION_ID.matcher(payload.operationId() == null ? "" : payload.operationId()).matches()
                && validContractField(payload.requesterId(), 36)
                && Set.of("IT", "FACILITIES", "HR", "OTHER").contains(payload.category())
                && validContractField(payload.title(), 120) && validContractField(payload.summary(), 2_000)
                && validContractField(payload.handlingSuggestion(), 2_000) && payload.sourceTaskId() != null
                && payload.sourceResultHash() != null && payload.sourceResultHash().matches("[0-9a-f]{64}");
    }

    private ServiceRequestReceipt parseServiceRequest(String body, String expectedOperationId) throws Exception {
        var root = json.readTree(body);
        var receipt = new ServiceRequestReceipt(root.path("requestId").asText(null), root.path("operationId").asText(null),
                root.path("status").asText(null), root.path("requesterId").asText(null), root.path("category").asText(null),
                root.path("title").asText(null), root.path("summary").asText(null), root.path("handlingSuggestion").asText(null),
                UUID.fromString(root.path("sourceTaskId").asText()), root.path("sourceResultHash").asText(null),
                root.hasNonNull("acceptedAt") ? Instant.parse(root.path("acceptedAt").asText()) : null);
        if (!validContractField(receipt.requestId(), 160) || !expectedOperationId.equals(receipt.operationId())
                || !"REGISTERED".equals(receipt.status()) || !validContractField(receipt.requesterId(), 36)
                || !Set.of("IT", "FACILITIES", "HR", "OTHER").contains(receipt.category())
                || !validContractField(receipt.title(), 120) || !validContractField(receipt.summary(), 2_000)
                || !validContractField(receipt.handlingSuggestion(), 2_000) || receipt.sourceTaskId() == null
                || receipt.sourceResultHash() == null || !receipt.sourceResultHash().matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("service request receipt");
        return receipt;
    }

    private String serviceRequestCredential(ConnectorDefinition connector, String use, String permission) {
        boolean allowed = connector != null && "P15_INTERNAL_SERVICE_DESK_FIXTURE".equals(connector.provider())
                && "p15-service-desk".equals(connector.credentialRef())
                && "eaf:p15-service-desk".equals(connector.audience())
                && connector.allowedUses().equals(Set.of("service.request.register", "service.request.verify"))
                && connector.permissions().equals(Set.of("service.request.register", "service.request.read"))
                && ("service.request.register".equals(use) && "service.request.register".equals(permission)
                || "service.request.verify".equals(use) && "service.request.read".equals(permission));
        if (!allowed) throw EafException.conflict("CREDENTIAL_SCOPE_DENIED", "服务台 Connector 未登记最小登记/核验凭据用途。");
        return credentials.resolve(new CredentialRequest(connector.tenantId(), connector.workspaceId(),
                connector.credentialRef(), connector.audience(), use, permission)).valueForOutboundRequest();
    }

    private boolean validOutcome(FollowupOutcomeRecord item) {
        return item != null && validContractField(item.externalId(), 160) && validContractField(item.customerId(), 160)
                && item.followupId() != null && item.resultId() != null && item.resultNo() > 0 && item.recordedBy() != null
                && Set.of("CONTACTED", "NO_RESPONSE", "RESOLVED", "OTHER").contains(item.outcomeCode())
                && validContractField(item.summary(), 2000) && (item.nextAction() == null || validContractField(item.nextAction(), 500))
                && Set.of("CONTINUE", "CLOSE").contains(item.disposition());
    }

    private FollowupOutcomeRecord parseOutcome(String body, String expectedOperationId) throws Exception {
        var root = json.readTree(body);
        var nextContactAt = root.hasNonNull("nextContactAt") ? Instant.parse(root.path("nextContactAt").asText()) : null;
        var record = new FollowupOutcomeRecord(root.path("operationId").asText(null), root.path("externalId").asText(null),
                root.path("customerId").asText(null), UUID.fromString(root.path("followupId").asText()),
                UUID.fromString(root.path("resultId").asText()), root.path("resultNo").asInt(0),
                UUID.fromString(root.path("recordedBy").asText()), root.path("outcomeCode").asText(null),
                root.path("summary").asText(null), root.hasNonNull("nextAction") ? root.path("nextAction").asText() : null,
                nextContactAt, root.path("disposition").asText(null), root.path("status").asText(null),
                Instant.parse(root.path("acceptedAt").asText(null)));
        if (!"EAF-CRM-FOLLOWUP-OUTCOME-V1".equals(root.path("contractVersion").asText(null))
                || !expectedOperationId.equals(record.operationId()) || !validOutcome(record)
                || !"RECORDED".equals(record.status()) || record.acceptedAt() == null)
            throw new IllegalArgumentException("CRM outcome receipt is incomplete or mismatched.");
        return record;
    }

    private FollowupRecord parseFollowup(String body, String operationId, String customerId, String summary, String ownerId) throws Exception {
        var root = json.readTree(body);
        return new FollowupRecord(root.path("operationId").asText(operationId), root.path("externalId").asText(null),
                root.path("customerId").asText(customerId), root.path("summary").asText(summary), root.path("ownerId").asText(ownerId),
                root.path("status").asText("CREATED"), root.has("acceptedAt") ? Instant.parse(root.path("acceptedAt").asText()) : Instant.now());
    }

    private FollowupRecord parseP7Followup(String body, String expectedOperationId) throws Exception {
        var root = json.readTree(body);
        var record = new FollowupRecord(root.path("operationId").asText(null), root.path("externalId").asText(null),
                root.path("customerId").asText(null), root.path("summary").asText(null), root.path("ownerId").asText(null),
                root.path("status").asText(null), Instant.parse(root.path("acceptedAt").asText(null)));
        if (!"EAF-CRM-WRITE-V1".equals(root.path("contractVersion").asText(null))
                || !expectedOperationId.equals(record.operationId()) || !validContractField(record.externalId(), 160)
                || !validContractField(record.customerId(), 160) || !validContractField(record.summary(), 2000)
                || !validContractField(record.ownerId(), 160) || !"CREATED".equals(record.status()) || record.acceptedAt() == null)
            throw new IllegalArgumentException("CRM write contract response is incomplete or mismatched.");
        return record;
    }

    private boolean validContractField(String value, int maxLength) {
        return value != null && !value.isBlank() && value.length() <= maxLength
                && value.chars().noneMatch(Character::isISOControl);
    }

    private String crmCredential(ConnectorDefinition connector, String use, String permission) {
        //  只读契约连接要求精确最小权限； 测试 CRM 仍保留既有审批写入用途。
        boolean allowed = connector != null && switch (connector.provider()) {
            case "TEST_CRM" -> "eaf:test-crm".equals(connector.audience())
                    && connector.allowedUses().contains(use) && connector.permissions().contains(permission);
            case "P7_CRM_READ_CONTRACT_FIXTURE" -> "p7-crm-read".equals(connector.credentialRef())
                    && "eaf:p7-crm-read".equals(connector.audience())
                    && connector.allowedUses().equals(Set.of("customer.read"))
                    && connector.permissions().equals(Set.of("crm.customer.read"))
                    && "customer.read".equals(use) && "crm.customer.read".equals(permission);
            case "P7_CRM_WRITE_CONTRACT_FIXTURE" -> "p7-crm-write".equals(connector.credentialRef())
                    && "eaf:p7-crm-write".equals(connector.audience())
                    && connector.allowedUses().equals(Set.of("customer.read", "followup.create", "followup.verify"))
                    && connector.permissions().equals(Set.of("crm.customer.read", "crm.followup.create", "crm.followup.read"))
                    && ("customer.read".equals(use) && "crm.customer.read".equals(permission)
                    || "followup.create".equals(use) && "crm.followup.create".equals(permission)
                    || "followup.verify".equals(use) && "crm.followup.read".equals(permission));
            case "P12_CRM_OUTCOME_FIXTURE" -> "p12-crm-outcome".equals(connector.credentialRef())
                    && "eaf:p12-crm-outcome".equals(connector.audience())
                    && connector.allowedUses().equals(Set.of("followup.result.create", "followup.result.verify"))
                    && connector.permissions().equals(Set.of("crm.followup.result", "crm.followup.read"))
                    && ("followup.result.create".equals(use) && "crm.followup.result".equals(permission)
                    || "followup.result.verify".equals(use) && "crm.followup.read".equals(permission));
            default -> false;
        };
        if (!allowed) throw EafException.conflict("CREDENTIAL_SCOPE_DENIED", "Connector 未登记本次 CRM 最小用途或权限。");
        return credentials.resolve(new CredentialRequest(connector.tenantId(), connector.workspaceId(),
                connector.credentialRef(), connector.audience(), use, permission)).valueForOutboundRequest();
    }

    private boolean validExternalVersion(String version) {
        return version != null && !version.isBlank() && version.length() <= 120
                && version.chars().noneMatch(Character::isISOControl);
    }

    private BoundedHttpResponse sendBounded(HttpRequest request, Instant deadline) throws Exception {
        if (!outboundPermits.tryAcquire()) throw EafException.conflict("CRM_OUTBOUND_CAPACITY", "测试 CRM 出站并发已满。");
        try {
            return BoundedHttpResponse.send(client, request, MAX_RESPONSE_BYTES, deadline, Duration.ofSeconds(10));
        } finally {
            outboundPermits.release();
        }
    }

    private Duration timeout(Instant deadline) { return Duration.ofMillis(Math.min(10_000, Math.max(1, Duration.between(Instant.now(), deadline).toMillis()))); }
}
// 本文件负责实现 EAF 的 TestCrmHttpIntegration.java 相关代码。
