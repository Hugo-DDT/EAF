package io.eaf.connector.infrastructure;

import io.eaf.connector.api.ConnectorDefinition;
import io.eaf.connector.api.ConnectorDisableReceipt;
import io.eaf.connector.api.ConnectorService;
import io.eaf.connector.api.A2aIntegrationPort;
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
import io.eaf.connector.api.RemoteA2aResult;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.time.Instant;
import java.time.Clock;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcConnectorService implements ConnectorService {
    private final JdbcTemplate jdbc;
    private final IntegrationPort integration;
    private final ServiceRequestIntegrationPort serviceRequests;
    private final A2aIntegrationPort a2a;
    private final WorkspaceAuthorization authorization;
    private final AuditPort audit;
    private final Clock clock;

    public JdbcConnectorService(JdbcTemplate jdbc, IntegrationPort integration, ServiceRequestIntegrationPort serviceRequests,
                                A2aIntegrationPort a2a,
                                WorkspaceAuthorization authorization, AuditPort audit, Clock clock) {
        this.jdbc = jdbc;
        this.integration = integration;
        this.serviceRequests = serviceRequests;
        this.a2a = a2a;
        this.authorization = authorization;
        this.audit = audit;
        this.clock = clock;
    }

    @Override
    @Transactional
    public ConnectorDisableReceipt disable(ActorContext actor, UUID workspaceId, UUID connectorId,
                                           String requestKey, String reason) {
        var normalizedReason = reason == null ? null : reason.strip();
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("Connector 停用只允许本人 HUMAN 操作者执行。");
        if (connectorId == null || requestKey == null || requestKey.isBlank() || requestKey.length() > 200
                || normalizedReason == null || normalizedReason.isBlank() || normalizedReason.length() > 500)
            throw EafException.invalid("Connector 停用需要目标连接、请求键和 1 至 500 字符的原因。");

        authorization.require(actor, workspaceId, "connector:disable");
        // 目标行锁使并发停用按该 Connector 的状态变化顺序提交，并隐藏跨 Workspace 目标是否存在。
        var target = jdbc.query("select status, row_version from connector.instance "
                        + "where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? new DisableTarget(rs.getString("status"), rs.getLong("row_version")) : null,
                connectorId, actor.tenantId(), workspaceId);
        if (target == null) throw EafException.notFound();

        var keyHash = Hashing.sha256(String.join("\u001f", actor.tenantId().toString(), workspaceId.toString(), requestKey));
        var requestHash = Hashing.sha256(String.join("\u001f", actor.actorId().toString(), connectorId.toString(), normalizedReason));
        var commandId = UUID.randomUUID();
        var now = Instant.now(clock);
        var inserted = jdbc.update("insert into connector.operations_command(command_id, tenant_id, workspace_id, connector_id, "
                        + "actor_id, request_key_hash, request_hash, reason, result_status, applied_version, created_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, 'DISABLED', 0, ?) on conflict (tenant_id, workspace_id, request_key_hash) do nothing",
                commandId, actor.tenantId(), workspaceId, connectorId, actor.actorId(), keyHash, requestHash,
                normalizedReason, Timestamp.from(now));
        if (inserted == 0) return replayDisable(actor, workspaceId, connectorId, keyHash, requestHash);

        var status = target.status();
        var version = target.version();
        if ("ACTIVE".equals(status)) {
            // 停用仅能把 ACTIVE 改为 DISABLED；REVOKED 是更强状态，不允许运维命令回退。
            status = "DISABLED";
            version = jdbc.queryForObject("update connector.instance set status = 'DISABLED', row_version = row_version + 1 "
                            + "where id = ? and tenant_id = ? and workspace_id = ? and status = 'ACTIVE' returning row_version",
                    Long.class, connectorId, actor.tenantId(), workspaceId);
        }
        jdbc.update("update connector.operations_command set result_status = ?, applied_version = ? where command_id = ?",
                status, version, commandId);
        audit.append(new AuditFact("connector-ops:" + commandId, actor.tenantId(), workspaceId, actor.actorId(), null,
                "CONNECTOR_DISABLED", status, "{\"connectorId\":\"" + connectorId + "\",\"version\":"
                        + version + ",\"reasonHash\":\""
                        + Hashing.sha256(normalizedReason) + "\"}", null));
        return new ConnectorDisableReceipt(commandId, connectorId, status, version, now, false);
    }

    private ConnectorDisableReceipt replayDisable(ActorContext actor, UUID workspaceId, UUID connectorId,
                                                  String keyHash, String requestHash) {
        var prior = jdbc.query("select command_id, connector_id, actor_id, request_hash, result_status, applied_version, created_at "
                        + "from connector.operations_command where tenant_id = ? and workspace_id = ? and request_key_hash = ?",
                rs -> rs.next() ? new DisableCommand(rs.getObject("command_id", UUID.class),
                        rs.getObject("connector_id", UUID.class), rs.getObject("actor_id", UUID.class),
                        rs.getString("request_hash"), rs.getString("result_status"), rs.getLong("applied_version"),
                        rs.getTimestamp("created_at").toInstant()) : null,
                actor.tenantId(), workspaceId, keyHash);
        if (prior == null) throw EafException.conflict("CONNECTOR_COMMAND_CONFLICT", "Connector 请求键已被并发命令占用。");
        if (!prior.connectorId().equals(connectorId) || !actor.actorId().equals(prior.actorId())
                || !requestHash.equals(prior.requestHash()))
            throw EafException.conflict("CONNECTOR_COMMAND_CONFLICT", "同一 Connector 请求键不能绑定不同目标、操作者或原因。");
        return new ConnectorDisableReceipt(prior.commandId(), prior.connectorId(), prior.status(),
                prior.version(), prior.createdAt(), true);
    }

    @Override
    public ConnectorDefinition requireActive(UUID tenantId, UUID workspaceId) {
        return requireActive(tenantId, workspaceId, "TEST_CRM");
    }

    @Override
    public ConnectorDefinition requireActive(UUID tenantId, UUID workspaceId, String provider) {
        if (provider == null || provider.isBlank()) throw EafException.invalid("Connector provider 无效。");
        return requireActiveInternal(tenantId, workspaceId, provider);
    }

    @Override
    public ConnectorDefinition requireActiveForTool(UUID tenantId, UUID workspaceId, String bindingRef) {
        // 只接受版本化 Tool 中的固定绑定引用；请求参数不能选择 Provider 或地址。
        return requireActiveInternal(tenantId, workspaceId, providerForBinding(bindingRef));
    }

    @Override
    public String bindingVersion(ConnectorDefinition connector) {
        if (connector == null) throw EafException.invalid("Connector 快照缺失。");
        var canonical = String.join("\u001f", connector.provider(), connector.baseUrl(), connector.credentialRef(),
                connector.audience(), String.join(",", connector.allowedUses().stream().sorted().toList()),
                String.join(",", connector.permissions().stream().sorted().toList()));
        return Hashing.sha256(canonical);
    }

    @Override
    public CustomerRecord readCustomer(UUID tenantId, UUID workspaceId, String customerId, Instant deadline) {
        return readCustomer(tenantId, workspaceId, "test-crm.customer-read", customerId, deadline);
    }

    @Override
    public CustomerRecord readCustomer(UUID tenantId, UUID workspaceId, String bindingRef, String customerId, Instant deadline) {
        // Provider 选择只接受已发布 Tool 的固定 bindingRef，不从 Tool 参数读取 endpoint 或 Provider。
        var provider = switch (bindingRef == null ? "" : bindingRef) {
            case "test-crm.customer-read" -> "TEST_CRM";
            case "p7-crm-read-contract.customer-read" -> "P7_CRM_READ_CONTRACT_FIXTURE";
            case "p7-crm-write-contract.customer-read" -> "P7_CRM_WRITE_CONTRACT_FIXTURE";
            default -> throw EafException.conflict("CONNECTOR_UNAVAILABLE", "客户读取 Tool 没有已登记的连接绑定。");
        };
        var connector = requireActiveInternal(tenantId, workspaceId, provider);
        var result = integration.readCustomer(connector, customerId, deadline);
        if (result == null || !customerId.equals(result.customerId())) throw EafException.conflict("INVALID_TOOL_RESULT", "CRM 返回的客户归属不匹配。");
        return result;
    }

    @Override
    public CustomerRecord readCustomerForEvaluation(UUID tenantId, UUID workspaceId, String customerId, Instant readAt) {
        if (customerId == null || customerId.isBlank() || customerId.length() > 160)
            throw EafException.invalid("评测样本 customerId 无效。");
        // 评测数据固定为合成记录，任何 ID 都不会被用于查询常规 CRM 连接。
        return new CustomerRecord(customerId, "ACTIVE", "2026-01-01", "合成评测样本：无投诉",
                "evaluation-sandbox:" + customerId, "evaluation-sandbox-v1", readAt);
    }

    @Override
    public ExternalWriteResult createFollowup(UUID tenantId, UUID workspaceId, String operationId, String customerId,
                                              String summary, String ownerId, Instant deadline) {
        return createFollowup(tenantId, workspaceId, "test-crm.followup-create", null,
                operationId, customerId, summary, ownerId, deadline);
    }

    @Override
    public ExternalWriteResult createFollowup(UUID tenantId, UUID workspaceId, String bindingRef, String expectedBindingVersion,
                                              String operationId, String customerId, String summary, String ownerId, Instant deadline) {
        var connector = requireActiveForTool(tenantId, workspaceId, bindingRef);
        if (expectedBindingVersion != null && !expectedBindingVersion.equals(bindingVersion(connector)))
            return ExternalWriteResult.rejected("CONNECTOR_BINDING_CHANGED", "审批后 CRM 目标或权限快照已变化。" );
        return integration.createFollowup(connector, operationId, customerId, summary, ownerId, deadline);
    }

    @Override
    public java.util.Optional<FollowupRecord> findFollowup(UUID tenantId, UUID workspaceId, String operationId, Instant deadline) {
        return findFollowup(tenantId, workspaceId, "test-crm.followup-create", null, operationId, deadline);
    }

    @Override
    public java.util.Optional<FollowupRecord> findFollowup(UUID tenantId, UUID workspaceId, String bindingRef,
                                                          String expectedBindingVersion, String operationId, Instant deadline) {
        var connector = requireActiveForTool(tenantId, workspaceId, bindingRef);
        if (expectedBindingVersion != null && !expectedBindingVersion.equals(bindingVersion(connector)))
            throw EafException.conflict("CONNECTOR_BINDING_CHANGED", "审批后 CRM 目标或权限快照已变化，拒绝核验其他目标。" );
        return integration.findFollowup(connector, operationId, deadline);
    }

    @Override
    public ExternalOutcomeWriteResult recordFollowupOutcome(UUID tenantId, UUID workspaceId, String bindingRef,
            String expectedBindingVersion, String operationId, FollowupOutcomeRecord outcome, Instant deadline) {
        var connector = requireActiveForTool(tenantId, workspaceId, bindingRef);
        if (expectedBindingVersion != null && !expectedBindingVersion.equals(bindingVersion(connector)))
            return ExternalOutcomeWriteResult.rejected("CONNECTOR_BINDING_CHANGED", "审批后结果 Connector 目标或用途快照已变化。");
        return integration.recordFollowupOutcome(connector, operationId, outcome, deadline);
    }

    @Override
    public java.util.Optional<FollowupOutcomeRecord> findFollowupOutcome(UUID tenantId, UUID workspaceId,
            String bindingRef, String expectedBindingVersion, String operationId, Instant deadline) {
        var connector = requireActiveForTool(tenantId, workspaceId, bindingRef);
        if (expectedBindingVersion != null && !expectedBindingVersion.equals(bindingVersion(connector)))
            throw EafException.conflict("CONNECTOR_BINDING_CHANGED", "结果核验目标与获批快照不一致。");
        return integration.findFollowupOutcome(connector, operationId, deadline);
    }

    @Override
    public ServiceRequestWriteResult registerServiceRequest(UUID tenantId, UUID workspaceId, String bindingRef,
            String expectedBindingVersion, ServiceRequestPayload payload, Instant deadline) {
        var connector = requireActiveForTool(tenantId, workspaceId, bindingRef);
        if (expectedBindingVersion != null && !expectedBindingVersion.equals(bindingVersion(connector)))
            return ServiceRequestWriteResult.rejected("CONNECTOR_BINDING_CHANGED", "审批后服务台目标或权限快照已变化。");
        return serviceRequests.register(connector, payload, deadline);
    }

    @Override
    public java.util.Optional<ServiceRequestReceipt> findServiceRequest(UUID tenantId, UUID workspaceId,
            String bindingRef, String expectedBindingVersion, String operationId, Instant deadline) {
        var connector = requireActiveForTool(tenantId, workspaceId, bindingRef);
        if (expectedBindingVersion != null && !expectedBindingVersion.equals(bindingVersion(connector)))
            throw EafException.conflict("CONNECTOR_BINDING_CHANGED", "服务请求核验目标与获批快照不一致。");
        return serviceRequests.find(connector, operationId, deadline);
    }

    private String providerForBinding(String bindingRef) {
        return switch (bindingRef == null ? "" : bindingRef) {
            case "test-crm.followup-create" -> "TEST_CRM";
            case "p7-crm-write-contract.followup-create" -> "P7_CRM_WRITE_CONTRACT_FIXTURE";
            case "p7-crm-write-contract.followup-result" -> "P12_CRM_OUTCOME_FIXTURE";
            case "p15-service-desk.register" -> "P15_INTERNAL_SERVICE_DESK_FIXTURE";
            default -> throw EafException.conflict("CONNECTOR_UNAVAILABLE", "写入 Tool 没有已登记的 CRM 绑定。" );
        };
    }

    @Override
    public RemoteA2aResult sendA2aTask(UUID tenantId, UUID workspaceId, UUID connectorId, String rpcId,
                                      String skillId, String messageId, String text, Instant deadline) {
        var connector = requireA2aConnector(tenantId, workspaceId, connectorId, "a2a.send");
        return a2a.sendTask(connector, rpcId, skillId, messageId, text, deadline);
    }

    @Override
    public RemoteA2aResult getA2aTask(UUID tenantId, UUID workspaceId, UUID connectorId, String rpcId,
                                     String remoteTaskId, Instant deadline) {
        var connector = requireA2aConnector(tenantId, workspaceId, connectorId, "a2a.get");
        return a2a.getTask(connector, rpcId, remoteTaskId, deadline);
    }

    @Override
    public RemoteA2aResult cancelA2aTask(UUID tenantId, UUID workspaceId, UUID connectorId, String rpcId,
                                         String remoteTaskId, Instant deadline) {
        var connector = requireA2aConnector(tenantId, workspaceId, connectorId, "a2a.cancel");
        return a2a.cancelTask(connector, rpcId, remoteTaskId, deadline);
    }

    private ConnectorDefinition requireA2aConnector(UUID tenantId, UUID workspaceId, UUID connectorId, String use) {
        var connector = jdbc.query("select id, tenant_id, workspace_id, provider, base_url, status, credential_ref, audience, allowed_uses, permissions "
                        + "from connector.instance where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? mapConnector(rs) : null, connectorId, tenantId, workspaceId);
        var permission = connector == null ? null : switch (connector.provider()) {
            case "A2A_REVIEW_PEER" -> "agent:risk-review";
            case "A2A_EVALUATION_REVIEW_PEER" -> "agent:risk-review-evaluation";
            default -> null;
        };
        // 只允许两个固定 peer profile；评测 Connector 默认禁用且不能借旧 USER 凭据完成出站。
        if (connector == null || permission == null || !"ACTIVE".equals(connector.status())
                || !connector.allowedUses().contains(use) || !connector.permissions().equals(Set.of(permission)))
            throw EafException.conflict("CONNECTOR_SCOPE_DENIED", "A2A Connector 不匹配已登记 reviewer 的固定权限。");
        return connector;
    }

    private ConnectorDefinition requireActiveInternal(UUID tenantId, UUID workspaceId, String provider) {
        var connector = jdbc.query("select id, tenant_id, workspace_id, provider, base_url, status, credential_ref, audience, allowed_uses, permissions from connector.instance where tenant_id = ? and workspace_id = ? and provider = ?",
                rs -> rs.next() ? mapConnector(rs) : null,
                tenantId, workspaceId, provider);
        if (connector == null || !"ACTIVE".equals(connector.status()))
            throw EafException.conflict("CONNECTOR_UNAVAILABLE", "已登记的外部连接不可用。");
        return connector;
    }

    private ConnectorDefinition mapConnector(ResultSet rs) throws java.sql.SQLException {
        return new ConnectorDefinition(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getString("provider"), rs.getString("base_url"),
                rs.getString("status"), rs.getString("credential_ref"), rs.getString("audience"),
                readSet(rs, "allowed_uses"), readSet(rs, "permissions"));
    }

    private record DisableTarget(String status, long version) { }
    private record DisableCommand(UUID commandId, UUID connectorId, UUID actorId, String requestHash,
                                  String status, long version, Instant createdAt) { }

    // PostgreSQL 数组用于持久化有限用途和权限；映射完成后立即释放 JDBC 资源。
    private Set<String> readSet(ResultSet rs, String column) throws java.sql.SQLException {
        var array = rs.getArray(column);
        if (array == null) return Set.of();
        try { return Set.copyOf(Arrays.asList((String[]) array.getArray())); }
        finally { array.free(); }
    }
}
// 本文件负责实现 EAF 的 JdbcConnectorService.java 相关代码。
