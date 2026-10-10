package io.eaf.connector.infrastructure;

import io.eaf.connector.api.ConnectorDefinition;
import io.eaf.connector.api.ConnectorService;
import io.eaf.connector.api.P27BusinessConnectionIntegrationPort;
import io.eaf.connector.api.P27BusinessConnectionService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcP27BusinessConnectionService implements P27BusinessConnectionService {
    private static final String OA_BINDING = "p27-oa.todo";
    private static final String SERVICE_BINDING = "p27-service-desk.result";
    private final JdbcTemplate jdbc;
    private final ConnectorService connectors;
    private final P27BusinessConnectionIntegrationPort integration;

    public JdbcP27BusinessConnectionService(JdbcTemplate jdbc, ConnectorService connectors,
            P27BusinessConnectionIntegrationPort integration) {
        this.jdbc = jdbc; this.connectors = connectors; this.integration = integration;
    }

    @Override
    public MappingSnapshot requireEmployeeMapping(ActorContext actor, UUID workspaceId, String bindingRef) {
        requireHuman(actor);
        var connector = requireConnector(actor, workspaceId, bindingRef);
        var mapping = jdbc.query("select external_subject_id, row_version from connector.employee_mapping "
                        + "where tenant_id = ? and workspace_id = ? and connector_id = ? and actor_id = ? and status = 'ACTIVE'",
                rs -> rs.next() ? new Object[]{rs.getString("external_subject_id"), rs.getLong("row_version")} : null,
                actor.tenantId(), workspaceId, connector.id(), actor.actorId());
        if (mapping == null) throw EafException.forbidden("当前员工没有该业务来源的有效身份映射。");
        var externalSubject = (String) mapping[0];
        if (externalSubject == null || !externalSubject.matches("[A-Za-z0-9._:-]{1,160}"))
            throw EafException.conflict("EMPLOYEE_MAPPING_INVALID", "员工映射标识无效。");
        var rowVersion = (Long) mapping[1];
        return new MappingSnapshot(connector.id(), externalSubject, rowVersion,
                Hashing.sha256(connectors.bindingVersion(connector) + "\u001f" + externalSubject + "\u001f" + rowVersion));
    }

    @Override
    public String bindingVersion(ActorContext actor, UUID workspaceId, String bindingRef) {
        var snapshot = requireEmployeeMapping(actor, workspaceId, bindingRef);
        return snapshot.bindingVersion();
    }

    @Override
    public P27BusinessConnectionIntegrationPort.OaTodoPage listTodos(ActorContext actor, UUID workspaceId,
            String bindingRef, String expectedBindingVersion, String status, String cursor, int limit, Instant deadline) {
        var snapshot = current(actor, workspaceId, bindingRef, expectedBindingVersion);
        return integration.listTodos(requireConnector(actor, workspaceId, bindingRef), snapshot.externalSubjectId(),
                status, cursor, limit, deadline);
    }

    @Override
    public Optional<P27BusinessConnectionIntegrationPort.OaTodo> getTodo(ActorContext actor, UUID workspaceId,
            String bindingRef, String expectedBindingVersion, String todoId, Instant deadline) {
        var snapshot = current(actor, workspaceId, bindingRef, expectedBindingVersion);
        return integration.getTodo(requireConnector(actor, workspaceId, bindingRef), snapshot.externalSubjectId(), todoId, deadline);
    }

    @Override
    public boolean canReadTodo(ActorContext actor, UUID workspaceId, String bindingRef, String expectedBindingVersion,
                               String todoId, Instant deadline) {
        var snapshot = current(actor, workspaceId, bindingRef, expectedBindingVersion);
        return integration.canReadTodos(requireConnector(actor, workspaceId, bindingRef), snapshot.externalSubjectId(), todoId, deadline);
    }

    @Override
    public P27BusinessConnectionIntegrationPort.ServiceRequestCurrentState readCurrentState(ActorContext actor,
            UUID workspaceId, String bindingRef, String expectedBindingVersion, String requestId, Instant deadline) {
        var snapshot = current(actor, workspaceId, bindingRef, expectedBindingVersion);
        return integration.readCurrentState(requireConnector(actor, workspaceId, bindingRef), snapshot.externalSubjectId(), requestId, deadline);
    }

    @Override
    public boolean canReadServiceRequest(ActorContext actor, UUID workspaceId, String bindingRef,
            String expectedBindingVersion, String requestId, Instant deadline) {
        var snapshot = current(actor, workspaceId, bindingRef, expectedBindingVersion);
        return integration.canReadServiceRequest(requireConnector(actor, workspaceId, bindingRef),
                snapshot.externalSubjectId(), requestId, deadline);
    }

    @Override
    public P27BusinessConnectionIntegrationPort.ServiceRequestResultWriteResult recordHandlingResult(ActorContext actor,
            UUID workspaceId, String bindingRef, String expectedBindingVersion,
            P27BusinessConnectionIntegrationPort.ServiceRequestHandlingPayload payload, Instant deadline) {
        final MappingSnapshot snapshot;
        try { snapshot = current(actor, workspaceId, bindingRef, expectedBindingVersion); }
        catch (EafException denied) {
            return P27BusinessConnectionIntegrationPort.ServiceRequestResultWriteResult.rejected(
                    denied.code(), "外部写入前员工映射或连接绑定检查失败。");
        }
        if (!snapshot.externalSubjectId().equals(payload.externalSubjectId()))
            return P27BusinessConnectionIntegrationPort.ServiceRequestResultWriteResult.rejected(
                    "EMPLOYEE_MAPPING_CHANGED", "审批后外部员工映射已变化。");
        return integration.recordHandlingResult(requireConnector(actor, workspaceId, bindingRef),
                snapshot.externalSubjectId(), payload, deadline);
    }

    @Override
    public Optional<P27BusinessConnectionIntegrationPort.ServiceRequestHandlingReceipt> findHandlingResult(
            ActorContext actor, UUID workspaceId, String bindingRef, String expectedBindingVersion,
            String operationId, Instant deadline) {
        var snapshot = current(actor, workspaceId, bindingRef, expectedBindingVersion);
        return integration.findHandlingResult(requireConnector(actor, workspaceId, bindingRef),
                snapshot.externalSubjectId(), operationId, deadline);
    }

    private MappingSnapshot current(ActorContext actor, UUID workspaceId, String bindingRef, String expectedVersion) {
        var snapshot = requireEmployeeMapping(actor, workspaceId, bindingRef);
        if (expectedVersion != null && !expectedVersion.equals(snapshot.bindingVersion()))
            throw EafException.conflict("CONNECTOR_BINDING_CHANGED", "审批或查询期间业务绑定/员工映射已变化。");
        return snapshot;
    }

    private ConnectorDefinition requireConnector(ActorContext actor, UUID workspaceId, String bindingRef) {
        var expected = switch (bindingRef == null ? "" : bindingRef) {
            case OA_BINDING -> "P27_OA_TODO_FIXTURE";
            case SERVICE_BINDING -> "P27_SERVICE_DESK_RESULT_FIXTURE";
            default -> null;
        };
        if (expected == null) throw EafException.forbidden("P27 不接受动态业务连接绑定。");
        var connector = connectors.requireActiveForTool(actor.tenantId(), workspaceId, bindingRef);
        if (!expected.equals(connector.provider())) throw EafException.forbidden("P27 连接绑定与固定业务来源不匹配。");
        return connector;
    }

    private void requireHuman(ActorContext actor) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("P27 业务连接只接受本人直接操作的 HUMAN 身份。");
    }
}
