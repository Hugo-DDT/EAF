package io.eaf.connector.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** 校验 P27 固定绑定和当前 HUMAN 员工映射后访问业务连接。 */
public interface P27BusinessConnectionService {
    MappingSnapshot requireEmployeeMapping(ActorContext actor, UUID workspaceId, String bindingRef);
    String bindingVersion(ActorContext actor, UUID workspaceId, String bindingRef);
    P27BusinessConnectionIntegrationPort.OaTodoPage listTodos(ActorContext actor, UUID workspaceId,
            String bindingRef, String expectedBindingVersion, String status, String cursor, int limit,
            Instant deadline);
    Optional<P27BusinessConnectionIntegrationPort.OaTodo> getTodo(ActorContext actor, UUID workspaceId,
            String bindingRef, String expectedBindingVersion, String todoId, Instant deadline);
    boolean canReadTodo(ActorContext actor, UUID workspaceId, String bindingRef, String expectedBindingVersion,
                        String todoId, Instant deadline);
    P27BusinessConnectionIntegrationPort.ServiceRequestCurrentState readCurrentState(ActorContext actor,
            UUID workspaceId, String bindingRef, String expectedBindingVersion, String requestId,
            Instant deadline);
    boolean canReadServiceRequest(ActorContext actor, UUID workspaceId, String bindingRef,
            String expectedBindingVersion, String requestId, Instant deadline);
    P27BusinessConnectionIntegrationPort.ServiceRequestResultWriteResult recordHandlingResult(ActorContext actor,
            UUID workspaceId, String bindingRef, String expectedBindingVersion,
            P27BusinessConnectionIntegrationPort.ServiceRequestHandlingPayload payload, Instant deadline);
    Optional<P27BusinessConnectionIntegrationPort.ServiceRequestHandlingReceipt> findHandlingResult(
            ActorContext actor, UUID workspaceId, String bindingRef, String expectedBindingVersion,
            String operationId, Instant deadline);

    record MappingSnapshot(UUID connectorId, String externalSubjectId, long rowVersion, String bindingVersion) { }
}
