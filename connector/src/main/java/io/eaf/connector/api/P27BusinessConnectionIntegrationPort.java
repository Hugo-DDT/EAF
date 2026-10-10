package io.eaf.connector.api;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** P27 loopback 合成来源的固定业务合同。 */
public interface P27BusinessConnectionIntegrationPort {
    OaTodoPage listTodos(ConnectorDefinition connector, String externalSubjectId, String status,
                         String cursor, int limit, Instant deadline);
    Optional<OaTodo> getTodo(ConnectorDefinition connector, String externalSubjectId,
                             String todoId, Instant deadline);
    boolean canReadTodos(ConnectorDefinition connector, String externalSubjectId, String todoId,
                         Instant deadline);
    ServiceRequestCurrentState readCurrentState(ConnectorDefinition connector, String externalSubjectId,
                                                 String requestId, Instant deadline);
    boolean canReadServiceRequest(ConnectorDefinition connector, String externalSubjectId,
                                 String requestId, Instant deadline);
    ServiceRequestResultWriteResult recordHandlingResult(ConnectorDefinition connector, String externalSubjectId,
            ServiceRequestHandlingPayload payload, Instant deadline);
    Optional<ServiceRequestHandlingReceipt> findHandlingResult(ConnectorDefinition connector,
            String externalSubjectId, String operationId, Instant deadline);

    record OaTodo(String sourceId, String todoId, String title, String status, String dueAt,
                  String sourceVersion, Instant updatedAt, Instant fetchedAt) { }
    record OaTodoPage(List<OaTodo> items, String nextCursor, Instant fetchedAt) {
        public OaTodoPage { items = items == null ? List.of() : List.copyOf(items); }
    }
    record ServiceRequestCurrentState(String sourceId, String requestId, String registrationOperationId,
            String status, String externalVersion, Instant updatedAt, Instant fetchedAt) { }
    record ServiceRequestHandlingPayload(String operationId, String requestId, String registrationOperationId,
            String workItemId, long workItemVersion, String sourceResultHash, String externalSubjectId,
            String completedBy, Instant completedAt, String outcome, String summary, String nextAction,
            String expectedExternalVersion) { }
    record ServiceRequestHandlingReceipt(String operationId, String requestId, String registrationOperationId, String resultId,
            String workItemId, long workItemVersion, String sourceResultHash, String externalSubjectId,
            String completedBy, Instant completedAt, String outcome, String summary, String nextAction,
            String recordState, String previousExternalVersion, String resultingExternalVersion,
            String resultingStatus, Instant acceptedAt) { }
    record ServiceRequestResultWriteResult(String state, String errorCode, String detail,
            ServiceRequestHandlingReceipt receipt) {
        public static ServiceRequestResultWriteResult accepted(ServiceRequestHandlingReceipt receipt) {
            return new ServiceRequestResultWriteResult("ACCEPTED", null, null, receipt);
        }
        public static ServiceRequestResultWriteResult unknown(String code, String detail) {
            return new ServiceRequestResultWriteResult("UNKNOWN", code, detail, null);
        }
        public static ServiceRequestResultWriteResult rejected(String code, String detail) {
            return new ServiceRequestResultWriteResult("REJECTED", code, detail, null);
        }
    }
}
