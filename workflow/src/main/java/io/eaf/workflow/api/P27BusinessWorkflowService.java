package io.eaf.workflow.api;

import com.fasterxml.jackson.databind.JsonNode;
import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Fixed P27 OA queries and approved service desk result syncs. */
public interface P27BusinessWorkflowService {
    WorkflowInstance createOaTodoListQuery(ActorContext actor, UUID workspaceId, String status,
            String cursor, Integer limit, String idempotencyKey);
    WorkflowInstance createOaTodoItemQuery(ActorContext actor, UUID workspaceId, String todoId,
            String idempotencyKey);
    WorkflowInstance createServiceRequestStateQuery(ActorContext actor, UUID workspaceId, UUID workItemId,
            String idempotencyKey);
    ResultSyncSnapshot createResultSync(ActorContext actor, UUID workspaceId, UUID workItemId,
            long expectedWorkItemVersion, UUID stateQueryId, String idempotencyKey);
    QuerySnapshot getQuery(ActorContext actor, UUID workspaceId, UUID queryId);
    QuerySnapshot getServiceRequestStateQuery(ActorContext actor, UUID workspaceId, UUID workItemId, UUID queryId);
    ResultSyncSnapshot getResultSync(ActorContext actor, UUID workspaceId, UUID workItemId, UUID syncId);
    ResultSyncPage listResultSyncs(ActorContext actor, UUID workspaceId, UUID workItemId,
            Instant cursorCreatedAt, UUID cursorSyncId, int pageSize);
    ResultSyncSnapshot verifyResultSync(ActorContext actor, UUID workspaceId, UUID workItemId,
            UUID syncId, String requestKey, String reason);

    record QuerySnapshot(UUID id, String status, UUID taskId, JsonNode result, String errorCode,
            long rowVersion, Instant updatedAt) { }
    record ResultSyncSnapshot(UUID id, UUID workItemId, String sourceResultHash, UUID workflowInstanceId,
            String workflowStatus, UUID taskId, String taskStatus, UUID executionId, UUID approvalId,
            String executionStatus, String syncStatus, String externalStatus, String errorCode,
            JsonNode result, List<String> allowedActions, long rowVersion) {
        public ResultSyncSnapshot { allowedActions = allowedActions == null ? List.of() : List.copyOf(allowedActions); }
    }
    record ResultSyncPage(List<ResultSyncSnapshot> items, Instant nextCreatedAt, UUID nextSyncId) {
        public ResultSyncPage { items = items == null ? List.of() : List.copyOf(items); }
    }
}
