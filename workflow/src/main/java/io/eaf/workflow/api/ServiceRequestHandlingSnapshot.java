package io.eaf.workflow.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

/** 只暴露交接流程状态与明确提交的业务结果。 */
public record ServiceRequestHandlingSnapshot(UUID instanceId, long rowVersion, String requestId,
        String workflowStatus, String currentStepId, Instant deadlineAt, UUID workItemId,
        String outcome, String summary, String nextAction, UUID completedBy, Instant completedAt,
        String resultSummary, String remainingWork, String errorCode, JsonNode teamExperienceUsage) { }
