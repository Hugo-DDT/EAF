package io.eaf.workflow.api;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;

/** 工作项只投影本轮明确共享的交接文本、处理结果与流程状态。 */
public record HumanWorkItem(UUID id, UUID instanceId, String requestId, String stepId,
        UUID creatorId, UUID assigneeId, String status, long rowVersion, Instant deadlineAt,
        String sharedBrief, String handlingAdvice, String cautions, String outcome, String summary,
        String nextAction, UUID completedBy, Instant completedAt, String resultSummary,
        String remainingWork, String workflowStatus, long workflowRowVersion, String currentStep,
        String errorCode, Set<String> allowedActions, Instant createdAt, JsonNode teamExperienceUsage) { }
