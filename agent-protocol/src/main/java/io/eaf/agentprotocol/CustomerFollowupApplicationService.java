package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.task.api.CustomerFollowupService;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.api.WorkflowInstance;
import io.eaf.execution.api.ExecutionService;
import io.eaf.execution.api.ExecutionSnapshot;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 组合私有客户会话、原跟进审批流程和 Task 域团队业务记录。 */
@Service
public class CustomerFollowupApplicationService {
    private static final UUID CUSTOMER_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-00000000000c");
    private static final UUID CUSTOMER_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-00000000000d");
    private final CustomerFollowupService followups;
    private final TaskService tasks;
    private final TaskApplicationService taskApplication;
    private final ObjectMapper json;
    private final WorkflowService workflows;
    private final ExecutionService executions;

    public CustomerFollowupApplicationService(CustomerFollowupService followups, TaskService tasks,
                                              TaskApplicationService taskApplication, ObjectMapper json,
                                              WorkflowService workflows, ExecutionService executions) {
        this.followups = followups;
        this.tasks = tasks;
        this.taskApplication = taskApplication;
        this.json = json;
        this.workflows = workflows;
        this.executions = executions;
    }

    @Transactional
    public CreateResponse create(ActorContext actor, UUID workspaceId, CreateRequest request, String requestKey) {
        requireDirectHuman(actor);
        if (request == null || request.conversationId() == null || request.sourceTaskId() == null
                || request.expectedTaskVersion() < 1 || request.expectedBriefRevision() < 0)
            throw EafException.invalid("来源会话、成功分析 Task、版本和简报修订必须提供。");
        var source = tasks.get(actor, workspaceId, request.sourceTaskId());
        requireCustomerAnalysis(source, actor);
        var binding = tasks.conversationBinding(actor, workspaceId, source.id());
        if (binding == null || !request.conversationId().equals(binding.conversationId())
                || !"CUSTOMER_ASSISTANT".equals(binding.mode()))
            throw EafException.conflict("FOLLOWUP_SOURCE_INVALID", "团队跟进必须来自本人绑定客户会话中的分析。");
        var conversation = tasks.getConversation(actor, workspaceId, binding.conversationId());
        var customerId = customerId(source);
        if (!customerId.equals(conversation.customerId()))
            throw EafException.conflict("FOLLOWUP_SOURCE_INVALID", "分析草稿客户与会话绑定不一致。");
        var card = followups.create(new CustomerFollowupService.CreateCardCommand(actor, workspaceId, customerId,
                binding.conversationId(), source.id(), request.expectedTaskVersion(), request.expectedBriefRevision(),
                request.assigneeId(), request.summary(), request.dueAt(), requestKey));
        var workflow = taskApplication.confirmFollowup(actor, workspaceId, source.id(),
                request.expectedTaskVersion(), request.summary(), requestKey, binding.conversationId(),
                request.expectedBriefRevision());
        if (card.creationWorkflowId() != null && !card.creationWorkflowId().equals(workflow.instanceId()))
            throw EafException.conflict("FOLLOWUP_WORKFLOW_CONFLICT", "幂等请求返回的跟进卡片与创建流程不匹配。");
        card = followups.attachCreationWorkflow(actor, workspaceId, card.id(), workflow.instanceId());
        return new CreateResponse(card, workflow, true);
    }

    public CustomerFollowupService.FollowupPage list(ActorContext actor, UUID workspaceId,
            String scope, String customerId, UUID assigneeId, String status, Boolean overdue,
            Instant cursorUpdatedAt, UUID cursorId, int limit) {
        return followups.list(actor, workspaceId, new CustomerFollowupService.ListQuery(scope, customerId,
                assigneeId, status, overdue, cursorUpdatedAt, cursorId, limit));
    }

    public CustomerFollowupService.FollowupDetail get(ActorContext actor, UUID workspaceId, UUID followupId) {
        return followups.get(actor, workspaceId, followupId);
    }

    public CustomerFollowupService.ResultPage results(ActorContext actor, UUID workspaceId, UUID followupId,
                                                        Integer beforeResultNo, int limit) {
        return followups.results(actor, workspaceId, followupId, beforeResultNo, limit);
    }

    public CustomerFollowupService.CardSnapshot update(ActorContext actor, UUID workspaceId, UUID followupId,
            long expectedVersion, UUID assigneeId, boolean assigneeProvided, Instant dueAt, boolean dueAtProvided) {
        return followups.update(new CustomerFollowupService.UpdateCardCommand(actor, workspaceId, followupId,
                expectedVersion, assigneeId, assigneeProvided, dueAt, dueAtProvided));
    }

    public CustomerFollowupService.ResultReceipt appendResult(ActorContext actor, UUID workspaceId, UUID followupId,
            ResultRequest request, String requestKey) {
        if (request == null) throw EafException.invalid("结果请求体必填。");
        return followups.appendResult(new CustomerFollowupService.ResultCommand(actor, workspaceId, followupId,
                request.expectedVersion(), request.outcomeCode(), request.summary(), request.nextAction(),
                request.nextContactAt(), request.disposition(), request.correctsResultId(), requestKey));
    }

    public List<CustomerFollowupService.Assignee> assignees(ActorContext actor, UUID workspaceId,
                                                             String customerId, int limit) {
        return followups.eligibleAssignees(actor, workspaceId, customerId, limit);
    }

    @Transactional
    public SyncProjection startSync(ActorContext actor, UUID workspaceId, UUID followupId,
            UUID resultId, String requestKey) {
        var card = followups.get(actor, workspaceId, followupId).card();
        var sync = followups.startSync(new CustomerFollowupService.SyncCommand(actor, workspaceId,
                followupId, resultId, requestKey));
        var workflow = sync.workflowInstanceId() == null
                ? workflows.createCustomerFollowupResultWorkflow(actor, workspaceId, followupId, resultId,
                        "p12-result-sync:" + sync.id())
                : workflows.getInstance(actor, workspaceId, sync.workflowInstanceId());
        if (sync.workflowInstanceId() == null)
            sync = followups.attachSyncWorkflow(actor, workspaceId, sync.id(), workflow.id());
        return projection(actor, workspaceId, card.id(), resultId, sync, workflow);
    }

    public SyncProjection syncStatus(ActorContext actor, UUID workspaceId, UUID followupId, UUID resultId) {
        var detail = followups.get(actor, workspaceId, followupId);
        if (detail.recentResults().stream().noneMatch(result -> result.id().equals(resultId)))
            followups.results(actor, workspaceId, followupId, null, 50);
        var sync = followups.syncAttempts(actor, workspaceId, followupId).stream()
                .filter(attempt -> attempt.resultId().equals(resultId))
                .max(java.util.Comparator.comparingInt(CustomerFollowupService.SyncAttempt::attemptNo)).orElse(null);
        if (sync == null) throw EafException.notFound();
        if (sync.workflowInstanceId() == null) return new SyncProjection(sync, null, null, null);
        var workflow = workflows.getInstance(actor, workspaceId, sync.workflowInstanceId());
        return projection(actor, workspaceId, followupId, resultId, sync, workflow);
    }

    public SyncProjection verifySync(ActorContext actor, UUID workspaceId, UUID followupId, UUID resultId,
            String requestKey, String reason) {
        var card = followups.get(actor, workspaceId, followupId);
        if (!card.allowedActions().contains("SYNC_RESULT"))
            throw EafException.forbidden("只有当前负责人可以核验此结果同步。");
        var current = syncStatus(actor, workspaceId, followupId, resultId);
        var execution = current.execution();
        if (execution == null) return current;
        if ("UNKNOWN".equals(execution.status()) || "VERIFICATION_FAILED".equals(execution.status())) {
            executions.verifyOperational(actor, workspaceId, execution.id(), requestKey, reason);
        }
        return syncStatus(actor, workspaceId, followupId, resultId);
    }

    private SyncProjection projection(ActorContext actor, UUID workspaceId, UUID followupId, UUID resultId,
            CustomerFollowupService.SyncAttempt sync, WorkflowInstance workflow) {
        ExecutionSnapshot execution = null;
        var taskId = workflow.rootTaskId() == null ? workflow.childTaskId() : workflow.rootTaskId();
        if (taskId != null) execution = executions.findForTask(actor, workspaceId, taskId).orElse(null);
        return new SyncProjection(sync, workflow.status(), workflow.currentStepId(), execution);
    }

    private void requireCustomerAnalysis(TaskSnapshot task, ActorContext actor) {
        if (task == null || task.status() != TaskStatus.SUCCEEDED || !"USER".equals(task.source())
                || !CUSTOMER_AGENT_ID.equals(task.agentId())
                || !Set.of("1.0.0", "1.1.0", "1.2.0").contains(task.agentVersion())
                || !actor.actorId().equals(task.actorId()))
            throw EafException.conflict("FOLLOWUP_SOURCE_INVALID", "只能共享本人客户分析中的跟进摘要。");
        var binding = task.assetBinding();
        if (binding == null || !CUSTOMER_CAPABILITY_ID.equals(binding.capabilityId())
                || !task.agentVersion().equals(binding.capabilityVersion()))
            throw EafException.conflict("FOLLOWUP_SOURCE_INVALID", "客户分析没有匹配的固定 Capability 版本。");
    }

    private String customerId(TaskSnapshot task) {
        JsonNode result;
        try { result = task.resultJson() == null ? null : json.readTree(task.resultJson()); }
        catch (Exception invalid) { throw EafException.conflict("FOLLOWUP_DRAFT_UNAVAILABLE", "分析结果无法读取。"); }
        var value = result.path("followupDraft").path("customerId");
        if (!value.isTextual() || value.asText().isBlank() || value.asText().length() > 160)
            throw EafException.conflict("FOLLOWUP_DRAFT_UNAVAILABLE", "分析结果没有可提交的已绑定客户草稿。");
        return value.asText();
    }

    private void requireDirectHuman(ActorContext actor) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("团队跟进创建只接受本人直接操作的 HUMAN 身份。");
    }

    public record CreateRequest(UUID conversationId, UUID sourceTaskId, long expectedTaskVersion,
                                int expectedBriefRevision, String summary, UUID assigneeId, Instant dueAt) { }
    public record ResultRequest(long expectedVersion, String outcomeCode, String summary, String nextAction,
                                Instant nextContactAt, String disposition, UUID correctsResultId) { }
    public record CreateResponse(CustomerFollowupService.CardSnapshot card,
                                 TaskApplicationService.FollowupResponse creationWorkflow,
                                 boolean created) { }
    public record SyncProjection(CustomerFollowupService.SyncAttempt attempt, String workflowStatus,
                                 String currentStepId, ExecutionSnapshot execution) { }
}
