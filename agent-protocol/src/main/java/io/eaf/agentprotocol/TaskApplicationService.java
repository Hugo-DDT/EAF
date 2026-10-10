package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.capability.api.CapabilityService;
import io.eaf.execution.api.ExecutionService;
import io.eaf.execution.api.ExecutionSnapshot;
import io.eaf.learning.api.FeedbackService;
import io.eaf.model.api.ModelProfileSelection;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workflow.api.CreateWorkflowInstanceCommand;
import io.eaf.workflow.api.WorkflowInstance;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.api.WorkflowSourcePage;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskPageCursor;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.task.api.TaskStepView;
import io.eaf.usage.api.UsageRecorder;
import io.eaf.usage.api.UsageRecord;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 协议适配器共用此入口，业务身份与结果可见性由可信上下文和领域 API 决定。 */
@Service
public class TaskApplicationService {
    private final TaskService tasks;
    private final RuntimeQuery runtime;
    private final CapabilityService capabilities;
    private final ExecutionService executions;
    private final ObjectMapper json;
    private final UsageRecorder usage;
    private final WorkflowService workflows;
    private final FeedbackService feedbacks;
    private final AgentCatalog agents;
    private KnowledgeService knowledge;
    private WorkspaceAuthorization workspaces;
    private static final UUID SERVICE_REQUEST_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000000e");
    private static final String SERVICE_REQUEST_WORKFLOW_VERSION = "1.0.0";
    private static final UUID P9_FOLLOWUP_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-00000000000a");
    private static final UUID P10_CUSTOMER_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-00000000000c");
    private static final UUID EXPERIENCE_DRAFT_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-00000000000d");
    private static final UUID EXPERIENCE_DRAFT_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-00000000000e");
    private static final UUID P9_FOLLOWUP_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-000000000009");
    private static final String P9_FOLLOWUP_WORKFLOW_VERSION = "1.1.0";
    private static final String P10_FOLLOWUP_WORKFLOW_VERSION = "1.2.0";

    public TaskApplicationService(TaskService tasks, RuntimeQuery runtime, CapabilityService capabilities,
                                  ExecutionService executions,
                                  ObjectMapper json, UsageRecorder usage, WorkflowService workflows,
                                  FeedbackService feedbacks, AgentCatalog agents) {
        this.tasks = tasks;
        this.runtime = runtime;
        this.capabilities = capabilities;
        this.executions = executions;
        this.json = json;
        this.usage = usage;
        this.workflows = workflows;
        this.feedbacks = feedbacks;
        this.agents = agents;
    }

    @Autowired
    void serviceRequestBoundaries(KnowledgeService knowledge, WorkspaceAuthorization workspaces) {
        this.knowledge = knowledge;
        this.workspaces = workspaces;
    }

    public TaskResponse create(ActorContext actor, UUID workspaceId, CreateRequest request,
                               String idempotencyKey, String traceId, String entryProtocol) {
        // 入口协议只能由协议适配器固定传入，不能由业务请求体声明。
        if (!List.of("REST", "MCP", "A2A").contains(entryProtocol))
            throw EafException.invalid("Task entryProtocol 无效。");
        var hasAgent = request.agentId() != null || request.agentVersion() != null;
        var hasCapability = request.capabilityId() != null || request.capabilityVersion() != null;
        if (hasAgent && (request.agentId() == null || request.agentVersion() == null))
            throw EafException.invalid("agentId 与 agentVersion 必须同时提供。");
        if (hasCapability && (request.capabilityId() == null || request.capabilityVersion() == null))
            throw EafException.invalid("capabilityId 与 capabilityVersion 必须同时提供。");
        // Capability 固定 Agent/Skill 版本；客户端不能覆盖其发布依赖。
        var capability = hasCapability
                ? capabilities.requirePublished(actor, workspaceId, request.capabilityId(), request.capabilityVersion())
                : null;
        if (capability != null && hasAgent && (!capability.agentId().equals(request.agentId())
                || !capability.agentVersion().equals(request.agentVersion())))
            throw EafException.conflict("CAPABILITY_AGENT_CONFLICT", "Agent 选择与 Capability 固定依赖不一致。");
        var agentId = capability == null ? request.agentId() : capability.agentId();
        var agentVersion = capability == null ? request.agentVersion() : capability.agentVersion();
        if (agentId == null || agentVersion == null)
            throw EafException.invalid("必须选择 Agent 或已发布 Capability。");
        var entity = request.businessEntity();
        var binding = capability == null ? null : new TaskAssetBinding(capability.id(), capability.version(),
                capability.contentHash(), capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        // source 始终由应用入口指定；协议请求体不能伪造来源、身份或授权范围。
        var task = tasks.create(new CreateTaskCommand(actor, workspaceId, agentId, agentVersion, request.input(),
                entity == null ? null : entity.type(), entity == null ? null : entity.id(), idempotencyKey,
                traceId == null || traceId.isBlank() ? UUID.randomUUID().toString() : traceId,
                "USER", binding, entryProtocol, request.modelProfileRef()));
        return project(actor, workspaceId, task, List.of());
    }

    public TaskResponse createForSkill(ActorContext actor, UUID workspaceId, String skillId,
                                       String input, String messageId) {
        if (skillId == null || skillId.isBlank() || skillId.length() > 200)
            throw EafException.invalid("A2A skillId 无效。");
        // AgentCard 的公开 skillId 只负责选择；执行前仍由 CapabilityService 核验当前发布版本。
        var capability = capabilities.list(actor, workspaceId).stream()
                .filter(item -> (item.name() + "@" + item.version()).equals(skillId))
                .findFirst().orElseThrow(EafException::notFound);
        return create(actor, workspaceId, new CreateRequest(null, null, capability.id(), capability.version(),
                input, null), messageId, null, "A2A");
    }

    @Transactional
    public TaskResponse createExperienceDraft(ActorContext actor, UUID workspaceId, UUID sourceTaskId,
                                              ExperienceDraftRequest request, String idempotencyKey,
                                              String traceId) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated()
                || request == null || request.feedbackId() == null || idempotencyKey == null
                || idempotencyKey.isBlank() || idempotencyKey.length() > 200
                || request.draftText() != null && request.draftText().length() > 800)
            throw EafException.invalid("经验整理请求需要本人反馈、有效幂等键和不超过 800 字的可选草稿。");
        var feedback = feedbacks.requireExperienceSource(actor, workspaceId, request.feedbackId());
        if (!sourceTaskId.equals(feedback.taskId())) throw EafException.notFound();
        var agent = agents.requirePublished(actor.tenantId(), workspaceId, feedback.source().agentId(),
                feedback.source().agentVersion());
        if (!Set.of("KNOWLEDGE_QA_V1", "CONVERSATIONAL_KNOWLEDGE_QA_V1", "CONVERSATIONAL_KNOWLEDGE_QA_V2",
                "CONVERSATIONAL_CUSTOMER_FOLLOWUP_V1", "CONVERSATIONAL_CUSTOMER_FOLLOWUP_V2",
                "CUSTOMER_RISK_V1", "CUSTOMER_FOLLOWUP_V1").contains(agent.responseProfile()))
            throw EafException.forbidden("个人整理只接受已发布的问答或客户分析反馈。");
        var capability = capabilities.requirePublished(actor, workspaceId, EXPERIENCE_DRAFT_CAPABILITY_ID, "1.0.0");
        final String input;
        try {
            // 只向整理模型传用户提交的纠正、依据和可选草稿；不复制原答案、客户简报或知识正文。
            input = json.writeValueAsString(new ExperienceDraftInput(feedback.correction(), feedback.evidence(), request.draftText()));
        } catch (com.fasterxml.jackson.core.JsonProcessingException invalid) {
            throw EafException.invalid("经验整理输入无法编码。");
        }
        if (input.length() > 8_000) throw EafException.invalid("经验整理输入合计不能超过 8000 字符。");
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        var taskCommand = new CreateTaskCommand(actor, workspaceId, EXPERIENCE_DRAFT_AGENT_ID, "1.0.0", input,
                null, null, idempotencyKey, traceId == null || traceId.isBlank()
                ? UUID.randomUUID().toString() : traceId, "USER", binding, "REST");
        var conversation = tasks.conversationBinding(actor, workspaceId, sourceTaskId);
        var task = tasks.createExperienceDraft(new TaskService.CreateExperienceDraftCommand(taskCommand,
                sourceTaskId, feedback.id(), conversation == null ? null : conversation.conversationId()));
        return project(actor, workspaceId, task, List.of());
    }

    public TaskResponse get(ActorContext actor, UUID workspaceId, UUID taskId) {
        tasks.requireConversationTaskAccess(actor, workspaceId, taskId);
        var task = tasks.get(actor, workspaceId, taskId);
        var steps = actor.delegated() && io.eaf.identity.api.IdentityService.MCP_AUDIENCE.equals(actor.delegationAudience())
                ? List.<TaskStepView>of() : runtime.steps(actor, workspaceId, taskId);
        return project(actor, workspaceId, task, steps);
    }

    public TaskPageResponse list(ActorContext actor, UUID workspaceId, UUID rootTaskId, Set<TaskStatus> statuses,
                                 Instant statusUpdatedAfter, TaskPageCursor cursor, int pageSize) {
        // Task 域在 SQL 层完成 Workspace/委托过滤，本层逐项应用 REST 共用的 currentness 结果投影。
        var page = tasks.list(actor, workspaceId, rootTaskId, statuses, statusUpdatedAfter, cursor, pageSize);
        var projected = page.items().stream().filter(task -> tasks.isConversationTaskVisible(actor, workspaceId, task.id()))
                .map(task -> project(actor, workspaceId, task, List.of())).toList();
        // totalSize 是匹配查询的总量；当前页投影条数只受 pageSize 限制，不能替代总量。
        return new TaskPageResponse(projected, page.totalSize(), page.nextCursor());
    }

    public io.eaf.agentruntime.api.TaskSources sources(ActorContext actor, UUID workspaceId, UUID taskId) {
        tasks.requireConversationTaskAccess(actor, workspaceId, taskId);
        tasks.get(actor, workspaceId, taskId);
        if (!runtime.canExposeResult(actor, workspaceId, taskId))
            return new io.eaf.agentruntime.api.TaskSources(false, List.of());
        return runtime.contextSourceContents(actor, workspaceId, taskId);
    }

    public TaskUsageResponse usage(ActorContext actor, UUID workspaceId, UUID taskId) {
        tasks.requireConversationTaskAccess(actor, workspaceId, taskId);
        var task = tasks.get(actor, workspaceId, taskId);
        var rows = usage.findForTask(actor.tenantId(), workspaceId, task.id());
        var selection = tasks.modelSelection(actor, workspaceId, task.id());
        if (rows.isEmpty()) return new TaskUsageResponse(0, null, null, "NOT_RECORDED", List.of(), selection);
        var known = rows.stream().filter(row -> "KNOWN".equals(row.usageStatus())
                && row.inputTokens() != null && row.outputTokens() != null).toList();
        var input = known.stream().mapToInt(UsageRecord::inputTokens).sum();
        var output = known.stream().mapToInt(UsageRecord::outputTokens).sum();
        var tokenStatus = known.size() == rows.size() ? "KNOWN" : known.isEmpty() ? "UNKNOWN" : "PARTIAL";
        var grouped = new java.util.LinkedHashMap<String, CostAccumulator>();
        for (var row : rows) {
            var actual = row.actualCost();
            var estimated = row.estimatedCost();
            var currency = actual != null ? row.actualCostCurrency() : row.costCurrency();
            var status = actual != null ? "BILLED" : row.costStatus();
            var key = String.valueOf(currency) + "\u001f" + String.valueOf(status);
            var entry = grouped.computeIfAbsent(key, ignored -> new CostAccumulator(currency, status));
            entry.calls++;
            if (actual != null) entry.amount = entry.amount.add(actual);
            else if (estimated != null) entry.amount = entry.amount.add(estimated);
            else entry.amountKnown = false;
        }
        var costs = grouped.values().stream().map(item -> new TaskUsageCost(item.currency,
                item.amountKnown ? item.amount : null, item.status, item.calls)).toList();
        return new TaskUsageResponse(rows.size(), known.isEmpty() ? null : input,
                known.isEmpty() ? null : output, tokenStatus, costs, selection);
    }

    @Transactional
    public ServiceRequestResponse confirmServiceRequest(ActorContext actor, UUID workspaceId, UUID sourceTaskId,
            long expectedVersion, String idempotencyKey) {
        // 不用捕获 not-found 来判断首次提交；嵌套事务会把该异常标记为 rollback-only。
        if (tasks.findServiceRequestSubmission(actor, workspaceId, sourceTaskId).isEmpty())
            requireCurrentServiceRequestEvidence(actor, workspaceId, sourceTaskId);
        var submission = tasks.confirmServiceRequest(actor, workspaceId, sourceTaskId, expectedVersion, idempotencyKey);
        var workflow = workflows.createServiceRequestRegistrationWorkflow(actor, workspaceId, submission.id(),
                "p15-service-request:" + submission.id());
        tasks.bindServiceRequestWorkflow(actor, workspaceId, submission.id(), workflow.id());
        return serviceRequestProjection(actor, workspaceId, submission, workflow);
    }

    public ServiceRequestResponse serviceRequest(ActorContext actor, UUID workspaceId, UUID sourceTaskId) {
        var submission = tasks.serviceRequestSubmission(actor, workspaceId, sourceTaskId);
        if (submission.workflowInstanceId() == null)
            throw EafException.conflict("SERVICE_REQUEST_WORKFLOW_PENDING", "服务请求登记流程尚未绑定。");
        var workflow = workflows.getInstance(actor, workspaceId, submission.workflowInstanceId());
        return serviceRequestProjection(actor, workspaceId, submission, workflow);
    }

    private void requireCurrentServiceRequestEvidence(ActorContext actor, UUID workspaceId, UUID sourceTaskId) {
        if (knowledge == null || workspaces == null)
            throw EafException.conflict("SERVICE_REQUEST_EVIDENCE_UNAVAILABLE", "知识来源复核服务不可用。");
        var task = tasks.get(actor, workspaceId, sourceTaskId);
        if (!actor.actorId().equals(task.actorId()) || task.resultJson() == null)
            throw EafException.notFound();
        try {
            var refs = json.readTree(task.resultJson()).path("contextRefs");
            if (!refs.isArray() || refs.isEmpty() || refs.size() > 10)
                throw EafException.conflict("SERVICE_REQUEST_EVIDENCE_INVALID", "分析结果没有可确认的知识出处。");
            workspaces.require(actor, workspaceId, "context:read");
            for (var ref : refs) {
                if (!"KNOWLEDGE".equals(ref.path("sourceType").asText()))
                    throw EafException.conflict("SERVICE_REQUEST_EVIDENCE_INVALID", "服务请求只允许引用正式 Knowledge。");
                var usable = knowledge.isUsable(actor, workspaceId, UUID.fromString(ref.path("documentId").asText()),
                        ref.path("documentVersion").asInt(), UUID.fromString(ref.path("chunkId").asText()),
                        UUID.fromString(ref.path("buildId").asText()), ref.path("contentHash").asText());
                if (!usable) throw EafException.conflict("SERVICE_REQUEST_EVIDENCE_UNAVAILABLE", "服务请求分析引用已撤回或当前不可读。");
            }
        } catch (EafException denied) {
            throw denied;
        } catch (Exception invalid) {
            throw EafException.conflict("SERVICE_REQUEST_EVIDENCE_INVALID", "服务请求分析出处格式无效。");
        }
    }

    private ServiceRequestResponse serviceRequestProjection(ActorContext actor, UUID workspaceId,
            io.eaf.task.api.ServiceRequestSubmission submission, WorkflowInstance workflow) {
        var taskId = workflow.rootTaskId() == null ? workflow.childTaskId() : workflow.rootTaskId();
        ExecutionSnapshot execution = taskId == null ? null : executions.findForTask(actor, workspaceId, taskId).orElse(null);
        return new ServiceRequestResponse(submission.id(), submission.sourceTaskId(), submission.sourceTaskVersion(),
                submission.sourceResultHash(), workflow.id(), workflow.status(), workflow.currentStepId(),
                execution == null ? null : new ServiceRequestExecution(execution.id(), execution.status(),
                        execution.operationId(), execution.resultJson() == null ? null : safeRequestId(execution.resultJson()),
                        execution.errorCode()), submission.createdAt());
    }

    private String safeRequestId(String resultJson) {
        try {
            var result = json.readTree(resultJson);
            var value = result.path("requestId").asText(null);
            return value == null || value.length() > 160 ? null : value;
        } catch (Exception ignored) { return null; }
    }

    private static final class CostAccumulator {
        private final String currency;
        private final String status;
        private BigDecimal amount = BigDecimal.ZERO;
        private int calls;
        private boolean amountKnown = true;
        private CostAccumulator(String currency, String status) { this.currency = currency; this.status = status; }
    }

    @Transactional
    public FollowupResponse confirmFollowup(ActorContext actor, UUID workspaceId, UUID taskId,
                                           long expectedVersion, String summary, String idempotencyKey) {
        return confirmFollowup(actor, workspaceId, taskId, expectedVersion, summary, idempotencyKey, null, null);
    }

    @Transactional
    public FollowupResponse confirmFollowup(ActorContext actor, UUID workspaceId, UUID taskId,
                                           long expectedVersion, String summary, String idempotencyKey,
                                           UUID requestedConversationId, Integer requestedBriefRevision) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || summary == null
                || summary.isBlank() || summary.length() > 2_000 || idempotencyKey == null
                || idempotencyKey.isBlank() || idempotencyKey.length() > 200)
            throw EafException.invalid("跟进确认输入无效。");
        var task = tasks.get(actor, workspaceId, taskId);
        var p9Source = P9_FOLLOWUP_AGENT_ID.equals(task.agentId()) && "1.0.0".equals(task.agentVersion());
        var p10Source = P10_CUSTOMER_AGENT_ID.equals(task.agentId()) && "1.0.0".equals(task.agentVersion());
        var p11Source = P10_CUSTOMER_AGENT_ID.equals(task.agentId()) && "1.1.0".equals(task.agentVersion());
        var p12Source = P10_CUSTOMER_AGENT_ID.equals(task.agentId()) && "1.2.0".equals(task.agentVersion());
        var conversationSource = p10Source || p11Source || p12Source;
        if (!"USER".equals(task.source()) || task.status() != io.eaf.task.api.TaskStatus.SUCCEEDED
                || !(p9Source || conversationSource))
            throw EafException.conflict("FOLLOWUP_SOURCE_INVALID", "只有成功的客户分析任务可确认。");
        if (!actor.actorId().equals(task.actorId()))
            throw EafException.forbidden("只有源分析任务的发起人可以确认跟进草稿。");
        UUID conversationId = null;
        Integer briefRevision = null;
        var workflowVersion = P9_FOLLOWUP_WORKFLOW_VERSION;
        if (conversationSource) {
            var binding = tasks.conversationBinding(actor, workspaceId, taskId);
            if (binding == null || !"CUSTOMER_ASSISTANT".equals(binding.mode())
                    || requestedConversationId != null && !requestedConversationId.equals(binding.conversationId()))
                throw EafException.conflict("FOLLOWUP_SOURCE_INVALID", "跟进必须来自同一客户会话。");
            conversationId = binding.conversationId();
            briefRevision = requestedBriefRevision == null ? binding.currentBriefRevision() : requestedBriefRevision;
            if (briefRevision < 0)
                throw EafException.invalid("expectedBriefRevision 无效。");
            var assetBinding = task.assetBinding();
            var expectedCapabilityVersion = task.agentVersion();
            if (assetBinding == null || !UUID.fromString("54000000-0000-4000-8000-00000000000d").equals(assetBinding.capabilityId())
                    || !expectedCapabilityVersion.equals(assetBinding.capabilityVersion()))
                throw EafException.conflict("FOLLOWUP_SOURCE_INVALID", "客户分析的固定 Capability 版本无效。");
            workflowVersion = P10_FOLLOWUP_WORKFLOW_VERSION;
        } else if (requestedConversationId != null || requestedBriefRevision != null) {
            throw EafException.conflict("FOLLOWUP_SOURCE_INVALID", "分析不能带入会话来源字段。");
        }
        var result = parseResult(task.resultJson());
        var draft = result == null ? null : result.path("followupDraft");
        var customerId = draft == null ? null : draft.path("customerId").asText(null);
        if (customerId == null || customerId.isBlank() || customerId.length() > 160)
            throw EafException.conflict("FOLLOWUP_DRAFT_UNAVAILABLE", "分析结果没有可提交的已绑定客户草稿。");
        if (conversationSource && !customerId.equals(tasks.getConversation(actor, workspaceId, conversationId).customerId()))
            throw EafException.conflict("FOLLOWUP_DRAFT_UNAVAILABLE", "跟进草稿的客户标识与会话绑定不一致。");
        var replay = workflows.lookupInstanceByIdempotencyKey(actor, workspaceId, P9_FOLLOWUP_WORKFLOW_ID,
                workflowVersion, idempotencyKey);
        if (replay.isPresent()) {
            if (!matchesFollowupReplay(replay.get(), taskId, expectedVersion, summary, customerId,
                    conversationId, requestedBriefRevision))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一 Workflow 幂等键对应了不同跟进请求。");
            return FollowupResponse.of(replay.get());
        }
        var input = new java.util.LinkedHashMap<String, Object>();
        input.put("sourceTaskId", taskId.toString());
        input.put("sourceTaskVersion", Long.toString(expectedVersion));
        input.put("customerId", customerId);
        input.put("summary", summary.trim());
        if (conversationSource) {
            input.put("conversationId", conversationId.toString());
            input.put("briefRevision", briefRevision);
        }
        final String inputJson;
        try { inputJson = json.writeValueAsString(input); }
        catch (Exception e) { throw EafException.invalid("跟进确认内容无法序列化。"); }
        var command = new CreateWorkflowInstanceCommand(actor, workspaceId, P9_FOLLOWUP_WORKFLOW_ID,
                workflowVersion, inputJson, idempotencyKey, "USER");
        if (task.version() != expectedVersion)
            throw EafException.conflict("VERSION_CONFLICT", "客户分析任务版本已变化，请重新读取。");
        if (!runtime.canExposeResult(actor, workspaceId, taskId))
            throw EafException.conflict("CONTEXT_SNAPSHOT_UNAVAILABLE", "分析使用的知识来源已失效，不能提交跟进。");
        if (conversationSource) {
            var binding = tasks.conversationBinding(actor, workspaceId, taskId);
            if (binding == null || binding.briefRevision() != briefRevision
                    || binding.currentBriefRevision() != briefRevision)
                throw EafException.conflict("FOLLOWUP_SOURCE_STALE", "会话简报已变化；请重新分析当前版本。");
        }
        if (conversationSource)
            tasks.requireCurrentConversationFollowup(actor, workspaceId, conversationId, taskId,
                    expectedVersion, briefRevision);
        var instance = workflows.createInstance(command);
        return FollowupResponse.of(instance);
    }

    private boolean matchesFollowupReplay(WorkflowInstance instance, UUID taskId, long expectedVersion,
                                          String summary, String customerId, UUID conversationId,
                                          Integer requestedBriefRevision) {
        if (!"USER".equals(instance.source())) return false;
        var input = parseResult(instance.inputJson());
        if (input == null || !taskId.toString().equals(input.path("sourceTaskId").asText())
                || !Long.toString(expectedVersion).equals(input.path("sourceTaskVersion").asText())
                || !customerId.equals(input.path("customerId").asText())
                || !summary.trim().equals(input.path("summary").asText())) return false;
        if (conversationId == null)
            return !input.has("conversationId") && !input.has("briefRevision");
        if (!conversationId.toString().equals(input.path("conversationId").asText())
                || !input.path("briefRevision").canConvertToInt() || input.path("briefRevision").asInt() < 0)
            return false;
        return requestedBriefRevision == null || requestedBriefRevision == input.path("briefRevision").asInt();
    }

    public FollowupPageResponse followups(ActorContext actor, UUID workspaceId, UUID taskId,
                                          Instant cursorCreatedAt, UUID cursorInstanceId, int limit) {
        if (limit < 1 || limit > 50 || (cursorCreatedAt == null) != (cursorInstanceId == null))
            throw EafException.invalid("跟进列表 limit 或游标无效。");
        tasks.requireConversationTaskAccess(actor, workspaceId, taskId);
        tasks.get(actor, workspaceId, taskId);
        WorkflowSourcePage page = workflows.listForSourceTask(actor, workspaceId, taskId,
                cursorCreatedAt, cursorInstanceId, limit);
        return new FollowupPageResponse(page.items().stream().map(FollowupResponse::of).toList(), page.totalSize(),
                page.nextCreatedAt(), page.nextInstanceId());
    }

    private com.fasterxml.jackson.databind.JsonNode parseResult(String text) {
        try { return text == null ? null : json.readTree(text); }
        catch (Exception invalid) { return null; }
    }

    public TaskResponse cancel(ActorContext actor, UUID workspaceId, UUID taskId, long expectedVersion) {
        tasks.requireConversationTaskAccess(actor, workspaceId, taskId);
        var canceled = tasks.cancel(actor, workspaceId, taskId, expectedVersion);
        return project(actor, workspaceId, cancelRemoteIfNeeded(actor, workspaceId, canceled), List.of());
    }

    public TaskResponse cancel(ActorContext actor, UUID workspaceId, UUID taskId) {
        tasks.requireConversationTaskAccess(actor, workspaceId, taskId);
        var current = tasks.get(actor, workspaceId, taskId);
        try {
            var canceled = tasks.cancel(actor, workspaceId, taskId, current.version());
            return project(actor, workspaceId, cancelRemoteIfNeeded(actor, workspaceId, canceled), List.of());
        } catch (EafException deniedOrNotCancelable) {
            // 先经过 Task 域的 task:cancel 校验；重复取消仅对原已取消任务返回同一结果。
            if (current.status() == TaskStatus.CANCELLED && "INVALID_STATE".equals(deniedOrNotCancelable.code()))
                return project(actor, workspaceId, current, List.of());
            throw deniedOrNotCancelable;
        }
    }

    private TaskSnapshot cancelRemoteIfNeeded(ActorContext actor, UUID workspaceId, TaskSnapshot task) {
        // REST/MCP/A2A 共享取消入口；只有已提交远端 Task 的取消才转交 Execution 发 CancelTask。
        if (task.status() != TaskStatus.CANCELLING_REMOTE) return task;
        executions.pending(task.tenantId(), workspaceId, task.id(), task.attempt())
                .filter(execution -> "AWAITING_REMOTE".equals(execution.status()))
                .ifPresent(execution -> executions.cancelRemote(actor, workspaceId, execution.id()));
        return tasks.get(actor, workspaceId, task.id());
    }

    public TaskResponse retry(ActorContext actor, UUID workspaceId, UUID taskId, long expectedVersion,
                              String idempotencyKey) {
        tasks.requireConversationTaskAccess(actor, workspaceId, taskId);
        return project(actor, workspaceId, tasks.retry(actor, workspaceId, taskId, expectedVersion, idempotencyKey), List.of());
    }

    public TaskResponse resume(ActorContext actor, UUID workspaceId, UUID taskId, long expectedVersion,
                               String idempotencyKey) {
        tasks.requireConversationTaskAccess(actor, workspaceId, taskId);
        return project(actor, workspaceId, tasks.resume(actor, workspaceId, taskId, expectedVersion, idempotencyKey), List.of());
    }

    private TaskResponse project(ActorContext actor, UUID workspaceId, TaskSnapshot task,
                                 List<TaskStepView> steps) {
        // 幂等重放和状态变更响应也重新检查来源 currentness，撤回后隐藏原始结果。
        return TaskResponse.of(task, json, steps, runtime.canExposeResult(actor, workspaceId, task.id()));
    }

    public record CreateRequest(UUID agentId, String agentVersion, UUID capabilityId, String capabilityVersion,
                                String input, BusinessEntity businessEntity,
                                io.eaf.model.api.ModelProfileRef modelProfileRef) {
        public CreateRequest(UUID agentId, String agentVersion, UUID capabilityId, String capabilityVersion,
                             String input, BusinessEntity businessEntity) {
            this(agentId, agentVersion, capabilityId, capabilityVersion, input, businessEntity, null);
        }
    }

    public record TaskPageResponse(List<TaskResponse> items, long totalSize, TaskPageCursor nextCursor) { }
    public record ExperienceDraftRequest(UUID feedbackId, String draftText) { }
    private record ExperienceDraftInput(String correction, String evidence, String draftText) { }
    public record TaskUsageResponse(int calls, Integer inputTokens, Integer outputTokens, String tokenStatus,
                                    List<TaskUsageCost> costs, ModelProfileSelection modelSelection) {
        public TaskUsageResponse(int calls, Integer inputTokens, Integer outputTokens, String tokenStatus,
                                 List<TaskUsageCost> costs) {
            this(calls, inputTokens, outputTokens, tokenStatus, costs, null);
        }
    }
    public record TaskUsageCost(String currency, BigDecimal amount, String status, int calls) { }
    public record ServiceRequestExecution(UUID id, String status, UUID operationId, String requestId, String errorCode) { }
    public record ServiceRequestResponse(UUID submissionId, UUID sourceTaskId, long sourceTaskVersion,
            String sourceResultHash, UUID workflowId, String workflowStatus, String currentStepId,
            ServiceRequestExecution execution, Instant createdAt) { }
    public record FollowupResponse(UUID instanceId, UUID workflowId, String workflowVersion, String status,
                                   String currentStepId, String errorCode, long rowVersion, Instant createdAt,
                                   Instant deadlineAt, String waitingReason, String businessEffectStatus) {
        static FollowupResponse of(WorkflowInstance instance) {
            return new FollowupResponse(instance.id(), instance.workflowId(), instance.workflowVersion(), instance.status(),
                    instance.currentStepId(), instance.errorCode(), instance.rowVersion(), instance.createdAt(),
                    instance.deadlineAt(), instance.waitingReason(), instance.businessEffectStatus());
        }
    }
    public record FollowupPageResponse(List<FollowupResponse> items, long totalSize,
                                       Instant nextCreatedAt, UUID nextInstanceId) { }

    public record BusinessEntity(String type, String id) { }
}
