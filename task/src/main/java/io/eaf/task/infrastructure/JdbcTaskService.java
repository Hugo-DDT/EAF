package io.eaf.task.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.agent.api.AgentDefinition;
import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.identity.api.IdentityService;
import io.eaf.model.api.ModelProfileCatalog;
import io.eaf.model.api.ModelProfileSelection;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.CreateChildTaskCommand;
import io.eaf.task.api.CreateToolExecutionCommand;
import io.eaf.task.api.CreateWorkflowTaskCommand;
import io.eaf.task.api.CreateAutomationReadTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.task.api.TaskWorkItem;
import io.eaf.task.api.ConversationPromptContext;
import io.eaf.task.api.BudgetReservation;
import io.eaf.task.api.BudgetScopeReference;
import io.eaf.task.api.CreateBudgetScopeCommand;
import io.eaf.task.api.TaskEvidence;
import io.eaf.task.api.ServiceRequestSubmission;
import io.eaf.task.api.ServiceRequestWritePayload;
import io.eaf.task.api.TaskExecutionCheck;
import io.eaf.task.api.QualityRunSourceVerifier;
import io.eaf.task.api.P29BriefTaskSourceVerifier;
import io.eaf.task.api.P30AutomationTaskSourceVerifier;
import io.eaf.task.api.TaskAttemptRecovery;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskPage;
import io.eaf.task.api.TaskPageCursor;
import io.eaf.task.api.UserTaskResultPage;
import io.eaf.task.api.RemoteTaskWakeStatus;
import io.eaf.task.api.WorkflowTaskCancellation;
import io.eaf.task.api.WorkflowTaskProvenance;
import io.eaf.task.api.WorkflowExecutionSource;
import io.eaf.task.api.CustomerFollowupService;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceOperationalControl;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.tool.api.ToolDefinition;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcTaskService implements TaskService {
    private static final UUID EXPERIENCE_DRAFT_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-00000000000d");
    private static final UUID EXPERIENCE_DRAFT_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-00000000000e");
    private static final UUID EXPERIENCE_DRAFT_SKILL_ID = UUID.fromString("53000000-0000-4000-8000-00000000000d");
    private static final UUID SERVICE_REQUEST_PLAN_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000010");
    private static final UUID SERVICE_REQUEST_PLAN_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000012");
    private static final UUID SERVICE_REQUEST_REGISTER_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-00000000000f");
    private static final UUID SERVICE_REQUEST_REGISTER_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000013");
    private static final UUID SERVICE_REQUEST_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000000e");
    private static final String SERVICE_REQUEST_TOOL = "service.request.register";
    private static final UUID P27_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000021");
    private static final UUID P27_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000022");
    private static final UUID P30_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000024");
    private static final UUID P30_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000024");
    private static final UUID P30_SKILL_ID = UUID.fromString("53000000-0000-4000-8000-000000000024");
    private static final ObjectMapper JSON = new ObjectMapper();
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final WorkspaceOperationalControl operationalControl;
    private final QualityRunSourceVerifier qualityRunSources;
    private final IdentityService identities;
    private final AgentCatalog agents;
    private final ToolCatalog tools;
    private final CustomerFollowupService customerFollowups;
    private final AuditPort audit;
    private final Clock clock;
    private final Duration activeDuration;
    private final Duration leaseDuration;
    private final Duration lifetime;
    private final int maxQueued;
    private final int maxQueuedPerWorkspace;
    private final boolean fairDispatchEnabled;
    private final int claimScanLimit;
    private final TransactionTemplate transactions;
    private final TaskOperationalMetrics metrics;
    private P29BriefTaskSourceVerifier p29Sources;
    private P30AutomationTaskSourceVerifier p30Sources;
    private ModelProfileCatalog modelProfiles;

    @org.springframework.beans.factory.annotation.Autowired
    void p29BriefTaskSourceVerifier(@org.springframework.context.annotation.Lazy P29BriefTaskSourceVerifier verifier) {
        this.p29Sources = verifier;
    }

    @org.springframework.beans.factory.annotation.Autowired
    void p30AutomationTaskSourceVerifier(@org.springframework.context.annotation.Lazy P30AutomationTaskSourceVerifier verifier) {
        this.p30Sources = verifier;
    }

    @org.springframework.beans.factory.annotation.Autowired
    void modelProfileCatalog(@org.springframework.context.annotation.Lazy ModelProfileCatalog catalog) {
        this.modelProfiles = catalog;
    }

    public JdbcTaskService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces,
                           WorkspaceOperationalControl operationalControl, QualityRunSourceVerifier qualityRunSources,
                           IdentityService identities, AgentCatalog agents, ToolCatalog tools,
                           CustomerFollowupService customerFollowups, AuditPort audit, Clock clock,
                           @Value("${eaf.task.active-duration:PT60S}") Duration activeDuration,
                           @Value("${eaf.task.lease-duration:PT15S}") Duration leaseDuration,
                           @Value("${eaf.task.lifetime:PT24H}") Duration lifetime,
                           @Value("${eaf.task.max-queued:256}") int maxQueued,
                           @Value("${eaf.task.max-queued-per-workspace:${eaf.task.max-queued:256}}") int maxQueuedPerWorkspace,
                           @Value("${eaf.task.fair-dispatch-enabled:true}") boolean fairDispatchEnabled,
                           @Value("${eaf.task.claim-scan-limit:32}") int claimScanLimit,
                           PlatformTransactionManager transactionManager,
                           TaskClusterCapacityMaintenance clusterCapacity,
                           TaskOperationalMetrics metrics) {
        this.jdbc = jdbc; this.workspaces = workspaces; this.operationalControl = operationalControl;
        this.qualityRunSources = qualityRunSources;
        this.identities = identities; this.agents = agents; this.tools = tools;
        this.customerFollowups = customerFollowups; this.audit = audit;
        this.clock = clock; this.activeDuration = activeDuration; this.leaseDuration = leaseDuration; this.lifetime = lifetime;
        if (maxQueued <= 0 || maxQueuedPerWorkspace <= 0 || maxQueuedPerWorkspace > maxQueued
                || claimScanLimit <= 0 || claimScanLimit > 256 || activeDuration.isNegative() || activeDuration.isZero()
                || leaseDuration.isNegative() || leaseDuration.isZero() || lifetime.isNegative() || lifetime.isZero())
            throw new IllegalArgumentException("eaf.task capacity and durations must be positive.");
        this.maxQueued = maxQueued;
        this.maxQueuedPerWorkspace = maxQueuedPerWorkspace;
        this.fairDispatchEnabled = fairDispatchEnabled;
        this.claimScanLimit = claimScanLimit;
        this.transactions = new TransactionTemplate(transactionManager);
        this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        java.util.Objects.requireNonNull(clusterCapacity);
        this.metrics = metrics;
    }

    @Override
    @Transactional
    public TaskSnapshot create(CreateTaskCommand c) {
        return create(c, null, false, null, null);
    }

    @Override
    @Transactional
    public TaskSnapshot createAutomationReadTask(CreateAutomationReadTaskCommand command) {
        if (command == null || command.actor() == null || command.actor().type() != io.eaf.shared.ActorType.HUMAN
                || command.actor().delegated() || command.workspaceId() == null || command.runId() == null
                || command.subscriptionId() == null || command.authorizationEpoch() < 1
                || !P30_AGENT_ID.equals(command.agentId()) || !"1.0.0".equals(command.agentVersion())
                || command.assetBinding() == null || !P30_CAPABILITY_ID.equals(command.assetBinding().capabilityId())
                || !"1.0.0".equals(command.assetBinding().capabilityVersion())
                || !P30_SKILL_ID.equals(command.assetBinding().skillId())
                || !"1.0.0".equals(command.assetBinding().skillVersion())
                || command.inputHash() == null || !command.inputHash().matches("[0-9a-f]{64}")
                || command.profileHash() == null || !command.profileHash().matches("[0-9a-f]{64}"))
            throw EafException.invalid("自动化只读 Task 的固定来源绑定不完整。");
        if (p30Sources == null) throw EafException.conflict("AUTOMATION_SOURCE_UNAVAILABLE", "自动化来源校验器未就绪。");
        p30Sources.requireTaskCreation(command);
        var task = new CreateTaskCommand(command.actor(), command.workspaceId(), command.agentId(), command.agentVersion(),
                command.input(), null, null, "p30-run:" + command.runId(), command.traceId(), "USER",
                command.assetBinding(), "REST");
        return create(task, null, false, command, null);
    }

    @Override
    @Transactional
    public void cancelAutomationTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        requireCurrentActor(actor, workspaceId, "task:cancel");
        var owner = jdbc.query("select owner_id from task.automation_task_binding where task_id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, taskId, actor.tenantId(), workspaceId);
        if (owner == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated()
                || !actor.actorId().equals(owner)) throw EafException.notFound();
        cancelTaskInternal(actor.tenantId(), workspaceId, actor.actorId(), taskId, null, false);
    }

    @Override
    @Transactional
    public TaskSnapshot createExperienceDraft(CreateExperienceDraftCommand command) {
        if (command == null || command.task() == null || command.sourceTaskId() == null || command.sourceFeedbackId() == null)
            throw EafException.invalid("经验整理 Task 的来源绑定不完整。");
        var taskCommand = command.task();
        var actor = taskCommand.actor();
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated()
                || !EXPERIENCE_DRAFT_AGENT_ID.equals(taskCommand.agentId()) || !"1.0.0".equals(taskCommand.agentVersion())
                || !"USER".equals(taskCommand.source()) || !"REST".equals(taskCommand.entryProtocol())
                || taskCommand.assetBinding() == null
                || !EXPERIENCE_DRAFT_CAPABILITY_ID.equals(taskCommand.assetBinding().capabilityId())
                || !"1.0.0".equals(taskCommand.assetBinding().capabilityVersion())
                || !EXPERIENCE_DRAFT_SKILL_ID.equals(taskCommand.assetBinding().skillId())
                || !"1.0.0".equals(taskCommand.assetBinding().skillVersion()))
            throw EafException.forbidden("经验整理 Task 必须通过固定 Capability 由本人 HUMAN 创建。");
        requireCurrentActor(actor, taskCommand.workspaceId(), "task:create");
        var source = get(actor, taskCommand.workspaceId(), command.sourceTaskId());
        var sourceEvidence = evidence(actor.tenantId(), taskCommand.workspaceId(), source.id());
        if (!actor.actorId().equals(source.actorId()) || !"USER".equals(source.source())
                || !"AGENT".equals(source.runKind()) || sourceEvidence.qualityRunId() != null
                || !List.of(TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.TIMED_OUT, TaskStatus.CANCELLED).contains(source.status()))
            throw EafException.forbidden("经验整理只允许绑定本人可读的普通 USER Agent Task。");
        var conversation = conversationBinding(actor, taskCommand.workspaceId(), source.id());
        var conversationId = command.sourceConversationId();
        if (conversationId != null && (conversation == null || !conversationId.equals(conversation.conversationId())))
            throw EafException.notFound();
        if (conversationId == null && conversation != null) conversationId = conversation.conversationId();

        var created = create(taskCommand, null, true);
        var inputHash = Hashing.sha256(taskCommand.input());
        jdbc.update("insert into task.experience_draft_binding(task_id, tenant_id, workspace_id, owner_id, source_task_id, source_feedback_id, source_conversation_id, input_hash) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?) on conflict (task_id) do nothing",
                created.id(), actor.tenantId(), taskCommand.workspaceId(), actor.actorId(), source.id(),
                command.sourceFeedbackId(), conversationId, inputHash);
        var binding = jdbc.query("select source_task_id, source_feedback_id, source_conversation_id, input_hash "
                        + "from task.experience_draft_binding where task_id = ? and tenant_id = ? and workspace_id = ? and owner_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("source_task_id", UUID.class), rs.getObject("source_feedback_id", UUID.class),
                        rs.getObject("source_conversation_id", UUID.class), rs.getString("input_hash")} : null,
                created.id(), actor.tenantId(), taskCommand.workspaceId(), actor.actorId());
        if (binding == null || !source.id().equals(binding[0]) || !command.sourceFeedbackId().equals(binding[1])
                || !java.util.Objects.equals(conversationId, binding[2]) || !inputHash.equals(binding[3]))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一整理 Task 请求键不能绑定不同反馈或来源 Task。");
        return get(actor, taskCommand.workspaceId(), created.id());
    }

    @Override
    @Transactional(readOnly = true)
    public ExperienceDraftBinding experienceDraftBinding(ActorContext actor, UUID workspaceId, UUID taskId) {
        requireCurrentActor(actor, workspaceId, "task:read");
        requireExperienceDraftOwner(actor, workspaceId, taskId);
        var binding = jdbc.query("select owner_id, source_task_id, source_feedback_id, source_conversation_id, input_hash, created_at "
                        + "from task.experience_draft_binding where task_id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new ExperienceDraftBinding(taskId, rs.getObject("owner_id", UUID.class),
                        rs.getObject("source_task_id", UUID.class), rs.getObject("source_feedback_id", UUID.class),
                        rs.getObject("source_conversation_id", UUID.class), rs.getString("input_hash"),
                        rs.getTimestamp("created_at").toInstant()) : null,
                taskId, actor.tenantId(), workspaceId);
        if (binding == null) throw EafException.notFound();
        return binding;
    }

    @Override
    public void requireExperienceDraftTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        var binding = experienceDraftBinding(actor, workspaceId, taskId);
        if (!actor.actorId().equals(binding.ownerId())) throw EafException.notFound();
    }

    @Override
    @Transactional
    public ServiceRequestSubmission confirmServiceRequest(ActorContext actor, UUID workspaceId, UUID sourceTaskId,
                                                           long expectedTaskVersion, String idempotencyKey) {
        requireServiceRequestActor(actor, workspaceId);
        if (sourceTaskId == null || expectedTaskVersion < 1 || idempotencyKey == null
                || idempotencyKey.isBlank() || idempotencyKey.length() > 200)
            throw EafException.invalid("服务请求确认需要源 Task 版本和有效幂等键。");
        var task = get(actor, workspaceId, sourceTaskId);
        if (!actor.actorId().equals(task.actorId()) || task.status() != TaskStatus.SUCCEEDED
                || !"USER".equals(task.source()) || !"AGENT".equals(task.runKind())
                || !SERVICE_REQUEST_PLAN_AGENT_ID.equals(task.agentId())
                || task.version() != expectedTaskVersion || task.assetBinding() == null
                || !SERVICE_REQUEST_PLAN_CAPABILITY_ID.equals(task.assetBinding().capabilityId())
                || !serviceRequestAnalysisVersion(task.agentVersion(), task.assetBinding().capabilityVersion()))
            throw EafException.conflict("SERVICE_REQUEST_SOURCE_INVALID", "只能确认本人固定版本的成功服务请求分析 Task。");
        var agent = agents.requirePublished(actor.tenantId(), workspaceId, task.agentId(), task.agentVersion());
        if (!"SERVICE_REQUEST_PLAN_V1".equals(agent.responseProfile()) || !agent.ragEnabled()
                || !"HYBRID".equals(agent.retrievalMode()) || !"NONE".equals(agent.evidencePolicy())
                || !agents.tools(actor.tenantId(), workspaceId, agent.id(), agent.version()).isEmpty())
            throw EafException.conflict("SERVICE_REQUEST_SOURCE_INVALID", "源 Task 不符合只读服务请求分析资产。");
        var entity = jdbc.query("select business_entity_type, business_entity_id, quality_run_id from task.task "
                        + "where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new Object[]{rs.getString(1), rs.getString(2), rs.getObject(3, UUID.class)} : null,
                sourceTaskId, actor.tenantId(), workspaceId);
        if (entity == null || entity[0] != null || entity[1] != null || entity[2] != null)
            throw EafException.forbidden("服务请求分析不能绑定其他业务实体或评测来源。");
        var result = parseServiceRequestResult(task.resultJson());
        var sourceHash = Hashing.sha256(canonical(result));
        var payload = serviceRequestPayload(result, sourceTaskId, sourceHash);
        var refs = result.path("contextRefs");
        if (!refs.isArray() || refs.isEmpty() || refs.size() > 10)
            throw EafException.conflict("SERVICE_REQUEST_EVIDENCE_INVALID", "服务请求分析缺少可复核的正式知识出处。");
        var keyHash = Hashing.sha256(String.join("\u001f", actor.tenantId().toString(), workspaceId.toString(),
                actor.actorId().toString(), idempotencyKey));
        var requestHash = Hashing.sha256(expectedTaskVersion + "\u001f" + sourceHash);

        var existing = findServiceSubmissionBySource(actor, workspaceId, sourceTaskId);
        if (existing != null) {
            if (existing.sourceTaskVersion() != expectedTaskVersion || !sourceHash.equals(existing.sourceResultHash()))
                throw EafException.conflict("SERVICE_REQUEST_SOURCE_CHANGED", "分析结果已变化，请重新分析后确认。");
            return existing;
        }
        var priorKey = findServiceSubmissionByKey(actor, workspaceId, keyHash);
        if (priorKey != null) {
            if (!priorKey.sourceTaskId().equals(sourceTaskId) || !requestHash.equals(serviceRequestHash(priorKey.id())))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "服务请求确认键已绑定其他内容。");
            return priorKey;
        }

        var submissionId = UUID.randomUUID();
        jdbc.update("insert into task.service_request_submission(id, tenant_id, workspace_id, owner_id, source_task_id, "
                        + "source_task_version, source_result_hash, request_key_hash, request_hash, payload_json, context_refs_json) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb) on conflict do nothing",
                submissionId, actor.tenantId(), workspaceId, actor.actorId(), sourceTaskId, expectedTaskVersion,
                sourceHash, keyHash, requestHash, payload, canonical(refs));
        var saved = findServiceSubmissionBySource(actor, workspaceId, sourceTaskId);
        if (saved == null) saved = findServiceSubmissionByKey(actor, workspaceId, keyHash);
        if (saved == null) throw EafException.conflict("SERVICE_REQUEST_CONFLICT", "并发确认未能建立唯一提交记录。");
        if (!saved.sourceTaskId().equals(sourceTaskId) || saved.sourceTaskVersion() != expectedTaskVersion
                || !sourceHash.equals(saved.sourceResultHash()))
            throw EafException.conflict("SERVICE_REQUEST_CONFLICT", "同一分析 Task 已绑定不同服务请求内容。");
        return saved;
    }

    // 仅允许同一精确资产对，避免确认入口被宽泛的 1.x 版本条件放开。
    private boolean serviceRequestAnalysisVersion(String agentVersion, String capabilityVersion) {
        return "1.0.0".equals(agentVersion) && "1.0.0".equals(capabilityVersion)
                || "1.1.0".equals(agentVersion) && "1.1.0".equals(capabilityVersion);
    }

    private boolean isP32P15Task(CreateTaskCommand command, AgentDefinition agent) {
        var binding = command.assetBinding();
        return command.actor().type() == io.eaf.shared.ActorType.HUMAN && !command.actor().delegated()
                && "USER".equals(command.source()) && "REST".equals(command.entryProtocol())
                && command.businessEntityType() == null && command.businessEntityId() == null
                && SERVICE_REQUEST_PLAN_AGENT_ID.equals(agent.id())
                && "SERVICE_REQUEST_PLAN_V1".equals(agent.responseProfile()) && agent.ragEnabled()
                && "HYBRID".equals(agent.retrievalMode()) && "NONE".equals(agent.evidencePolicy())
                && binding != null && SERVICE_REQUEST_PLAN_CAPABILITY_ID.equals(binding.capabilityId())
                && serviceRequestAnalysisVersion(agent.version(), binding.capabilityVersion())
                && agents.tools(command.actor().tenantId(), command.workspaceId(), agent.id(), agent.version()).isEmpty();
    }

    @Override
    @Transactional(readOnly = true)
    public ServiceRequestSubmission serviceRequestSubmission(ActorContext actor, UUID workspaceId, UUID sourceTaskId) {
        requireServiceRequestActor(actor, workspaceId);
        var source = get(actor, workspaceId, sourceTaskId);
        if (!actor.actorId().equals(source.actorId())) throw EafException.notFound();
        var submission = findServiceSubmissionBySource(actor, workspaceId, sourceTaskId);
        if (submission == null) throw EafException.notFound();
        return submission;
    }

    @Override
    @Transactional(readOnly = true)
    public java.util.Optional<ServiceRequestSubmission> findServiceRequestSubmission(ActorContext actor,
            UUID workspaceId, UUID sourceTaskId) {
        requireServiceRequestActor(actor, workspaceId);
        var source = get(actor, workspaceId, sourceTaskId);
        if (!actor.actorId().equals(source.actorId())) throw EafException.notFound();
        return java.util.Optional.ofNullable(findServiceSubmissionBySource(actor, workspaceId, sourceTaskId));
    }

    @Override
    @Transactional(readOnly = true)
    public ServiceRequestSubmission requireServiceRequestSubmission(ActorContext actor, UUID workspaceId,
                                                                     UUID submissionId) {
        requireServiceRequestActor(actor, workspaceId);
        var submission = findServiceSubmission(actor, workspaceId, submissionId);
        if (submission == null || !actor.actorId().equals(submission.ownerId())) throw EafException.notFound();
        return submission;
    }

    @Override
    @Transactional
    public void bindServiceRequestWorkflow(ActorContext actor, UUID workspaceId, UUID submissionId,
                                           UUID workflowInstanceId) {
        if (workflowInstanceId == null) throw EafException.invalid("服务请求 Workflow 实例缺失。");
        var submission = requireServiceRequestSubmission(actor, workspaceId, submissionId);
        if (submission.workflowInstanceId() != null) {
            if (!submission.workflowInstanceId().equals(workflowInstanceId))
                throw EafException.conflict("SERVICE_REQUEST_WORKFLOW_CONFLICT", "提交记录已绑定其他 Workflow。");
            return;
        }
        var changed = jdbc.update("update task.service_request_submission set workflow_instance_id = ? "
                        + "where id = ? and tenant_id = ? and workspace_id = ? and owner_id = ? and workflow_instance_id is null",
                workflowInstanceId, submissionId, actor.tenantId(), workspaceId, actor.actorId());
        if (changed == 0) {
            var current = requireServiceRequestSubmission(actor, workspaceId, submissionId);
            if (!workflowInstanceId.equals(current.workflowInstanceId()))
                throw EafException.conflict("SERVICE_REQUEST_WORKFLOW_CONFLICT", "提交记录并发绑定了其他 Workflow。");
        }
    }

    @Override
    public void requireServiceRequestTaskCreation(ActorContext actor, UUID workspaceId,
            WorkflowTaskProvenance provenance, String toolName, String toolVersion, String argumentsJson) {
        requireServiceRequestActor(actor, workspaceId);
        if (provenance == null || !provenance.complete() || !SERVICE_REQUEST_WORKFLOW_ID.equals(provenance.workflowId())
                || !"1.0.0".equals(provenance.workflowVersion()) || !"register".equals(provenance.stepId())
                || !SERVICE_REQUEST_TOOL.equals(toolName) || !"1.0.0".equals(toolVersion))
            throw EafException.forbidden("服务请求登记 Tool 只能由固定 Workflow 的 register 步骤创建。");
        var submissionId = serviceRequestSubmissionId(argumentsJson);
        var submission = requireServiceRequestSubmission(actor, workspaceId, submissionId);
        if (!provenance.workflowInstanceId().equals(submission.workflowInstanceId()))
            throw EafException.forbidden("服务请求 Workflow provenance 与源提交记录不匹配。");
    }

    @Override
    @Transactional(readOnly = true)
    public void requireServiceRequestHandlingTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("服务请求处理 Agent 只接受 Workflow 发起人的直接 HUMAN 身份。");
        get(actor, workspaceId, taskId);
        var binding = jdbc.query("select workflow_id, workflow_version, workflow_step_id, run_kind, agent_id, agent_version, capability_id, capability_version "
                        + "from task.task where id = ? and tenant_id = ? and workspace_id = ? and actor_id = ? and source = 'USER'",
                rs -> rs.next() ? new HandlingTaskBinding(rs.getObject("workflow_id", UUID.class),
                        rs.getString("workflow_version"), rs.getString("workflow_step_id"), rs.getString("run_kind"),
                        rs.getObject("agent_id", UUID.class), rs.getString("agent_version"),
                        rs.getObject("capability_id", UUID.class), rs.getString("capability_version")) : null,
                taskId, actor.tenantId(), workspaceId, actor.actorId());
        var expectedAgent = "prepare".equals(binding == null ? null : binding.stepId())
                ? UUID.fromString("20000000-0000-4000-8000-000000000011")
                : UUID.fromString("20000000-0000-4000-8000-000000000012");
        var expectedCapability = "prepare".equals(binding == null ? null : binding.stepId())
                ? UUID.fromString("54000000-0000-4000-8000-000000000014")
                : UUID.fromString("54000000-0000-4000-8000-000000000015");
        var p16 = binding != null && "1.0.0".equals(binding.workflowVersion());
        var p17 = binding != null && "1.1.0".equals(binding.workflowVersion());
        var p18 = binding != null && "1.2.0".equals(binding.workflowVersion());
        var teamExperienceFlow = p17 || p18;
        var expectedVersion = teamExperienceFlow && "prepare".equals(binding == null ? null : binding.stepId())
                ? binding.workflowVersion() : "1.0.0";
        if (binding == null || !UUID.fromString("58000000-0000-4000-8000-00000000000f").equals(binding.workflowId())
                || !p16 && !p17 && !p18 || !java.util.Set.of("prepare", "summarize").contains(binding.stepId())
                || !"AGENT".equals(binding.runKind()) || !expectedAgent.equals(binding.agentId())
                || !expectedVersion.equals(binding.agentVersion()) || !expectedCapability.equals(binding.capabilityId())
                || !expectedVersion.equals(binding.capabilityVersion()))
            throw EafException.forbidden("只允许固定处理 Workflow 调用服务请求协作 Agent。");
    }

    private record HandlingTaskBinding(UUID workflowId, String workflowVersion, String stepId, String runKind,
                                       UUID agentId, String agentVersion, UUID capabilityId, String capabilityVersion) { }

    @Override
    @Transactional(readOnly = true)
    public ServiceRequestWritePayload requireServiceRequestWritePayload(ActorContext actor, UUID workspaceId,
            UUID taskId, int attempt, UUID submissionId) {
        requireServiceRequestActor(actor, workspaceId);
        var binding = jdbc.query("select actor_id, source, run_kind, attempt, agent_id, agent_version, tool_name, "
                        + "tool_version, tool_arguments_json::text, workflow_instance_id, workflow_id, workflow_version, workflow_step_id "
                        + "from task.task where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new ServiceRequestTaskBinding(rs.getObject("actor_id", UUID.class), rs.getString("source"),
                        rs.getString("run_kind"), rs.getInt("attempt"), rs.getObject("agent_id", UUID.class),
                        rs.getString("agent_version"), rs.getString("tool_name"), rs.getString("tool_version"),
                        rs.getString("tool_arguments_json"), rs.getObject("workflow_instance_id", UUID.class),
                        rs.getObject("workflow_id", UUID.class), rs.getString("workflow_version"),
                        rs.getString("workflow_step_id")) : null,
                taskId, actor.tenantId(), workspaceId);
        if (binding == null || !actor.actorId().equals(binding.actorId()) || !"USER".equals(binding.source())
                || !"TOOL_EXECUTION".equals(binding.runKind()) || binding.attempt() != attempt
                || !SERVICE_REQUEST_REGISTER_AGENT_ID.equals(binding.agentId()) || !"1.0.0".equals(binding.agentVersion())
                || !SERVICE_REQUEST_TOOL.equals(binding.toolName()) || !"1.0.0".equals(binding.toolVersion())
                || binding.workflowInstanceId() == null || !SERVICE_REQUEST_WORKFLOW_ID.equals(binding.workflowId())
                || !"1.0.0".equals(binding.workflowVersion()) || !"register".equals(binding.workflowStepId())
                || !submissionId.equals(serviceRequestSubmissionId(binding.argumentsJson())))
            throw EafException.forbidden("Execution 不是固定服务请求登记 Task。");
        var submission = requireServiceRequestSubmission(actor, workspaceId, submissionId);
        if (!binding.workflowInstanceId().equals(submission.workflowInstanceId()))
            throw EafException.forbidden("服务请求 Tool Task 未绑定已确认的 Workflow 实例。");
        var payload = parseServiceRequestResult(submission.payloadJson());
        return new ServiceRequestWritePayload(submission.id(), submission.ownerId(), submission.sourceTaskId(),
                submission.sourceResultHash(), payload.path("category").asText(), payload.path("title").asText(),
                payload.path("summary").asText(), payload.path("handlingSuggestion").asText(),
                submission.contextRefsJson());
    }

    private void requireServiceRequestActor(ActorContext actor, UUID workspaceId) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("内部服务请求只接受本人直接操作的 HUMAN 身份。");
        workspaces.require(actor, workspaceId, "task:create");
        workspaces.require(actor, workspaceId, "task:read");
        workspaces.require(actor, workspaceId, "service-request:submit");
    }

    private JsonNode parseServiceRequestResult(String raw) {
        try {
            var node = raw == null ? null : JSON.readTree(raw);
            if (node == null || !node.isObject()) throw new IllegalArgumentException();
            return node;
        } catch (Exception invalid) {
            throw EafException.conflict("SERVICE_REQUEST_RESULT_INVALID", "服务请求结果无法读取。");
        }
    }

    private String serviceRequestPayload(JsonNode result, UUID sourceTaskId, String sourceResultHash) {
        if (!result.path("readyToSubmit").asBoolean(false) || !"READY".equals(result.path("outcome").asText())
                || !List.of("IT", "FACILITIES", "HR", "OTHER").contains(result.path("category").asText())
                || !serviceText(result, "title", 120) || !serviceText(result, "summary", 2_000)
                || !serviceText(result, "handlingSuggestion", 2_000) || !result.path("citations").isArray()
                || result.path("citations").isEmpty() || !servicePlanIsFixed(result.path("plan")))
            throw EafException.conflict("SERVICE_REQUEST_NOT_READY", "分析结果尚不能确认登记或固定计划已失效。");
        var refs = result.path("contextRefs");
        if (!refs.isArray() || refs.isEmpty() || refs.size() > 10)
            throw EafException.conflict("SERVICE_REQUEST_EVIDENCE_INVALID", "服务请求分析缺少可复核的正式知识出处。");
        for (var ref : refs) if (!validServiceRequestRef(ref))
            throw EafException.conflict("SERVICE_REQUEST_EVIDENCE_INVALID", "服务请求出处格式无效。");
        var payload = JSON.createObjectNode().put("category", result.path("category").asText())
                .put("title", result.path("title").asText().strip())
                .put("summary", result.path("summary").asText().strip())
                .put("handlingSuggestion", result.path("handlingSuggestion").asText().strip())
                .put("sourceTaskId", sourceTaskId.toString()).put("sourceResultHash", sourceResultHash);
        return canonical(payload);
    }

    private boolean serviceText(JsonNode result, String field, int maxLength) {
        var value = result.path(field);
        return value.isTextual() && !value.asText().isBlank() && value.asText().strip().length() <= maxLength
                && value.asText().chars().noneMatch(Character::isISOControl);
    }

    private boolean servicePlanIsFixed(JsonNode plan) {
        return plan.isObject()
                && SERVICE_REQUEST_WORKFLOW_ID.toString().equals(plan.path("workflowId").asText())
                && "1.0.0".equals(plan.path("workflowVersion").asText())
                && SERVICE_REQUEST_REGISTER_CAPABILITY_ID.toString().equals(plan.path("capabilityId").asText())
                && "1.0.0".equals(plan.path("capabilityVersion").asText())
                && "service.request.register".equals(plan.path("toolName").asText())
                && "1.0.0".equals(plan.path("toolVersion").asText())
                && plan.path("requiresConfirmation").asBoolean(false)
                && plan.path("requiresApproval").asBoolean(false);
    }

    private boolean validServiceRequestRef(JsonNode ref) {
        try {
            return ref.isObject() && "KNOWLEDGE".equals(ref.path("sourceType").asText())
                    && UUID.fromString(ref.path("documentId").asText()) != null
                    && ref.path("documentVersion").canConvertToInt() && ref.path("documentVersion").asInt() > 0
                    && UUID.fromString(ref.path("chunkId").asText()) != null
                    && UUID.fromString(ref.path("buildId").asText()) != null
                    && ref.path("contentHash").asText().matches("[0-9a-f]{64}");
        } catch (RuntimeException invalid) { return false; }
    }

    private UUID serviceRequestSubmissionId(String raw) {
        try {
            var args = raw == null ? null : JSON.readTree(raw);
            if (args == null || !args.isObject() || args.size() != 1 || !args.path("submissionId").isTextual())
                throw new IllegalArgumentException();
            return UUID.fromString(args.path("submissionId").asText());
        } catch (Exception invalid) {
            throw EafException.invalid("服务请求登记 Tool 只接受一个有效 submissionId。");
        }
    }

    private ServiceRequestSubmission findServiceSubmissionBySource(ActorContext actor, UUID workspaceId,
                                                                    UUID sourceTaskId) {
        return queryServiceSubmission("source_task_id = ?", actor, workspaceId, sourceTaskId);
    }

    private ServiceRequestSubmission findServiceSubmissionByKey(ActorContext actor, UUID workspaceId, String keyHash) {
        return queryServiceSubmission("request_key_hash = ?", actor, workspaceId, keyHash);
    }

    private ServiceRequestSubmission findServiceSubmission(ActorContext actor, UUID workspaceId, UUID submissionId) {
        return queryServiceSubmission("id = ?", actor, workspaceId, submissionId);
    }

    private ServiceRequestSubmission queryServiceSubmission(String predicate, ActorContext actor, UUID workspaceId,
                                                              Object value) {
        return jdbc.query("select id, source_task_id, owner_id, source_task_version, source_result_hash, "
                        + "payload_json::text, context_refs_json::text, workflow_instance_id, created_at "
                        + "from task.service_request_submission where tenant_id = ? and workspace_id = ? and owner_id = ? and "
                        + predicate,
                rs -> rs.next() ? new ServiceRequestSubmission(rs.getObject("id", UUID.class),
                        rs.getObject("source_task_id", UUID.class), rs.getObject("owner_id", UUID.class),
                        rs.getLong("source_task_version"), rs.getString("source_result_hash"), rs.getString("payload_json"),
                        rs.getString("context_refs_json"), rs.getObject("workflow_instance_id", UUID.class),
                        rs.getTimestamp("created_at").toInstant()) : null,
                actor.tenantId(), workspaceId, actor.actorId(), value);
    }

    private String serviceRequestHash(UUID submissionId) {
        return jdbc.query("select request_hash from task.service_request_submission where id = ?",
                rs -> rs.next() ? rs.getString(1) : null, submissionId);
    }

    private String canonical(JsonNode node) {
        try { return JSON.writeValueAsString(sortJson(node)); }
        catch (Exception invalid) { throw new IllegalStateException("服务请求 JSON 无法规范化。", invalid); }
    }

    private JsonNode sortJson(JsonNode node) {
        if (node.isObject()) {
            var sorted = JSON.createObjectNode();
            var names = new ArrayList<String>();
            node.fieldNames().forEachRemaining(names::add);
            names.sort(String::compareTo);
            names.forEach(name -> sorted.set(name, sortJson(node.get(name))));
            return sorted;
        }
        if (node.isArray()) {
            var sorted = JSON.createArrayNode();
            node.forEach(value -> sorted.add(sortJson(value)));
            return sorted;
        }
        return node.deepCopy();
    }

    @Override
    @Transactional
    public ConversationSnapshot createConversation(CreateConversationCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null)
            throw EafException.invalid("会话身份或 Workspace 缺失。");
        requireConversationActor(command.actor(), command.workspaceId(), "task:create");
        if (!List.of("KNOWLEDGE_QA", "CUSTOMER_ASSISTANT").contains(command.mode())
                || command.title() == null || command.title().isBlank() || command.title().trim().length() > 200
                || command.idempotencyKey() == null || command.idempotencyKey().isBlank()
                || command.idempotencyKey().length() > 200)
            throw EafException.invalid("会话模式、标题或幂等键无效。");
        if ("CUSTOMER_ASSISTANT".equals(command.mode())
                ? command.customerId() == null || command.customerId().isBlank() || command.customerId().length() > 160
                : command.customerId() != null)
            throw EafException.invalid("会话客户绑定与模式不一致。");
        var binding = new TaskAssetBinding(command.capabilityId(), command.capabilityVersion(), command.capabilityHash(),
                command.skillId(), command.skillVersion(), command.skillHash());
        validateAssetBinding(binding, "USER");
        if (command.agentId() == null || command.agentVersion() == null || command.agentVersion().isBlank())
            throw EafException.invalid("会话固定 Agent 版本缺失。");

        var createKey = conversationKey(command.actor(), command.workspaceId(), command.idempotencyKey());
        var createHash = Hashing.sha256(String.join("\u001f", command.mode(), command.title().trim(),
                String.valueOf(command.customerId()), command.capabilityId().toString(), command.capabilityVersion(),
                command.capabilityHash(), command.agentId().toString(), command.agentVersion(),
                command.skillId().toString(), command.skillVersion(), command.skillHash()));
        var existing = jdbc.query("select id, create_hash from task.conversation where tenant_id = ? and workspace_id = ? and owner_id = ? and create_key = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("id", UUID.class), rs.getString("create_hash")} : null,
                command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), createKey);
        if (existing != null) {
            if (!createHash.equals(existing[1]))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "会话创建键已绑定到不同请求。");
            return getConversation(command.actor(), command.workspaceId(), (UUID) existing[0]);
        }

        var id = UUID.randomUUID();
        var now = Instant.now(clock);
        var inserted = jdbc.update("insert into task.conversation(id, tenant_id, workspace_id, owner_id, mode, title, customer_id, capability_id, capability_version, capability_hash, agent_id, agent_version, skill_id, skill_version, skill_hash, status, current_brief_revision, row_version, last_turn_no, create_key, create_hash, created_at, updated_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', 0, 1, 0, ?, ?, ?, ?) on conflict (tenant_id, workspace_id, owner_id, create_key) do nothing",
                id, command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), command.mode(),
                command.title().trim(), command.customerId(), command.capabilityId(), command.capabilityVersion(),
                command.capabilityHash(), command.agentId(), command.agentVersion(), command.skillId(),
                command.skillVersion(), command.skillHash(), createKey, createHash, Timestamp.from(now), Timestamp.from(now));
        if (inserted == 0) {
            var concurrent = jdbc.queryForMap("select id, create_hash from task.conversation where tenant_id = ? and workspace_id = ? and owner_id = ? and create_key = ?",
                    command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), createKey);
            if (!createHash.equals(concurrent.get("create_hash")))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "会话创建键已绑定到不同请求。");
            return getConversation(command.actor(), command.workspaceId(), (UUID) concurrent.get("id"));
        }
        jdbc.update("insert into task.conversation_brief_revision(conversation_id, revision, content, confirmed_by, created_at) values (?, 0, '', ?, ?)",
                id, command.actor().actorId(), Timestamp.from(now));
        return getConversation(command.actor(), command.workspaceId(), id);
    }

    @Override
    public ConversationSnapshot getConversation(ActorContext actor, UUID workspaceId, UUID conversationId) {
        requireConversationActor(actor, workspaceId, "task:read");
        var result = jdbc.query(conversationSelect() + " where c.id = ? and c.tenant_id = ? and c.workspace_id = ? and c.owner_id = ?",
                rs -> rs.next() ? mapConversation(rs) : null, conversationId, actor.tenantId(), workspaceId, actor.actorId());
        if (result == null) throw EafException.notFound();
        return result;
    }

    @Override
    public ConversationPage listConversations(ActorContext actor, UUID workspaceId, String status,
                                               Instant cursorUpdatedAt, UUID cursorId, int pageSize) {
        requireConversationActor(actor, workspaceId, "task:read");
        if (pageSize < 1 || pageSize > 50 || (cursorUpdatedAt == null) != (cursorId == null)
                || status != null && !List.of("ACTIVE", "ARCHIVED").contains(status))
            throw EafException.invalid("会话分页参数无效。");
        var sql = new StringBuilder(conversationSelect() + " where c.tenant_id = ? and c.workspace_id = ? and c.owner_id = ?");
        var args = new ArrayList<Object>(List.of(actor.tenantId(), workspaceId, actor.actorId()));
        if (status != null) { sql.append(" and c.status = ?"); args.add(status); }
        if (cursorUpdatedAt != null) {
            sql.append(" and (c.updated_at < ? or (c.updated_at = ? and c.id < ?))");
            args.add(Timestamp.from(cursorUpdatedAt)); args.add(Timestamp.from(cursorUpdatedAt)); args.add(cursorId);
        }
        sql.append(" order by c.updated_at desc, c.id desc limit ?"); args.add(pageSize + 1);
        var items = jdbc.query(sql.toString(), (rs, row) -> mapConversation(rs), args.toArray());
        boolean more = items.size() > pageSize;
        if (more) items = new ArrayList<>(items.subList(0, pageSize));
        var last = more && !items.isEmpty() ? items.get(items.size() - 1) : null;
        return new ConversationPage(items, last == null ? null : last.updatedAt(), last == null ? null : last.id());
    }

    @Override
    @Transactional
    public ConversationSnapshot updateConversation(ActorContext actor, UUID workspaceId, UUID conversationId,
                                                   long expectedVersion, String title, String status) {
        requireConversationActor(actor, workspaceId, "task:create");
        var row = lockConversation(actor, workspaceId, conversationId);
        if (row.version() != expectedVersion)
            throw EafException.conflict("VERSION_CONFLICT", "会话版本已变化，请重新读取。");
        if (title == null && status == null) return getConversation(actor, workspaceId, conversationId);
        var nextTitle = title == null ? row.title() : title.trim();
        var nextStatus = status == null ? row.status() : status;
        if (nextTitle.isBlank() || nextTitle.length() > 200
                || !List.of("ACTIVE", "ARCHIVED").contains(nextStatus))
            throw EafException.invalid("会话标题或状态无效。");
        if ("ARCHIVED".equals(nextStatus) && hasActiveConversationTask(conversationId, null))
            throw EafException.conflict("CONVERSATION_BUSY", "有活动轮次时不能归档会话。");
        if (nextTitle.equals(row.title()) && nextStatus.equals(row.status()))
            return getConversation(actor, workspaceId, conversationId);
        var now = Instant.now(clock);
        jdbc.update("update task.conversation set title = ?, status = ?, row_version = row_version + 1, updated_at = ? where id = ?",
                nextTitle, nextStatus, Timestamp.from(now), conversationId);
        return getConversation(actor, workspaceId, conversationId);
    }

    @Override
    @Transactional
    public ConversationTurn createConversationTurn(CreateConversationTurnCommand command) {
        if (command == null || command.actor() == null || command.conversationId() == null || command.turnId() == null)
            throw EafException.invalid("会话轮次请求缺少身份或会话 ID。");
        requireConversationActor(command.actor(), command.workspaceId(), "task:create");
        if (command.input() == null || command.input().isBlank() || command.input().length() > 8_000
                || command.idempotencyKey() == null || command.idempotencyKey().isBlank()
                || command.idempotencyKey().length() > 200 || command.contextSnapshotJson() == null
                || command.contextSnapshotJson().length() > 24_000 || command.expectedBriefRevision() < 0
                || command.expectedHistoryThroughTurnNo() < 0 || command.includedTaskIds().size() > 3
                || command.includedTaskIds().stream().distinct().count() != command.includedTaskIds().size())
            throw EafException.invalid("会话轮次内容、上下文快照或幂等键无效。");
        var requestHash = Hashing.sha256(command.input());
        var key = conversationKey(command.actor(), command.workspaceId(),
                command.conversationId() + ":turn:" + command.idempotencyKey());
        var old = jdbc.query("select t.id, t.task_id, t.turn_no, t.input_text, t.brief_revision, t.history_through_turn_no, t.created_at, q.status, q.row_version, t.request_hash "
                        + "from task.conversation_turn t join task.task q on q.id = t.task_id where t.conversation_id = ? and t.idempotency_key = ?",
                rs -> rs.next() ? mapConversationTurn(rs, command.conversationId()) : null, command.conversationId(), key);
        if (old != null) {
            var oldHash = jdbc.queryForObject("select request_hash from task.conversation_turn where id = ?", String.class, old.id());
            if (!requestHash.equals(oldHash))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "会话轮次幂等键已绑定到不同输入。");
            return old;
        }

        var conversation = lockConversation(command.actor(), command.workspaceId(), command.conversationId());
        if (!"ACTIVE".equals(conversation.status()))
            throw EafException.conflict("CONVERSATION_ARCHIVED", "归档会话不能创建新轮次。");
        if (conversation.currentBriefRevision() != command.expectedBriefRevision()
                || conversation.lastTurnNo() != command.expectedHistoryThroughTurnNo())
            throw EafException.conflict("CONVERSATION_STATE_CHANGED", "会话上下文已变化，请重新读取后再发送。");
        if (hasActiveConversationTask(command.conversationId(), null))
            throw EafException.conflict("CONVERSATION_BUSY", "当前会话已有活动轮次。");

        var taskIdempotency = Hashing.sha256("conversation-task\u001f" + key);
        var entityType = "CUSTOMER_ASSISTANT".equals(conversation.mode()) ? "CUSTOMER" : "CONVERSATION_QA";
        var entityId = "CUSTOMER_ASSISTANT".equals(conversation.mode())
                ? conversation.customerId() : conversation.id().toString();
        var now = Instant.now(clock);
        var snapshot = create(new CreateTaskCommand(command.actor(), command.workspaceId(), conversation.agentId(),
                conversation.agentVersion(), command.input().trim(), entityType, entityId, taskIdempotency,
                UUID.randomUUID().toString(), "USER", new TaskAssetBinding(conversation.capabilityId(),
                conversation.capabilityVersion(), conversation.capabilityHash(), conversation.skillId(),
                conversation.skillVersion(), conversation.skillHash()), "REST"));
        var turnId = command.turnId();
        var turnNo = conversation.lastTurnNo() + 1;
        var inserted = jdbc.update("insert into task.conversation_turn(id, tenant_id, workspace_id, conversation_id, task_id, turn_no, input_text, brief_revision, history_through_turn_no, idempotency_key, request_hash, context_snapshot_json, included_task_ids, created_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?) on conflict (conversation_id, idempotency_key) do nothing",
                turnId, command.actor().tenantId(), command.workspaceId(), command.conversationId(), snapshot.id(), turnNo,
                command.input().trim(), conversation.currentBriefRevision(), conversation.lastTurnNo(), key, requestHash,
                command.contextSnapshotJson(), joinIds(command.includedTaskIds()), Timestamp.from(now));
        if (inserted == 0)
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "会话轮次已被并发请求创建，请查询原轮次。");
        jdbc.update("update task.conversation set last_turn_no = ?, row_version = row_version + 1, updated_at = ? where id = ?",
                turnNo, Timestamp.from(now), conversation.id());
        audit.append(new AuditFact("conversation-turn-created:" + turnId, command.actor().tenantId(),
                command.workspaceId(), command.actor().actorId(), snapshot.id(), "CONVERSATION_TURN_CREATED",
                "ACCEPTED", "{}", snapshot.traceId()));
        return new ConversationTurn(turnId, conversation.id(), snapshot.id(), turnNo, command.input().trim(),
                conversation.currentBriefRevision(), conversation.lastTurnNo(), snapshot.status().name(),
                snapshot.version(), now);
    }

    @Override
    public ConversationTurnPage listConversationTurns(ActorContext actor, UUID workspaceId, UUID conversationId,
                                                       Integer beforeTurnNo, int pageSize) {
        requireConversationActor(actor, workspaceId, "task:read");
        if (pageSize < 1 || pageSize > 50 || beforeTurnNo != null && beforeTurnNo < 1)
            throw EafException.invalid("会话轮次分页参数无效。");
        getConversation(actor, workspaceId, conversationId);
        var sql = new StringBuilder("select t.id, t.task_id, t.turn_no, t.input_text, t.brief_revision, t.history_through_turn_no, t.created_at, q.status, q.row_version "
                + "from task.conversation_turn t join task.task q on q.id = t.task_id where t.conversation_id = ?");
        var args = new ArrayList<Object>(); args.add(conversationId);
        if (beforeTurnNo != null) { sql.append(" and t.turn_no < ?"); args.add(beforeTurnNo); }
        sql.append(" order by t.turn_no desc limit ?"); args.add(pageSize + 1);
        var rows = jdbc.query(sql.toString(), (rs, row) -> mapConversationTurn(rs, conversationId), args.toArray());
        var more = rows.size() > pageSize;
        if (more) rows = new ArrayList<>(rows.subList(0, pageSize));
        var next = more && !rows.isEmpty() ? rows.get(rows.size() - 1).turnNo() : 0;
        rows = new ArrayList<>(rows);
        rows.sort(Comparator.comparingInt(ConversationTurn::turnNo));
        return new ConversationTurnPage(rows, next);
    }

    @Override
    public ConversationBrief getConversationBrief(ActorContext actor, UUID workspaceId, UUID conversationId) {
        var conversation = getConversation(actor, workspaceId, conversationId);
        return loadConversationBrief(conversation.id(), conversation.currentBriefRevision());
    }

    @Override
    @Transactional
    public ConversationBriefSave saveConversationBrief(SaveConversationBriefCommand command) {
        if (command == null || command.actor() == null || command.conversationId() == null
                || command.workspaceId() == null || command.expectedRevision() < 0
                || command.content() == null || command.content().length() > 2_000
                || command.idempotencyKey() == null || command.idempotencyKey().isBlank()
                || command.idempotencyKey().length() > 200 || command.sourceTurnIds().size() > 3)
            throw EafException.invalid("简报保存请求无效。");
        requireConversationActor(command.actor(), command.workspaceId(), "task:create");
        var conversation = lockConversation(command.actor(), command.workspaceId(), command.conversationId());
        var normalized = command.content().trim();
        var sourceIds = command.sourceTurnIds().stream().distinct().sorted().toList();
        if (sourceIds.size() != command.sourceTurnIds().size())
            throw EafException.invalid("简报来源轮次重复。");
        var requestHash = Hashing.sha256(String.join("\u001f", Integer.toString(command.expectedRevision()), normalized,
                String.valueOf(command.suggestionTaskId()), joinIds(sourceIds)));
        var saveKey = conversationKey(command.actor(), command.workspaceId(),
                command.conversationId() + ":brief:" + command.idempotencyKey());
        var old = jdbc.query("select request_hash, result_revision from task.conversation_brief_save where conversation_id = ? and save_key = ?",
                rs -> rs.next() ? new Object[]{rs.getString("request_hash"), rs.getInt("result_revision")} : null,
                command.conversationId(), saveKey);
        if (old != null) {
            if (!requestHash.equals(old[0]))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "简报保存键已绑定到不同请求。");
            var prior = loadConversationBrief(command.conversationId(), (Integer) old[1]);
            return new ConversationBriefSave(prior, conversation.currentBriefRevision(), false);
        }
        if (!"ACTIVE".equals(conversation.status()))
            throw EafException.conflict("CONVERSATION_ARCHIVED", "归档会话不能修改简报。");
        if (hasActiveConversationTask(command.conversationId(), null))
            throw EafException.conflict("CONVERSATION_BUSY", "有活动轮次时不能修改简报。");
        if (conversation.currentBriefRevision() != command.expectedRevision())
            throw EafException.conflict("BRIEF_REVISION_CONFLICT", "简报版本已变化，请重新读取。");

        if (command.suggestionTaskId() == null && !sourceIds.isEmpty())
            throw EafException.invalid("手工简报不能附带模型建议来源轮次。");
        if (command.suggestionTaskId() != null) {
            var source = jdbc.query("select ct.turn_no, ct.brief_revision, q.status from task.conversation_turn ct join task.task q on q.id = ct.task_id "
                            + "where ct.conversation_id = ? and ct.task_id = ? and ct.tenant_id = ? and ct.workspace_id = ? and q.actor_id = ?",
                    rs -> rs.next() ? new Object[]{rs.getInt("turn_no"), rs.getInt("brief_revision"), rs.getString("status")} : null,
                    command.conversationId(), command.suggestionTaskId(), command.actor().tenantId(), command.workspaceId(), command.actor().actorId());
            if (source == null || !TaskStatus.SUCCEEDED.name().equals(source[2])
                    || ((Integer) source[1]) != conversation.currentBriefRevision())
                throw EafException.conflict("BRIEF_SUGGESTION_STALE", "简报建议已失效或不属于当前会话。");
            if (!sourceIds.isEmpty()) {
                var validSources = jdbc.queryForObject("select count(*) from task.conversation_turn where conversation_id = ? and id = any(string_to_array(?, ',')::uuid[]) and turn_no <= ?",
                        Integer.class, command.conversationId(), joinIds(sourceIds), (Integer) source[0]);
                if (validSources != sourceIds.size())
                    throw EafException.invalid("简报建议包含不属于本会话的来源轮次。");
            }
            if (sourceIds.isEmpty()) throw EafException.invalid("接受简报建议时必须保留来源轮次。");
        }

        var changed = !loadConversationBrief(command.conversationId(), conversation.currentBriefRevision()).content().equals(normalized);
        var revision = conversation.currentBriefRevision();
        var now = Instant.now(clock);
        if (changed) {
            revision++;
            jdbc.update("insert into task.conversation_brief_revision(conversation_id, revision, content, source_task_id, source_turn_ids, confirmed_by, created_at) values (?, ?, ?, ?, ?, ?, ?)",
                    command.conversationId(), revision, normalized, command.suggestionTaskId(), joinIds(sourceIds),
                    command.actor().actorId(), Timestamp.from(now));
            jdbc.update("update task.conversation set current_brief_revision = ?, row_version = row_version + 1, updated_at = ? where id = ?",
                    revision, Timestamp.from(now), command.conversationId());
        }
        jdbc.update("insert into task.conversation_brief_save(conversation_id, save_key, request_hash, result_revision, created_at) values (?, ?, ?, ?, ?)",
                command.conversationId(), saveKey, requestHash, revision, Timestamp.from(now));
        return new ConversationBriefSave(loadConversationBrief(command.conversationId(), revision), revision, changed);
    }

    @Override
    public ConversationRuntimeContext conversationRuntimeContext(UUID taskId) {
        if (taskId == null) return null;
        return jdbc.query("select c.id conversation_id, ct.id turn_id, c.mode, c.customer_id, ct.brief_revision, ct.context_snapshot_json::text, ct.included_task_ids "
                        + "from task.conversation_turn ct join task.conversation c on c.id = ct.conversation_id where ct.task_id = ?",
                rs -> rs.next() ? new ConversationRuntimeContext(rs.getObject("conversation_id", UUID.class),
                        rs.getObject("turn_id", UUID.class), rs.getString("mode"), rs.getString("customer_id"),
                        rs.getInt("brief_revision"), rs.getString("context_snapshot_json"),
                        parseIds(rs.getString("included_task_ids"))) : null, taskId);
    }

    @Override
    public ConversationBinding conversationBinding(ActorContext actor, UUID workspaceId, UUID taskId) {
        requireConversationActor(actor, workspaceId, "task:read");
        return jdbc.query("select c.id, c.mode, c.customer_id, ct.brief_revision, c.current_brief_revision, ct.turn_no, c.status "
                        + "from task.conversation_turn ct join task.conversation c on c.id = ct.conversation_id "
                        + "where ct.task_id = ? and ct.tenant_id = ? and ct.workspace_id = ? and c.owner_id = ?",
                rs -> rs.next() ? new ConversationBinding(rs.getObject("id", UUID.class), rs.getString("mode"),
                        rs.getString("customer_id"), rs.getInt("brief_revision"), rs.getInt("current_brief_revision"),
                        rs.getInt("turn_no"), rs.getString("status")) : null,
                taskId, actor.tenantId(), workspaceId, actor.actorId());
    }

    @Override
    public void requireConversationTaskAccess(ActorContext actor, UUID workspaceId, UUID taskId) {
        requireCurrentActor(actor, workspaceId, "task:read");
        requireConversationTaskOwner(actor, workspaceId, taskId);
    }

    @Override
    public boolean isConversationTaskVisible(ActorContext actor, UUID workspaceId, UUID taskId) {
        requireCurrentActor(actor, workspaceId, "task:read");
        var privateConversation = isConversationTask(actor, workspaceId, taskId);
        var privateDraft = isExperienceDraftTask(actor.tenantId(), workspaceId, taskId);
        return !(privateConversation || privateDraft)
                || actor.type() == io.eaf.shared.ActorType.HUMAN && !actor.delegated()
                && (!privateConversation || isConversationTaskOwnedBy(actor, workspaceId, taskId))
                && (!privateDraft || isExperienceDraftTaskOwnedBy(actor, workspaceId, taskId));
    }

    private boolean isConversationTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        return jdbc.queryForObject("select exists(select 1 from task.conversation_turn where task_id = ? and tenant_id = ? and workspace_id = ?)",
                Boolean.class, taskId, actor.tenantId(), workspaceId);
    }

    private boolean isConversationTaskOwnedBy(ActorContext actor, UUID workspaceId, UUID taskId) {
        return jdbc.queryForObject("select exists(select 1 from task.conversation_turn ct join task.conversation c on c.id = ct.conversation_id "
                        + "where ct.task_id = ? and ct.tenant_id = ? and ct.workspace_id = ? and c.owner_id = ?)",
                Boolean.class, taskId, actor.tenantId(), workspaceId, actor.actorId());
    }

    // 会话 Task 是所有者私有数据；直接 TaskService 调用同样执行检查，避免 REST 投影外的入口泄漏。
    private void requireConversationTaskOwner(ActorContext actor, UUID workspaceId, UUID taskId) {
        if (isConversationTask(actor, workspaceId, taskId)
                && (actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated()
                || !isConversationTaskOwnedBy(actor, workspaceId, taskId))) throw EafException.notFound();
        requireExperienceDraftOwner(actor, workspaceId, taskId);
    }

    private boolean isExperienceDraftTask(UUID tenantId, UUID workspaceId, UUID taskId) {
        return Boolean.TRUE.equals(jdbc.queryForObject("select exists(select 1 from task.experience_draft_binding where task_id = ? and tenant_id = ? and workspace_id = ?)",
                Boolean.class, taskId, tenantId, workspaceId));
    }

    private boolean isExperienceDraftTaskOwnedBy(ActorContext actor, UUID workspaceId, UUID taskId) {
        return Boolean.TRUE.equals(jdbc.queryForObject("select exists(select 1 from task.experience_draft_binding where task_id = ? and tenant_id = ? and workspace_id = ? and owner_id = ?)",
                Boolean.class, taskId, actor.tenantId(), workspaceId, actor.actorId()));
    }

    private void requireExperienceDraftOwner(ActorContext actor, UUID workspaceId, UUID taskId) {
        if (isExperienceDraftTask(actor.tenantId(), workspaceId, taskId)
                && (actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated()
                || !isExperienceDraftTaskOwnedBy(actor, workspaceId, taskId))) throw EafException.notFound();
    }

    @Override
    @Transactional
    public void requireCurrentConversationFollowup(ActorContext actor, UUID workspaceId, UUID conversationId,
                                                   UUID taskId, long expectedTaskVersion, int expectedBriefRevision) {
        requireConversationActor(actor, workspaceId, "task:create");
        var conversation = lockConversation(actor, workspaceId, conversationId);
        var binding = conversationBinding(actor, workspaceId, taskId);
        if (binding == null || !conversationId.equals(binding.conversationId())
                || !"CUSTOMER_ASSISTANT".equals(binding.mode()))
            throw EafException.conflict("FOLLOWUP_SOURCE_INVALID", "跟进分析不属于当前客户会话。");
        var source = get(actor, workspaceId, taskId);
        if (source.version() != expectedTaskVersion || source.status() != TaskStatus.SUCCEEDED
                || !conversation.agentId().equals(source.agentId())
                || !conversation.agentVersion().equals(source.agentVersion()))
            throw EafException.conflict("FOLLOWUP_SOURCE_STALE", "客户分析任务已变化或尚未成功。");
        if (!"ACTIVE".equals(conversation.status()) || conversation.currentBriefRevision() != expectedBriefRevision
                || binding.briefRevision() != expectedBriefRevision || binding.currentBriefRevision() != expectedBriefRevision)
            throw EafException.conflict("FOLLOWUP_SOURCE_STALE", "会话资料已变化；请重新分析当前简报。");
        if (hasActiveConversationTask(conversationId, taskId))
            throw EafException.conflict("CONVERSATION_BUSY", "当前会话仍有活动轮次。");
        var newer = jdbc.queryForObject("select count(*) from task.conversation_turn ct join task.task q on q.id = ct.task_id "
                        + "where ct.conversation_id = ? and ct.turn_no > ? and ct.brief_revision = ? and q.agent_id = ? "
                        + "and q.status = 'SUCCEEDED' and coalesce(q.result_json->>'clarificationQuestion', '') = ''",
                Integer.class, conversationId, binding.turnNo(), expectedBriefRevision, conversation.agentId());
        if (newer > 0)
            throw EafException.conflict("FOLLOWUP_SOURCE_STALE", "已有更新的成功分析；请提交最新分析草稿。");
    }

    private void requireConversationActor(ActorContext actor, UUID workspaceId, String action) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("业务会话仅允许直接 HUMAN 所有者访问。");
        requireCurrentActor(actor, workspaceId, action);
    }

    private String conversationKey(ActorContext actor, UUID workspaceId, String key) {
        return Hashing.sha256(String.join("\u001f", actor.tenantId().toString(), workspaceId.toString(),
                actor.actorId().toString(), key));
    }

    private String conversationSelect() {
        return "select c.*, a.id active_task_id, a.status active_task_status from task.conversation c "
                + "left join lateral (select q.id, q.status from task.conversation_turn t join task.task q on q.id = t.task_id "
                + "where t.conversation_id = c.id and q.status not in ('SUCCEEDED','FAILED','TIMED_OUT','CANCELLED') "
                + "order by t.turn_no desc limit 1) a on true";
    }

    private ConversationSnapshot mapConversation(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new ConversationSnapshot(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("mode"),
                rs.getString("title"), rs.getString("customer_id"), rs.getObject("capability_id", UUID.class),
                rs.getString("capability_version"), rs.getString("capability_hash"), rs.getObject("agent_id", UUID.class),
                rs.getString("agent_version"), rs.getObject("skill_id", UUID.class), rs.getString("skill_version"),
                rs.getString("skill_hash"), rs.getString("status"), rs.getInt("current_brief_revision"),
                rs.getLong("row_version"), rs.getInt("last_turn_no"), rs.getObject("active_task_id", UUID.class),
                rs.getString("active_task_status"), rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    private ConversationLock lockConversation(ActorContext actor, UUID workspaceId, UUID conversationId) {
        var row = jdbc.query("select id, tenant_id, workspace_id, owner_id, mode, title, customer_id, capability_id, capability_version, capability_hash, agent_id, agent_version, skill_id, skill_version, skill_hash, status, current_brief_revision, row_version, last_turn_no from task.conversation where id = ? and tenant_id = ? and workspace_id = ? and owner_id = ? for update",
                rs -> rs.next() ? new ConversationLock(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("mode"),
                        rs.getString("title"), rs.getString("customer_id"), rs.getObject("capability_id", UUID.class),
                        rs.getString("capability_version"), rs.getString("capability_hash"), rs.getObject("agent_id", UUID.class),
                        rs.getString("agent_version"), rs.getObject("skill_id", UUID.class), rs.getString("skill_version"),
                        rs.getString("skill_hash"), rs.getString("status"), rs.getInt("current_brief_revision"),
                        rs.getLong("row_version"), rs.getInt("last_turn_no")) : null,
                conversationId, actor.tenantId(), workspaceId, actor.actorId());
        if (row == null) throw EafException.notFound();
        return row;
    }

    private boolean hasActiveConversationTask(UUID conversationId, UUID exceptTaskId) {
        var sql = "select count(*) from task.conversation_turn t join task.task q on q.id = t.task_id "
                + "where t.conversation_id = ? and q.status not in ('SUCCEEDED','FAILED','TIMED_OUT','CANCELLED')";
        return (exceptTaskId == null
                ? jdbc.queryForObject(sql, Integer.class, conversationId)
                : jdbc.queryForObject(sql + " and q.id <> ?", Integer.class, conversationId, exceptTaskId)) > 0;
    }

    private ConversationTurn mapConversationTurn(java.sql.ResultSet rs, UUID conversationId) throws java.sql.SQLException {
        return new ConversationTurn(rs.getObject("id", UUID.class), conversationId, rs.getObject("task_id", UUID.class),
                rs.getInt("turn_no"), rs.getString("input_text"), rs.getInt("brief_revision"),
                rs.getInt("history_through_turn_no"), rs.getString("status"), rs.getLong("row_version"),
                rs.getTimestamp("created_at").toInstant());
    }

    private ConversationBrief loadConversationBrief(UUID conversationId, int revision) {
        return jdbc.query("select conversation_id, revision, content, source_task_id, source_turn_ids, confirmed_by, created_at from task.conversation_brief_revision where conversation_id = ? and revision = ?",
                rs -> rs.next() ? new ConversationBrief(rs.getObject("conversation_id", UUID.class), rs.getInt("revision"),
                        rs.getString("content"), rs.getObject("source_task_id", UUID.class), parseIds(rs.getString("source_turn_ids")),
                        rs.getObject("confirmed_by", UUID.class), rs.getTimestamp("created_at").toInstant()) : null,
                conversationId, revision);
    }

    private String joinIds(List<UUID> ids) {
        return ids == null ? "" : ids.stream().map(UUID::toString).collect(Collectors.joining(","));
    }

    private List<UUID> parseIds(String ids) {
        if (ids == null || ids.isBlank()) return List.of();
        return java.util.Arrays.stream(ids.split(",")).map(UUID::fromString).toList();
    }

    private record ConversationLock(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String mode, String title,
                                    String customerId, UUID capabilityId, String capabilityVersion, String capabilityHash,
                                    UUID agentId, String agentVersion, UUID skillId, String skillVersion, String skillHash,
                                    String status, int currentBriefRevision, long version, int lastTurnNo) { }

    @Override
    @Transactional
    public TaskSnapshot createQualityRunTask(io.eaf.task.api.CreateQualityRunTaskCommand command) {
        if (command == null || command.qualityRunId() == null)
            throw EafException.invalid("质量运行 Task 缺少服务端运行标记。");
        requireQualityRunSource(command.actor(), command.workspaceId(), command.qualityRunId(), command.source());
        var scenarioRun = qualityRunSources.isScenarioRun(command.actor().tenantId(), command.workspaceId(),
                command.qualityRunId());
        var p23GenerationScope = qualityRunSources.isTeamImprovementRun(command.actor().tenantId(),
                command.workspaceId(), command.qualityRunId());
        var p23PreparationScope = qualityRunSources.isTeamPreparationRun(command.actor().tenantId(), command.workspaceId(),
                command.qualityRunId());
        var teamPreparationRun = p23PreparationScope && "TEAM_EXPERIENCE_PREPARATION".equals(command.businessEntityType());
        var teamImprovementRun = p23GenerationScope && !p23PreparationScope
                && "TEAM_EXPERIENCE_IMPROVEMENT".equals(command.businessEntityType());
        if ((p23GenerationScope || p23PreparationScope) && !teamPreparationRun && !teamImprovementRun)
            throw EafException.conflict("TEAM_IMPROVEMENT_TASK_BINDING_MISMATCH", "质量运行只能创建登记的生成或固定 prepare 样本 Task。");
        if (scenarioRun && (command.scenarioSampleId() == null || command.assetBinding() == null
                || !"EVALUATION".equals(command.source()) || command.deadlineAt() == null
                || !qualityRunSources.scenarioSampleMatches(command.actor().tenantId(), command.workspaceId(),
                command.actor().actorId(), command.qualityRunId(), command.scenarioSampleId(), command.idempotencyKey(),
                command.agentId(), command.agentVersion(), Hashing.sha256(command.input()), command.assetBinding())))
            throw EafException.conflict("SCENARIO_SAMPLE_BINDING_MISMATCH", "Task 不匹配 Evaluation 登记的样本、输入、资产或稳定键。");
        UUID improvementRunId = null;
        if (teamPreparationRun && (command.scenarioSampleId() == null || command.assetBinding() == null
                || !"EVALUATION".equals(command.source()) || command.deadlineAt() == null
                || !"TEAM_EXPERIENCE_PREPARATION".equals(command.businessEntityType())
                || command.businessEntityId() == null
                || !validPreparationSnapshotId(command.businessEntityId())
                || !qualityRunSources.teamPreparationSampleMatches(command.actor().tenantId(), command.workspaceId(),
                command.actor().actorId(), command.qualityRunId(), command.scenarioSampleId(), UUID.fromString(command.businessEntityId()), command.idempotencyKey(),
                command.agentId(), command.agentVersion(), Hashing.sha256(command.input()), command.assetBinding(), command.deadlineAt())))
            throw EafException.conflict("TEAM_PREPARATION_SAMPLE_BINDING_MISMATCH", "Task 不匹配固定快照、样本、侧别、输入、资产或期限。");
        if (teamImprovementRun) {
            try {
                if (!"TEAM_EXPERIENCE_IMPROVEMENT".equals(command.businessEntityType())
                        || command.businessEntityId() == null || command.scenarioSampleId() != null
                        || command.assetBinding() == null || command.deadlineAt() == null)
                    throw new IllegalArgumentException();
                improvementRunId = UUID.fromString(command.businessEntityId());
            } catch (RuntimeException invalidBinding) {
                throw EafException.invalid("Task 必须绑定登记的改进运行、固定资产和截止时间。");
            }
            if (!qualityRunSources.teamImprovementGenerationMatches(command.actor().tenantId(), command.workspaceId(),
                    command.actor().actorId(), command.qualityRunId(), improvementRunId, command.idempotencyKey(),
                    command.agentId(), command.agentVersion(), Hashing.sha256(command.input()), command.assetBinding(),
                    command.deadlineAt()))
                throw EafException.conflict("TEAM_IMPROVEMENT_TASK_BINDING_MISMATCH", "Task 与登记的唯一生成输入、资产或运行状态不一致。");
        }
        boolean p15ModelTask = SERVICE_REQUEST_PLAN_AGENT_ID.equals(command.agentId())
                && SERVICE_REQUEST_PLAN_CAPABILITY_ID.equals(command.assetBinding() == null ? null : command.assetBinding().capabilityId())
                && serviceRequestAnalysisVersion(command.agentVersion(), command.assetBinding() == null
                ? null : command.assetBinding().capabilityVersion());
        var modelSelection = command.modelSelection();
        if (modelSelection != null && (!scenarioRun || !p15ModelTask || command.scenarioSampleId() == null
                || !qualityRunSources.scenarioSampleModelProfileMatches(command.actor().tenantId(), command.workspaceId(),
                command.actor().actorId(), command.qualityRunId(), command.scenarioSampleId(), modelSelection)))
            throw EafException.conflict("SCENARIO_MODEL_PROFILE_BINDING_MISMATCH", "模型档位不匹配固定 P15 评测样本清单。");
        if (modelSelection == null && p15ModelTask) {
            if (modelProfiles == null) throw EafException.conflict("MODEL_PROFILE_CONFIGURATION_UNAVAILABLE", "模型档位目录未就绪。");
            var p15Agent = agents.requirePublished(command.actor().tenantId(), command.workspaceId(),
                    command.agentId(), command.agentVersion());
            modelSelection = modelProfiles.resolveForTask(p15Agent.modelProfileId(), null);
        }
        if (!scenarioRun && !teamPreparationRun && !teamImprovementRun && (command.scenarioSampleId() != null
                || command.assetBinding() != null || command.deadlineAt() != null))
            throw EafException.invalid("只有场景样本可指定场景绑定字段。");
        var task = new CreateTaskCommand(command.actor(), command.workspaceId(), command.agentId(), command.agentVersion(),
                command.input(), command.businessEntityType(), command.businessEntityId(), command.idempotencyKey(),
                command.traceId(), command.source(), command.assetBinding());
        var created = create(task, command.qualityRunId(), false, null, modelSelection);
        if (scenarioRun || teamPreparationRun) jdbc.update("update task.task set deadline_at = least(deadline_at, ?), "
                        + "active_deadline_at = least(active_deadline_at, ?) where id = ?",
                Timestamp.from(command.deadlineAt()), Timestamp.from(command.deadlineAt()), created.id());
        if (teamImprovementRun) {
            jdbc.update("update task.task set deadline_at = least(deadline_at, ?), active_deadline_at = least(active_deadline_at, ?) where id = ?",
                    Timestamp.from(command.deadlineAt()), Timestamp.from(command.deadlineAt()), created.id());
            if (!qualityRunSources.bindTeamImprovementGenerationTask(command.actor().tenantId(), command.workspaceId(),
                    command.actor().actorId(), command.qualityRunId(), improvementRunId, created.id(), created.attempt(),
                    command.idempotencyKey(), command.agentId(), command.agentVersion(), Hashing.sha256(command.input()),
                    command.assetBinding(), command.deadlineAt()))
                throw EafException.conflict("TEAM_IMPROVEMENT_TASK_BINDING_MISMATCH", "无法固定生成 Task 的唯一来源。");
        }
        if (teamPreparationRun && !qualityRunSources.bindTeamPreparationSample(command.actor().tenantId(),
                command.workspaceId(), command.actor().actorId(), command.qualityRunId(), command.scenarioSampleId(),
                UUID.fromString(command.businessEntityId()), created.id(), created.attempt(), command.idempotencyKey(), command.agentId(), command.agentVersion(),
                Hashing.sha256(command.input()), command.assetBinding(), command.deadlineAt()))
            throw EafException.conflict("TEAM_PREPARATION_SAMPLE_BINDING_MISMATCH", "无法固定样本 Task 来源。");
        return created;
    }

    private boolean validPreparationSnapshotId(String value) {
        try { UUID.fromString(value); return true; }
        catch (RuntimeException invalid) { return false; }
    }

    @Override
    public void requireQualityRunSource(ActorContext actor, UUID workspaceId, UUID qualityRunId, String source) {
        if (qualityRunId == null || source == null || !List.of("USER", "EVALUATION").contains(source))
            throw EafException.invalid("质量运行来源验证请求无效。");
        requireCurrentActor(actor, workspaceId, "evaluation:run");
        // 质量运行 ID 由 Evaluation 签发；来源不匹配时必须在创建预算、Task 或外部执行前失败。
        if (!qualityRunSources.sourceMatches(actor.tenantId(), workspaceId, qualityRunId, source))
            throw EafException.conflict("QUALITY_RUN_SOURCE_MISMATCH", "Task 来源与 Evaluation 质量运行登记不一致。");
    }

    @Override
    public void requireQualityRunWorkflow(ActorContext actor, UUID workspaceId, UUID qualityRunId, String source,
                                          UUID workflowId, String workflowVersion) {
        if (workflowId == null || workflowVersion == null || workflowVersion.isBlank())
            throw EafException.invalid("质量 Workflow 版本绑定不完整。");
        requireQualityRunSource(actor, workspaceId, qualityRunId, source);
        // 先校验质量运行来源，再核对 Evaluation 清单；拒绝发生在 Workflow 创建预算之前。
        if (!qualityRunSources.workflowMatches(actor.tenantId(), workspaceId, qualityRunId,
                workflowId, workflowVersion))
            throw EafException.conflict("QUALITY_RUN_WORKFLOW_MISMATCH", "Workflow 版本不在该协作评测运行的固定清单内。");
    }

    @Override
    public void requireEvaluationReviewerTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        workspaces.require(actor, workspaceId, "evaluation:run");
        var binding = jdbc.query("select source, quality_run_id, run_kind, tool_name, tool_version, tool_binding_ref, "
                        + "workflow_instance_id, workflow_id, workflow_version, workflow_step_id, root_task_id, parent_task_id "
                        + "from task.task where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new EvaluationReviewerTaskBinding(rs.getString("source"),
                        rs.getObject("quality_run_id", UUID.class), rs.getString("run_kind"), rs.getString("tool_name"),
                        rs.getString("tool_version"), rs.getString("tool_binding_ref"),
                        rs.getObject("workflow_instance_id", UUID.class), rs.getObject("workflow_id", UUID.class),
                        rs.getString("workflow_version"), rs.getString("workflow_step_id"),
                        rs.getObject("root_task_id", UUID.class), rs.getObject("parent_task_id", UUID.class)) : null,
                taskId, actor.tenantId(), workspaceId);
        // 只信任 Task 域内已经落库的父子关系；reviewer 必须挂在同一评测运行、同一 Workflow 实例的 analyze 根 Task 下。
        var root = binding == null || binding.rootTaskId() == null ? null : jdbc.query(
                "select id, root_task_id, source, quality_run_id, run_kind, workflow_instance_id, workflow_id, "
                        + "workflow_version, workflow_step_id from task.task where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new EvaluationReviewerRootBinding(rs.getObject("id", UUID.class),
                        rs.getObject("root_task_id", UUID.class), rs.getString("source"),
                        rs.getObject("quality_run_id", UUID.class), rs.getString("run_kind"),
                        rs.getObject("workflow_instance_id", UUID.class), rs.getObject("workflow_id", UUID.class),
                        rs.getString("workflow_version"), rs.getString("workflow_step_id")) : null,
                binding.rootTaskId(), actor.tenantId(), workspaceId);
        if (binding == null || !"EVALUATION".equals(binding.source()) || binding.qualityRunId() == null
                || !"TOOL_EXECUTION".equals(binding.runKind())
                || !"agent.risk.review.evaluation".equals(binding.toolName()) || !"1.0.0".equals(binding.toolVersion())
                || !"A2A_EVALUATION_REVIEW_PEER:risk-review-evaluation@1.0.0".equals(binding.toolBindingRef())
                || binding.workflowInstanceId() == null || binding.workflowId() == null || binding.workflowVersion() == null
                || !"peer-review".equals(binding.workflowStepId())
                || binding.rootTaskId() == null || !binding.rootTaskId().equals(binding.parentTaskId()) || root == null
                || !binding.rootTaskId().equals(root.id()) || !root.id().equals(root.rootTaskId())
                || !"EVALUATION".equals(root.source()) || !binding.qualityRunId().equals(root.qualityRunId())
                || !"AGENT".equals(root.runKind()) || !binding.workflowInstanceId().equals(root.workflowInstanceId())
                || !binding.workflowId().equals(root.workflowId()) || !binding.workflowVersion().equals(root.workflowVersion())
                || !"analyze".equals(root.workflowStepId())
                || !qualityRunSources.collaborationReviewerStepMatches(actor.tenantId(), workspaceId, binding.qualityRunId(),
                        binding.workflowId(), binding.workflowVersion(), binding.workflowStepId()))
            throw EafException.forbidden("评测 reviewer 必须属于固定 Workflow 的 peer-review 步骤和同一实例 analyze 根 Task。");
    }

    private TaskSnapshot create(CreateTaskCommand c, UUID qualityRunId) {
        return create(c, qualityRunId, false, null, null);
    }

    private TaskSnapshot create(CreateTaskCommand c, UUID qualityRunId, boolean experienceDraftEntry) {
        return create(c, qualityRunId, experienceDraftEntry, null, null);
    }

    private TaskSnapshot create(CreateTaskCommand c, UUID qualityRunId, boolean experienceDraftEntry,
                                CreateAutomationReadTaskCommand automationCommand) {
        return create(c, qualityRunId, experienceDraftEntry, automationCommand, null);
    }

    private TaskSnapshot create(CreateTaskCommand c, UUID qualityRunId, boolean experienceDraftEntry,
                                CreateAutomationReadTaskCommand automationCommand,
                                ModelProfileSelection trustedModelSelection) {
        if (c == null || c.actor() == null) throw EafException.invalid("Task 身份上下文缺失。");
        if (c.input() == null || c.input().isBlank()) throw EafException.invalid("input 不能为空。");
        if (c.input().length() > 8_000) throw EafException.invalid("input 超过 8,000 字符限制。");
        if (c.idempotencyKey() == null || c.idempotencyKey().isBlank()) throw EafException.invalid("Idempotency-Key 必填。");
        if (!"USER".equals(c.source()) && !"EVALUATION".equals(c.source())) throw EafException.invalid("Task source 无效。");
        if (!List.of("REST", "MCP", "A2A").contains(c.entryProtocol())) throw EafException.invalid("Task entryProtocol 无效。");
        if (c.actor().delegated() && (!"USER".equals(c.source())
                || !validDelegation(c.actor(), c.workspaceId(), c.entryProtocol())))
            throw EafException.forbidden("委托 Task 必须使用当前有效的一跳 USER 委托身份。");
        var p30Asset = P30_AGENT_ID.equals(c.agentId()) || c.assetBinding() != null
                && (P30_CAPABILITY_ID.equals(c.assetBinding().capabilityId()) || P30_SKILL_ID.equals(c.assetBinding().skillId()));
        if (p30Asset != (automationCommand != null))
            throw EafException.forbidden("P30 固定摘要 Agent/Capability 只能由已登记自动化运行创建。");
        if (automationCommand != null && (!P30_AGENT_ID.equals(c.agentId()) || !P30_CAPABILITY_ID.equals(c.assetBinding().capabilityId())
                || !P30_SKILL_ID.equals(c.assetBinding().skillId()) || !c.idempotencyKey().equals("p30-run:" + automationCommand.runId())
                || c.businessEntityType() != null || c.businessEntityId() != null || !"USER".equals(c.source())))
            throw EafException.forbidden("P30 摘要 Task 必须保持固定 USER 根任务和原运行身份。");
        boolean controlledEvaluation = qualityRunId != null
                && (qualityRunSources.isScenarioRun(c.actor().tenantId(), c.workspaceId(), qualityRunId)
                || qualityRunSources.isTeamImprovementRun(c.actor().tenantId(), c.workspaceId(), qualityRunId));
        validateAssetBinding(c.assetBinding(), c.source(), controlledEvaluation);
        var access = workspaces.require(c.actor(), c.workspaceId(), "task:create");
        var taskAdmissionOpen = operationalControl.taskAdmissionOpen(c.actor().tenantId(), access.workspaceId());
        AgentDefinition agent = agents.requirePublished(c.actor().tenantId(), access.workspaceId(), c.agentId(), c.agentVersion());
        if (c.actor().delegated() && IdentityService.MCP_AUDIENCE.equals(c.actor().delegationAudience())
                && !validMcpReadonlyTask(c.actor(), c.workspaceId(), c.entryProtocol(), c.source(),
                c.businessEntityType(), c.businessEntityId(), agent.id(), agent.version(), c.assetBinding()))
            throw EafException.forbidden("MCP 委托只能创建绑定范围内的只读服务请求 Task。");
        if ("EXPERIENCE_DRAFT_V1".equals(agent.responseProfile()) != experienceDraftEntry)
            throw EafException.forbidden("EXPERIENCE_DRAFT_V1 只能通过带来源绑定的个人整理入口创建。");
        boolean p32P15Task = isP32P15Task(c, agent);
        if (c.modelProfileRef() != null && !p32P15Task)
            throw EafException.forbidden("显式模型档位只允许本人通过 REST 创建固定 P15 只读根 Task。");
        if (c.modelProfileRef() != null && (c.modelProfileRef().profileId() == null
                || c.modelProfileRef().version() == null || c.modelProfileRef().version().isBlank()))
            throw EafException.invalid("modelProfileRef 必须包含精确 profileId 和 version。");
        var now = Instant.now(clock);
        var id = UUID.randomUUID();
        // 幂等摘要绑定入口协议，避免同一业务键在 REST 与 MCP 之间跨入口重放。
        var hash = Hashing.sha256(String.join("\u001f", c.agentId().toString(), c.agentVersion(), c.input(),
                String.valueOf(c.businessEntityType()), String.valueOf(c.businessEntityId()), bindingHash(c.assetBinding()),
                c.source(), c.entryProtocol(), "AGENT",
                c.actor().actorId().toString(), String.valueOf(c.actor().principalId()),
                String.valueOf(c.actor().delegationId()), String.valueOf(c.actor().authorizationHash())));
        // DEFAULT 保留旧摘要以便迁移前请求可重放；显式 ref 进入摘要并与默认请求隔离。
        if (c.modelProfileRef() != null)
            hash = Hashing.sha256(hash + "\u001fmodel-profile:EXPLICIT:" + c.modelProfileRef().profileId()
                    + "@" + c.modelProfileRef().version());
        if (trustedModelSelection != null && "EXPLICIT".equals(trustedModelSelection.selectionKind())) {
            var selected = trustedModelSelection.effectiveProfile();
            hash = Hashing.sha256(hash + "\u001fmodel-profile:EXPLICIT:" + selected.profileId()
                    + "@" + selected.version() + ":" + selected.configurationHash());
        }
        if (qualityRunId != null) hash = Hashing.sha256(hash + "\u001fquality-run:" + qualityRunId);
        var storedKey = storedIdempotencyKey(c.actor(), c.workspaceId(), c.idempotencyKey());
        var existing = findByIdempotency(c.actor(), c.workspaceId(), storedKey);
        if (existing.isPresent()) {
            if (automationCommand == null) return existingTask(c.actor(), c.workspaceId(), storedKey, hash);
            if (!existing.get().requestHash().equals(hash))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一自动化运行键对应不同 Task 输入。");
            requireAutomationBinding(existing.get().id(), automationCommand);
            return readTask(c.actor(), c.workspaceId(), existing.get().id());
        }
        if (!taskAdmissionOpen) throw taskAdmissionStopped();
        var modelSelection = trustedModelSelection;
        if (modelSelection == null && qualityRunId == null && p32P15Task) {
            if (modelProfiles == null)
                throw EafException.conflict("MODEL_PROFILE_CONFIGURATION_UNAVAILABLE", "模型档位目录未就绪。");
            modelSelection = modelProfiles.resolveForTask(agent.modelProfileId(), c.modelProfileRef());
        }
        if (trustedModelSelection != null) {
            if (qualityRunId == null || modelProfiles == null
                    || !agent.modelProfileId().equals(trustedModelSelection.assetDefaultProfileId())
                    || trustedModelSelection.effectiveProfile() == null)
                throw EafException.forbidden("评测模型档位选择必须绑定已登记的 P15 Agent 默认档位。");
            modelProfiles.requireCurrent(trustedModelSelection.effectiveProfile());
        }
        if (insertTask(id, c.actor(), c.workspaceId(), agent, c.input(), c.businessEntityType(), c.businessEntityId(),
                storedKey, hash, c.traceId(), c.source(), c.assetBinding(), id, null, c.entryProtocol(), "AGENT", null, null,
                qualityRunId, null, now, now.plus(lifetime), modelSelection) == 0) {
            if (automationCommand == null) return existingTask(c.actor(), c.workspaceId(), storedKey, hash);
            var concurrent = findByIdempotency(c.actor(), c.workspaceId(), storedKey)
                    .orElseThrow(() -> EafException.conflict("IDEMPOTENCY_CONFLICT", "自动化 Task 创建竞争未能恢复原记录。"));
            if (!concurrent.requestHash().equals(hash))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一自动化运行键对应不同 Task 输入。");
            requireAutomationBinding(concurrent.id(), automationCommand);
            return readTask(c.actor(), c.workspaceId(), concurrent.id());
        }
        jdbc.update("insert into task.budget_scope(scope_id, tenant_id, workspace_id, root_task_id, max_steps, steps_used, max_model_calls, model_calls, max_tool_calls, tool_calls, max_tokens, token_used, token_reserved, max_active_ms, active_used_ms) values (?, ?, ?, ?, 24, 0, 8, 0, 16, 0, 8000, 0, 0, ?, 0)",
                id, c.actor().tenantId(), c.workspaceId(), id, activeDuration.toMillis());
        if (automationCommand != null)
            jdbc.update("insert into task.automation_task_binding(task_id, tenant_id, workspace_id, owner_id, subscription_id, run_id, authorization_epoch, input_hash, profile_hash) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), automationCommand.subscriptionId(),
                    automationCommand.runId(), automationCommand.authorizationEpoch(), automationCommand.inputHash(), automationCommand.profileHash());
        audit.append(new AuditFact("task-created:" + id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), id, "TASK_CREATED", "ACCEPTED", "{}", c.traceId()));
        return automationCommand == null ? get(c.actor(), c.workspaceId(), id) : readTask(c.actor(), c.workspaceId(), id);
    }

    private void requireAutomationBinding(UUID taskId, CreateAutomationReadTaskCommand command) {
        var binding = jdbc.query("select owner_id, subscription_id, run_id, authorization_epoch, input_hash, profile_hash "
                        + "from task.automation_task_binding where task_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("owner_id", UUID.class),
                        rs.getObject("subscription_id", UUID.class), rs.getObject("run_id", UUID.class),
                        rs.getLong("authorization_epoch"), rs.getString("input_hash"), rs.getString("profile_hash")} : null,
                taskId);
        if (binding == null || !command.actor().actorId().equals(binding[0])
                || !command.subscriptionId().equals(binding[1]) || !command.runId().equals(binding[2])
                || command.authorizationEpoch() != (long) binding[3]
                || !command.inputHash().equals(binding[4]) || !command.profileHash().equals(binding[5]))
            throw EafException.conflict("AUTOMATION_TASK_BINDING_CONFLICT", "原自动化运行已绑定到不同 Task 或证据。");
    }

    @Override
    @Transactional
    public BudgetScopeReference createBudgetScope(CreateBudgetScopeCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null
                || command.idempotencyKey() == null || command.idempotencyKey().isBlank()
                || command.idempotencyKey().length() > 200
                || command.requestHash() == null || !command.requestHash().matches("[0-9a-f]{64}"))
            throw EafException.invalid("预算范围创建请求或幂等绑定无效。");
        requireCurrentActor(command.actor(), command.workspaceId(), "task:create");
        var taskAdmissionOpen = operationalControl.taskAdmissionOpen(command.actor().tenantId(), command.workspaceId());
        // 数据库只保存作用域化后的稳定键；重放必须与原请求摘要完全相同。
        var storedKey = Hashing.sha256(String.join("\u001f", command.actor().tenantId().toString(),
                command.workspaceId().toString(), command.actor().actorId().toString(), command.idempotencyKey()));
        var prior = jdbc.query("select scope_id, creation_hash from task.budget_scope where tenant_id = ? and workspace_id = ? and created_by = ? and creation_key = ?",
                rs -> rs.next() ? new ExistingBudgetScope(rs.getObject("scope_id", UUID.class), rs.getString("creation_hash")) : null,
                command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), storedKey);
        if (prior != null) {
            if (!command.requestHash().equals(prior.requestHash()))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一预算幂等键对应了不同请求。");
            return new BudgetScopeReference(prior.id());
        }
        if (!taskAdmissionOpen) throw taskAdmissionStopped();
        var scopeId = UUID.randomUUID();
        var inserted = jdbc.update("insert into task.budget_scope(scope_id, tenant_id, workspace_id, root_task_id, max_steps, steps_used, max_model_calls, model_calls, max_tool_calls, tool_calls, max_tokens, token_used, token_reserved, max_active_ms, active_used_ms, created_by, creation_key, creation_hash) "
                        + "values (?, ?, ?, null, 24, 0, 8, 0, 16, 0, 8000, 0, 0, ?, 0, ?, ?, ?) "
                        + "on conflict (tenant_id, workspace_id, created_by, creation_key) do nothing",
                scopeId, command.actor().tenantId(), command.workspaceId(), activeDuration.toMillis(),
                command.actor().actorId(), storedKey, command.requestHash());
        if (inserted == 1) return new BudgetScopeReference(scopeId);
        var existing = jdbc.query("select scope_id, creation_hash from task.budget_scope where tenant_id = ? and workspace_id = ? and created_by = ? and creation_key = ?",
                rs -> rs.next() ? new ExistingBudgetScope(rs.getObject("scope_id", UUID.class), rs.getString("creation_hash")) : null,
                command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), storedKey);
        if (existing == null) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "预算范围幂等键已占用。");
        if (!command.requestHash().equals(existing.requestHash()))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一预算幂等键对应了不同请求。");
        return new BudgetScopeReference(existing.id());
    }

    @Override
    @Transactional
    public TaskSnapshot createWorkflowTask(CreateWorkflowTaskCommand c) {
        validateChildCommand(c == null ? null : c.actor(), c == null ? null : c.dispatchKey(), c == null ? null : c.input());
        if (c.workspaceId() == null || c.budgetScopeId() == null || c.agentId() == null
                || c.agentVersion() == null || c.source() == null || c.assetBinding() == null
                || (c.toolName() == null) != (c.toolVersion() == null)
                || (c.toolName() == null) != (c.toolArgumentsJson() == null))
            throw EafException.invalid("Workflow Task 绑定字段不完整。");
        if (!"USER".equals(c.source()) && !"EVALUATION".equals(c.source()))
            throw EafException.invalid("Workflow Task source 无效。");
        if (P30_AGENT_ID.equals(c.agentId()) || P30_CAPABILITY_ID.equals(c.assetBinding().capabilityId())
                || P30_SKILL_ID.equals(c.assetBinding().skillId()))
            throw EafException.forbidden("P30 固定摘要 Agent/Capability 不能通过 Workflow Task 入口创建。");
        if (c.actor().delegated() && (!"USER".equals(c.source())
                || !validDelegation(c.actor(), c.workspaceId(), "REST")))
            throw EafException.forbidden("委托 Workflow Task 必须保留当前有效的一跳 USER 委托身份。");
        requireCurrentActor(c.actor(), c.workspaceId(), "task:create");
        var taskAdmissionOpen = operationalControl.taskAdmissionOpen(c.actor().tenantId(), c.workspaceId());
        if ("EVALUATION".equals(c.source())) workspaces.require(c.actor(), c.workspaceId(), "evaluation:run");
        if (c.qualityRunId() != null) workspaces.require(c.actor(), c.workspaceId(), "evaluation:run");
        if (c.qualityRunId() != null) {
            var provenance = c.workflowProvenance();
            if (provenance == null || !provenance.complete())
                throw EafException.invalid("带质量运行标记的 Workflow Task 必须绑定 Workflow 实例和步骤。");
            requireQualityRunSource(c.actor(), c.workspaceId(), c.qualityRunId(), c.source());
            if (!qualityRunSources.workflowStepMatches(c.actor().tenantId(), c.workspaceId(), c.qualityRunId(),
                    provenance.workflowId(), provenance.workflowVersion(), provenance.stepId()))
                throw EafException.conflict("QUALITY_RUN_WORKFLOW_STEP_MISMATCH", "Workflow Task 步骤不在质量运行固定清单内。");
        }
        if (c.workflowProvenance() != null
                && c.workflowProvenance().workflowId() != null
                && UUID.fromString("58000000-0000-4000-8000-000000000018").equals(c.workflowProvenance().workflowId())) {
            var knowledge = "gather-knowledge".equals(c.workflowProvenance().stepId());
            var expectedAgent = UUID.fromString(knowledge ? "20000000-0000-4000-8000-000000000018"
                    : "20000000-0000-4000-8000-000000000019");
            var expectedCapability = UUID.fromString(knowledge ? "54000000-0000-4000-8000-000000000016"
                    : "54000000-0000-4000-8000-000000000017");
            if (!c.workflowProvenance().p21ParallelBranch() || c.actor().type() != io.eaf.shared.ActorType.HUMAN
                    || c.actor().delegated() || !"USER".equals(c.source()) || c.qualityRunId() != null
                    || c.toolName() != null || !expectedAgent.equals(c.agentId())
                    || !expectedCapability.equals(c.assetBinding().capabilityId()))
                throw EafException.forbidden("分支 Task 必须由固定批次 Workflow 使用对应只读角色创建。");
        }
        var p29Workflow = c.workflowProvenance() != null && UUID.fromString("58000000-0000-4000-8000-00000000001d")
                .equals(c.workflowProvenance().workflowId());
        var p29Asset = UUID.fromString("20000000-0000-4000-8000-000000000023").equals(c.agentId())
                || c.assetBinding() != null && UUID.fromString("54000000-0000-4000-8000-000000000023")
                        .equals(c.assetBinding().capabilityId());
        if (p29Asset != p29Workflow) throw EafException.forbidden("P29 固定 Agent/Capability 只能用于固定项目简报 Workflow。");
        if (p29Workflow) {
            if (p29Sources == null || c.actor().type() != io.eaf.shared.ActorType.HUMAN || c.actor().delegated()
                    || !"prepare".equals(c.workflowProvenance().stepId())
                    || !"1.0.0".equals(c.workflowProvenance().workflowVersion())
                    || !UUID.fromString("20000000-0000-4000-8000-000000000023").equals(c.agentId())
                    || !"1.0.0".equals(c.agentVersion()) || c.assetBinding() == null
                    || !UUID.fromString("54000000-0000-4000-8000-000000000023").equals(c.assetBinding().capabilityId())
                    || !"1.0.0".equals(c.assetBinding().capabilityVersion()) || c.toolName() != null
                    || c.qualityRunId() != null || !"USER".equals(c.source()))
                throw EafException.forbidden("P29 Task 必须使用固定只读能力和 USER prepare 步骤。");
            p29Sources.requireTaskCreation(c);
        }
        validateAssetBinding(c.assetBinding(), c.source(), true);
        var agent = agents.requirePublished(c.actor().tenantId(), c.workspaceId(), c.agentId(), c.agentVersion());
        ToolDefinition tool = null;
        if (c.toolName() != null) {
            tool = tools.requirePublished(c.actor().tenantId(), c.workspaceId(), c.toolName(), c.toolVersion());
            if (tool.bindingRef() == null || tool.bindingRef().isBlank())
                throw EafException.conflict("TOOL_BINDING_MISSING", "固定工具没有已发布连接绑定。");
            if ("EVALUATION".equals(c.source()) && "WRITE".equals(tool.effect()))
                throw EafException.forbidden("EVALUATION Workflow 不能创建写入工具 Task。");
            if ("crm.followup.result.record".equals(c.toolName()))
                customerFollowups.requireSyncTaskCreation(c.actor(), c.workspaceId(), c.workflowProvenance(),
                        c.toolName(), c.toolArgumentsJson());
            if (isP27Tool(c.toolName())) requireP27WorkflowTaskCreation(c);
            if (SERVICE_REQUEST_TOOL.equals(c.toolName())) {
                if (!SERVICE_REQUEST_REGISTER_AGENT_ID.equals(c.agentId()) || !"1.0.0".equals(c.agentVersion())
                        || c.assetBinding() == null || !SERVICE_REQUEST_REGISTER_CAPABILITY_ID.equals(c.assetBinding().capabilityId())
                        || !"1.0.0".equals(c.assetBinding().capabilityVersion()))
                    throw EafException.forbidden("服务请求 Tool Task 必须使用固定登记 Agent 与 Capability。");
                requireServiceRequestTaskCreation(c.actor(), c.workspaceId(), c.workflowProvenance(),
                        c.toolName(), c.toolVersion(), c.toolArgumentsJson());
            }
        }

        var key = storedIdempotencyKey(c.actor(), c.workspaceId(), "workflow:" + c.dispatchKey());
        var rootTaskId = c.rootTaskId();
        ParentTask root = null;
        if (rootTaskId != null) {
            root = requireParent(c.actor(), c.workspaceId(), rootTaskId);
            if (!rootTaskId.equals(root.rootTaskId()) || !c.source().equals(root.source())
                    || !java.util.Objects.equals(c.qualityRunId(), root.qualityRunId()))
                throw EafException.conflict("WORKFLOW_ROOT_CONFLICT", "Workflow 根 Task 来源或关系不匹配。");
            var scopeRoot = budgetScopeRoot(c.actor().tenantId(), c.workspaceId(), c.budgetScopeId());
            if (!rootTaskId.equals(scopeRoot))
                throw EafException.conflict("WORKFLOW_BUDGET_CONFLICT", "Workflow 子 Task 未绑定实例预算范围。");
        }
        var runKind = tool == null ? "AGENT" : "TOOL_EXECUTION";
        var entityType = tool == null ? "WORKFLOW_CAPABILITY" : "WORKFLOW_TOOL";
        var entityId = tool == null ? c.assetBinding().capabilityId() + "@" + c.assetBinding().capabilityVersion()
                : tool.name() + "@" + tool.version();
        var hash = Hashing.sha256(String.join("\u001f", c.budgetScopeId().toString(), String.valueOf(rootTaskId),
                agent.id().toString(), agent.version(), c.input(), c.source(), bindingHash(c.assetBinding()),
                runKind, String.valueOf(c.toolName()), String.valueOf(c.toolVersion()), String.valueOf(c.toolArgumentsJson()),
                String.valueOf(c.workflowProvenance()),
                c.actor().actorId().toString(), String.valueOf(c.actor().principalId()),
                String.valueOf(c.actor().delegationId()), String.valueOf(c.actor().authorizationHash())));
        if (c.qualityRunId() != null) hash = Hashing.sha256(hash + "\u001fquality-run:" + c.qualityRunId());
        var dispatch = lockWorkflowDispatch(c.actor(), c.workspaceId(), c.dispatchKey(), hash);
        if (dispatch.taskId() != null && (dispatch.state().equals("ACTIVE") || dispatch.state().equals("CANCEL_REQUESTED")))
            return existingTask(c.actor(), c.workspaceId(), key, hash);
        if (findByIdempotency(c.actor(), c.workspaceId(), key).isPresent()) {
            var existing = existingTask(c.actor(), c.workspaceId(), key, hash);
            jdbc.update("update task.workflow_dispatch set task_id = ?, updated_at = now() where tenant_id = ? and workspace_id = ? and actor_id = ? and dispatch_key = ? and task_id is null",
                    existing.id(), c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.dispatchKey());
            return existing;
        }
        if (!"ACTIVE".equals(dispatch.state()))
            throw EafException.conflict("WORKFLOW_CHILD_CANCELLED", "Workflow 步骤已取消，不能再创建子 Task。");
        if (!taskAdmissionOpen) throw taskAdmissionStopped();

        var scope = jdbc.query("select root_task_id from task.budget_scope where scope_id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? rs.getObject("root_task_id", UUID.class) : null,
                c.budgetScopeId(), c.actor().tenantId(), c.workspaceId());
        if (scope == null && jdbc.queryForObject("select count(*) from task.budget_scope where scope_id = ? and tenant_id = ? and workspace_id = ?",
                Integer.class, c.budgetScopeId(), c.actor().tenantId(), c.workspaceId()) == 0)
            throw EafException.notFound();
        if (rootTaskId == null && scope != null)
            throw EafException.conflict("WORKFLOW_BUDGET_CONFLICT", "实例预算已绑定到其他根 Task。");
        if (rootTaskId != null && !rootTaskId.equals(scope))
            throw EafException.conflict("WORKFLOW_BUDGET_CONFLICT", "Workflow 根 Task 与实例预算范围不匹配。");

        var now = Instant.now(clock);
        var id = UUID.randomUUID();
        var actualRoot = rootTaskId == null ? id : rootTaskId;
        var parentId = rootTaskId;
        var traceId = traceOrParent(c.traceId(), root == null ? null : root.traceId());
        if (insertTask(id, c.actor(), c.workspaceId(), agent, c.input(), entityType, entityId, key, hash, traceId,
                c.source(), c.assetBinding(), actualRoot, parentId, root == null ? "REST" : root.entryProtocol(),
                runKind, tool, c.toolArgumentsJson(), c.qualityRunId(), c.workflowProvenance(), now, now.plus(lifetime), null) == 0)
            return existingTask(c.actor(), c.workspaceId(), key, hash);
        if (c.workflowProvenance() != null && c.workflowProvenance().p21ParallelBranch()) {
            var slice = jdbc.queryForObject("select max_active_ms / 2 from task.budget_scope where scope_id = ?",
                    Long.class, c.budgetScopeId());
            if (slice == null || slice < 1) throw EafException.conflict("WORKFLOW_BUDGET_CONFLICT", "根活动预算不足以创建双分支。");
            jdbc.update("update task.task set max_active_slice_ms = ? where id = ?", slice, id);
        }
        jdbc.update("update task.workflow_dispatch set task_id = ?, updated_at = now() where tenant_id = ? and workspace_id = ? and actor_id = ? and dispatch_key = ? and state = 'ACTIVE' and task_id is null",
                id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.dispatchKey());
        if (rootTaskId == null && jdbc.update("update task.budget_scope set root_task_id = ? where scope_id = ? and tenant_id = ? and workspace_id = ? and root_task_id is null",
                id, c.budgetScopeId(), c.actor().tenantId(), c.workspaceId()) != 1)
            throw EafException.conflict("WORKFLOW_BUDGET_CONFLICT", "Workflow 实例预算被其他根 Task 绑定。");
        var action = tool == null ? "WORKFLOW_CAPABILITY_TASK_CREATED" : "WORKFLOW_TOOL_TASK_CREATED";
        audit.append(new AuditFact("task-created:" + id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), id,
                action, "ACCEPTED", "{}", traceId));
        return p29Workflow ? readTask(c.actor(), c.workspaceId(), id) : get(c.actor(), c.workspaceId(), id);
    }

    @Override
    public Optional<TaskSnapshot> findByIdempotencyKey(ActorContext actor, UUID workspaceId, String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200)
            throw EafException.invalid("Task 来源键无效。");
        requireCurrentActor(actor, workspaceId, "task:read");
        var key = storedIdempotencyKey(actor, workspaceId, "workflow:" + idempotencyKey);
        var existing = findByIdempotency(actor, workspaceId, key);
        return existing.map(value -> get(actor, workspaceId, value.id()));
    }

    @Override
    @Transactional
    public TaskSnapshot createChild(CreateChildTaskCommand c) {
        if (c != null && c.actor() != null && IdentityService.MCP_AUDIENCE.equals(c.actor().delegationAudience()))
            throw EafException.forbidden("MCP 只读委托不创建子 Task。");
        // 稳定创建键按父 Task 隔离；重复键必须同时匹配原请求摘要。
        validateChildCommand(c == null ? null : c.actor(), c == null ? null : c.creationKey(), c == null ? null : c.input());
        requireCurrentActor(c.actor(), c.workspaceId(), "task:create");
        var taskAdmissionOpen = operationalControl.taskAdmissionOpen(c.actor().tenantId(), c.workspaceId());
        var parent = requireParent(c.actor(), c.workspaceId(), c.parentTaskId());
        if (isP30AutomationTask(parent.id()))
            throw EafException.forbidden("P30 摘要 Task 不创建子 Task。");
        if (qualityRunSources.isScenarioRun(c.actor().tenantId(), c.workspaceId(), parent.qualityRunId()))
            throw EafException.forbidden("只允许每个登记样本创建一个服务请求分析 Task。");
        var storedKey = childIdempotencyKey(c.actor(), c.workspaceId(), parent.id(), c.creationKey());
        var hash = childHash(parent, c.input(), "AGENT", null, null, null);
        var existing = findByIdempotency(c.actor(), c.workspaceId(), storedKey);
        if (existing.isPresent()) return existingTask(c.actor(), c.workspaceId(), storedKey, hash);
        if (!taskAdmissionOpen) throw taskAdmissionStopped();
        validateParentCanCreate(parent);
        var agent = agents.requirePublished(c.actor().tenantId(), c.workspaceId(), parent.agentId(), parent.agentVersion());
        var now = Instant.now(clock);
        var id = UUID.randomUUID();
        if (insertTask(id, c.actor(), c.workspaceId(), agent, c.input(), parent.businessEntityType(), parent.businessEntityId(),
                storedKey, hash, traceOrParent(c.traceId(), parent.traceId()), parent.source(), parent.assetBinding(),
                parent.rootTaskId(), parent.id(), parent.entryProtocol(), "AGENT", null, null, parent.qualityRunId(), null, now, parent.deadline(), null) == 0)
            return existingTask(c.actor(), c.workspaceId(), storedKey, hash);
        audit.append(new AuditFact("task-created:" + id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), id,
                "CHILD_TASK_CREATED", "ACCEPTED", "{}", traceOrParent(c.traceId(), parent.traceId())));
        return get(c.actor(), c.workspaceId(), id);
    }

    @Override
    @Transactional
    public TaskSnapshot createToolExecution(CreateToolExecutionCommand c) {
        if (c != null && c.actor() != null && IdentityService.MCP_AUDIENCE.equals(c.actor().delegationAudience()))
            throw EafException.forbidden("MCP 只读委托不创建 Tool Task。");
        // 服务端解析并冻结 Tool 版本与绑定，禁止调用方指定适配器或执行实现。
        validateChildCommand(c == null ? null : c.actor(), c == null ? null : c.creationKey(), c == null ? null : c.argumentsJson());
        if (c.toolName() == null || c.toolName().isBlank() || c.toolVersion() == null || c.toolVersion().isBlank())
            throw EafException.invalid("固定工具 Task 必须指定 Tool 名称和版本。");
        if ("crm.followup.result.record".equals(c.toolName()) || SERVICE_REQUEST_TOOL.equals(c.toolName())
                || isP27Tool(c.toolName()))
            throw EafException.forbidden("保留业务写入 Tool Task 只能由固定 Workflow 创建。");
        requireCurrentActor(c.actor(), c.workspaceId(), "task:create");
        var taskAdmissionOpen = operationalControl.taskAdmissionOpen(c.actor().tenantId(), c.workspaceId());
        var parent = requireParent(c.actor(), c.workspaceId(), c.parentTaskId());
        if (isP30AutomationTask(parent.id()))
            throw EafException.forbidden("P30 摘要 Task 不创建工具 Task。");
        if (qualityRunSources.isScenarioRun(c.actor().tenantId(), c.workspaceId(), parent.qualityRunId()))
            throw EafException.forbidden("评测运行不能创建子 Task 或 Tool Task。");
        var storedKey = childIdempotencyKey(c.actor(), c.workspaceId(), parent.id(), c.creationKey());
        var tool = tools.requirePublished(c.actor().tenantId(), c.workspaceId(), c.toolName(), c.toolVersion());
        if (tool.bindingRef() == null || tool.bindingRef().isBlank())
            throw EafException.conflict("TOOL_BINDING_MISSING", "固定工具没有已发布连接绑定。");
        if ("EVALUATION".equals(parent.source()) && "WRITE".equals(tool.effect()))
            throw EafException.forbidden("EVALUATION 子任务不能创建写入工具执行。");
        var hash = childHash(parent, c.argumentsJson(), "TOOL_EXECUTION", c.toolName(), c.toolVersion(), tool.bindingRef());
        var existing = findByIdempotency(c.actor(), c.workspaceId(), storedKey);
        if (existing.isPresent()) return existingTask(c.actor(), c.workspaceId(), storedKey, hash);
        if (!taskAdmissionOpen) throw taskAdmissionStopped();
        validateParentCanCreate(parent);
        var agent = agents.requirePublished(c.actor().tenantId(), c.workspaceId(), parent.agentId(), parent.agentVersion());
        var now = Instant.now(clock);
        var id = UUID.randomUUID();
        var label = "固定工具执行 " + tool.name() + "@" + tool.version();
        if (insertTask(id, c.actor(), c.workspaceId(), agent, label, "TOOL_EXECUTION", tool.name() + "@" + tool.version(),
                storedKey, hash, traceOrParent(c.traceId(), parent.traceId()), parent.source(), parent.assetBinding(),
                parent.rootTaskId(), parent.id(), parent.entryProtocol(), "TOOL_EXECUTION", tool, c.argumentsJson(), parent.qualityRunId(), null, now, parent.deadline(), null) == 0)
            return existingTask(c.actor(), c.workspaceId(), storedKey, hash);
        audit.append(new AuditFact("task-created:" + id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), id,
                "TOOL_TASK_CREATED", "ACCEPTED", "{}", traceOrParent(c.traceId(), parent.traceId())));
        return get(c.actor(), c.workspaceId(), id);
    }

    // Task 与固定来源一并落库；质量 reviewer 的权限复核读取此快照而不信任调用参数。
    private int insertTask(UUID id, ActorContext actor, UUID workspaceId, AgentDefinition agent, String input,
                           String entityType, String entityId, String storedKey, String hash, String traceId,
                           String source, TaskAssetBinding binding, UUID rootTaskId, UUID parentTaskId,
                           String entryProtocol, String runKind, ToolDefinition tool, String toolArguments,
                           UUID qualityRunId, WorkflowTaskProvenance workflowProvenance, Instant now, Instant deadline,
                           ModelProfileSelection modelSelection) {
        if (!admitQueuedTask(actor, workspaceId, storedKey, hash)) return 0;
        String modelSelectionJson = null;
        if (modelSelection != null) try { modelSelectionJson = JSON.writeValueAsString(modelSelection); }
        catch (com.fasterxml.jackson.core.JsonProcessingException invalid) { throw EafException.invalid("模型选择快照无法编码。"); }
        var inserted = jdbc.update("insert into task.task(id, tenant_id, workspace_id, actor_id, principal_id, delegate_id, delegation_id, authorization_hash, agent_id, agent_version, prompt_id, prompt_version, model_profile_id, input_text, business_entity_type, business_entity_id, idempotency_key, request_hash, trace_id, status, attempt, row_version, source, quality_run_id, workflow_instance_id, workflow_id, workflow_version, workflow_step_id, created_at, updated_at, active_deadline_at, deadline_at, capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash, root_task_id, parent_task_id, entry_protocol, run_kind, tool_name, tool_version, tool_binding_ref, tool_arguments_json, model_selection) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'QUEUED', 1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb) on conflict (tenant_id, workspace_id, actor_id, idempotency_key) do nothing",
                id, actor.tenantId(), workspaceId, actor.actorId(), actor.delegated() ? actor.principalId() : null,
                actor.delegated() ? actor.actorId() : null, actor.delegationId(), actor.authorizationHash(),
                agent.id(), agent.version(), agent.promptId(), agent.promptVersion(), agent.modelProfileId(), input, entityType, entityId,
                storedKey, hash, traceId, source, qualityRunId,
                workflowProvenance == null ? null : workflowProvenance.workflowInstanceId(),
                workflowProvenance == null ? null : workflowProvenance.workflowId(),
                workflowProvenance == null ? null : workflowProvenance.workflowVersion(),
                workflowProvenance == null ? null : workflowProvenance.stepId(),
                Timestamp.from(now), Timestamp.from(now), Timestamp.from(deadline), Timestamp.from(deadline),
                bindingValue(binding, 0), bindingValue(binding, 1), bindingValue(binding, 2),
                bindingValue(binding, 3), bindingValue(binding, 4), bindingValue(binding, 5), rootTaskId, parentTaskId,
                entryProtocol, runKind, tool == null ? null : tool.name(), tool == null ? null : tool.version(),
                tool == null ? null : tool.bindingRef(), toolArguments, modelSelectionJson);
        if (inserted == 1) jdbc.update("insert into task.task_attempt(task_id, attempt, status) values (?, 1, 'QUEUED')", id);
        return inserted;
    }

    private boolean admitQueuedTask(ActorContext actor, UUID workspaceId, String storedKey, String hash) {
        // 同一幂等键先串行化，令并发重放等待首个事务提交后读到原 Task。
        jdbc.query("select pg_advisory_xact_lock(hashtext('eaf.task.idempotency.v1'), hashtext(?))",
                rs -> { if (rs.next()) rs.getObject(1); return null; }, storedKey);
        var existing = findByIdempotency(actor, workspaceId, storedKey);
        if (existing.isPresent()) {
            if (!hash.equals(existing.get().requestHash()))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一幂等键对应了不同请求。");
            return false;
        }
        if (!tryQueueAdmissionLock()) {
            metrics.admissionRejected("lock_busy");
            throw taskCapacityExceeded();
        }
        if (!hasQueueCapacity(actor.tenantId(), workspaceId)) {
            throw taskCapacityExceeded();
        }
        return true;
    }

    private boolean tryQueueAdmissionLock() {
        // ponytail: 一个全局短锁加 COUNT，队列规模扩大后的入队吞吐由再按实测优化。
        return Boolean.TRUE.equals(jdbc.queryForObject(
                "select pg_try_advisory_xact_lock(hashtext('eaf.task.queue.admission.v1'), 0)", Boolean.class));
    }

    private EafException taskCapacityExceeded() {
        return new EafException(429, "TASK_CAPACITY_EXCEEDED", "Task 队列当前繁忙，请稍后使用原幂等键重试。", true);
    }

    private TaskSnapshot existingTask(ActorContext actor, UUID workspaceId, String key, String hash) {
        var existing = findByIdempotency(actor, workspaceId, key)
                .orElseThrow(() -> EafException.conflict("IDEMPOTENCY_CONFLICT", "幂等键已占用。"));
        if (!hash.equals(existing.requestHash())) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一幂等键对应了不同请求。");
        return get(actor, workspaceId, existing.id());
    }

    private WorkflowDispatch lockWorkflowDispatch(ActorContext actor, UUID workspaceId, String dispatchKey, String requestHash) {
        jdbc.update("insert into task.workflow_dispatch(tenant_id, workspace_id, actor_id, dispatch_key, request_hash, state) values (?, ?, ?, ?, ?, 'ACTIVE') on conflict do nothing",
                actor.tenantId(), workspaceId, actor.actorId(), dispatchKey, requestHash);
        var dispatch = jdbc.query("select request_hash, state, task_id from task.workflow_dispatch where tenant_id = ? and workspace_id = ? and actor_id = ? and dispatch_key = ? for update",
                rs -> rs.next() ? new WorkflowDispatch(rs.getString("request_hash"), rs.getString("state"), rs.getObject("task_id", UUID.class)) : null,
                actor.tenantId(), workspaceId, actor.actorId(), dispatchKey);
        if (dispatch == null) throw EafException.conflict("WORKFLOW_DISPATCH_MISSING", "Workflow 步骤分发状态缺失。");
        if (dispatch.requestHash() != null && !dispatch.requestHash().equals(requestHash))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "Workflow 步骤键已绑定到不同 Task 请求。");
        if ("CANCELLED".equals(dispatch.state()) || "CANCEL_REQUESTED".equals(dispatch.state()) && dispatch.taskId() == null)
            throw EafException.conflict("WORKFLOW_CHILD_CANCELLED", "Workflow 步骤已取消，不能再创建子 Task。");
        return dispatch;
    }

    private ParentTask requireParent(ActorContext actor, UUID workspaceId, UUID parentTaskId) {
        if (parentTaskId == null) throw EafException.invalid("父 Task 必填。");
        var parent = jdbc.query("select id, tenant_id, workspace_id, actor_id, principal_id, delegate_id, delegation_id, authorization_hash, agent_id, agent_version, source, status, run_kind, root_task_id, entry_protocol, deadline_at, trace_id, business_entity_type, business_entity_id, capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash, quality_run_id from task.task where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new ParentTask(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                        rs.getObject("actor_id", UUID.class), rs.getObject("principal_id", UUID.class), rs.getObject("delegate_id", UUID.class),
                        rs.getObject("delegation_id", UUID.class), rs.getString("authorization_hash"), rs.getObject("agent_id", UUID.class),
                        rs.getString("agent_version"), rs.getString("source"), rs.getString("status"), rs.getString("run_kind"),
                        rs.getObject("root_task_id", UUID.class), rs.getString("entry_protocol"), rs.getTimestamp("deadline_at").toInstant(),
                        rs.getString("trace_id"), rs.getString("business_entity_type"), rs.getString("business_entity_id"), mapAssetBinding(rs),
                        rs.getObject("quality_run_id", UUID.class)) : null,
                parentTaskId, actor.tenantId(), workspaceId);
        if (parent == null) throw EafException.notFound();
        boolean identityMatches = actor.delegated()
                ? actor.actorId().equals(parent.actorId()) && actor.principalId().equals(parent.principalId())
                    && actor.actorId().equals(parent.delegateId()) && actor.delegationId().equals(parent.delegationId())
                    && actor.authorizationHash().equals(parent.authorizationHash())
                : !actor.delegated() && actor.actorId().equals(parent.actorId()) && parent.principalId() == null
                    && parent.delegateId() == null && parent.delegationId() == null && parent.authorizationHash() == null;
        if (!identityMatches) throw EafException.forbidden("子 Task 必须继承父 Task 的原始身份。");
        return parent;
    }

    private void validateParentCanCreate(ParentTask parent) {
        if (!"RUNNING".equals(parent.status()) || !"AGENT".equals(parent.runKind()))
            throw EafException.conflict("INVALID_PARENT_STATE", "只有运行中的 Agent Task 可以创建子任务。");
        if (!parent.deadline().isAfter(Instant.now(clock))) throw EafException.conflict("DEADLINE_EXCEEDED", "父 Task 已超过总期限。");
    }

    private void validateChildCommand(ActorContext actor, String key, String input) {
        if (actor == null) throw EafException.invalid("Task 身份上下文缺失。");
        if (key == null || key.isBlank() || key.length() > 200) throw EafException.invalid("子 Task 稳定创建键必填且不超过 200 字符。");
        if (input == null || input.isBlank() || input.length() > 8_000) throw EafException.invalid("子 Task 输入必须为 1—8,000 字符。");
    }

    private String childIdempotencyKey(ActorContext actor, UUID workspaceId, UUID parentId, String key) {
        return storedIdempotencyKey(actor, workspaceId, "child:" + Hashing.sha256(parentId + "\u001f" + key));
    }

    private String childHash(ParentTask parent, String input, String runKind, String toolName, String toolVersion, String bindingRef) {
        return Hashing.sha256(String.join("\u001f", parent.id().toString(), parent.rootTaskId().toString(), parent.source(),
                parent.entryProtocol(), parent.agentId().toString(), parent.agentVersion(), bindingHash(parent.assetBinding()),
                runKind, String.valueOf(toolName), String.valueOf(toolVersion), String.valueOf(bindingRef), input,
                String.valueOf(parent.actorId()), String.valueOf(parent.principalId()), String.valueOf(parent.delegationId()),
                String.valueOf(parent.authorizationHash())));
    }

    private String traceOrParent(String traceId, String parentTrace) { return traceId == null || traceId.isBlank() ? parentTrace : traceId; }

    private record ParentTask(UUID id, UUID tenantId, UUID workspaceId, UUID actorId, UUID principalId, UUID delegateId,
                              UUID delegationId, String authorizationHash, UUID agentId, String agentVersion,
                              String source, String status, String runKind, UUID rootTaskId, String entryProtocol,
                              Instant deadline, String traceId, String businessEntityType, String businessEntityId,
                              TaskAssetBinding assetBinding, UUID qualityRunId) { }

    @Override
    public Optional<WorkflowExecutionSource> findWorkflowExecutionSource(ActorContext actor, UUID workspaceId,
            UUID taskId, int attempt) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated() || workspaceId == null
                || taskId == null || attempt < 1)
            throw EafException.forbidden("Workflow 来源元数据只允许固定 P27 的直接 HUMAN 查询。");
        return jdbc.query("select status, attempt, row_version, workflow_instance_id, workflow_id, workflow_version, "
                        + "workflow_step_id from task.task where id = ? and tenant_id = ? and workspace_id = ? "
                        + "and actor_id = ? and attempt = ? and workflow_instance_id is not null and workflow_id is not null "
                        + "and workflow_version is not null and workflow_step_id is not null",
                rs -> rs.next() ? Optional.of(new WorkflowExecutionSource(TaskStatus.valueOf(rs.getString("status")),
                        rs.getInt("attempt"), rs.getLong("row_version"), new WorkflowTaskProvenance(
                        rs.getObject("workflow_instance_id", UUID.class), rs.getObject("workflow_id", UUID.class),
                        rs.getString("workflow_version"), rs.getString("workflow_step_id")))) : Optional.empty(),
                taskId, actor.tenantId(), workspaceId, actor.actorId(), attempt);
    }

    @Override
    public TaskSnapshot get(ActorContext actor, UUID workspaceId, UUID taskId) {
        requireCurrentActor(actor, workspaceId, "task:read");
        requireConversationTaskOwner(actor, workspaceId, taskId);
        requireP21BatchTaskOwner(actor, workspaceId, taskId);
        var task = readTask(actor, workspaceId, taskId);
        var p29Attempt = jdbc.query("select attempt from task.task where id = ? and workflow_id = '58000000-0000-4000-8000-00000000001d'::uuid",
                rs -> rs.next() ? rs.getInt(1) : null, taskId);
        if (p29Attempt != null && p29Sources != null)
            p29Sources.requireTaskResultCurrent(actor, workspaceId, taskId, p29Attempt);
        var p30Attempt = automationTaskAttempt(taskId, actor.tenantId(), workspaceId);
        if (p30Attempt != null) {
            if (p30Sources == null) throw EafException.conflict("AUTOMATION_SOURCE_UNAVAILABLE", "自动化来源校验器未就绪。");
            p30Sources.requireTaskResultCurrent(actor, workspaceId, taskId, p30Attempt);
        }
        return qualityRunSources.isScenarioTask(actor.tenantId(), workspaceId, taskId)
                || qualityRunSources.isTeamImprovementTask(actor.tenantId(), workspaceId, taskId)
                ? redactScenarioTask(task) : task;
    }

    @Override
    public Instant deadlineAt(UUID tenantId, UUID workspaceId, UUID taskId) {
        var deadline = jdbc.query("select deadline_at from task.task where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getTimestamp(1).toInstant() : null, taskId, tenantId, workspaceId);
        if (deadline == null) throw EafException.notFound();
        return deadline;
    }

    @Override
    public ModelProfileSelection modelSelection(ActorContext actor, UUID workspaceId, UUID taskId) {
        var task = get(actor, workspaceId, taskId);
        var row = jdbc.query("select model_selection::text, model_profile_id, capability_id from task.task "
                        + "where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new Object[]{rs.getString(1), rs.getObject(2, UUID.class), rs.getObject(3, UUID.class)} : null,
                taskId, actor.tenantId(), workspaceId);
        if (row == null) throw EafException.notFound();
        if (row[0] != null) return readModelSelection((String) row[0]);
        if (SERVICE_REQUEST_PLAN_AGENT_ID.equals(task.agentId()) && SERVICE_REQUEST_PLAN_CAPABILITY_ID.equals(row[2]))
            return new ModelProfileSelection("LEGACY_UNSNAPSHOTTED", (UUID) row[1], null);
        return null;
    }

    @Override
    public TaskSnapshot getTeamImprovementGenerationTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("生成结果只允许直接 HUMAN Owner 经 Learning 内部读取。");
        workspaces.require(actor, workspaceId, "evaluation:read");
        if (!qualityRunSources.isTeamImprovementTask(actor.tenantId(), workspaceId, taskId)) throw EafException.notFound();
        var task = readTask(actor, workspaceId, taskId);
        if (!actor.actorId().equals(task.actorId()) || !"EVALUATION".equals(task.source())) throw EafException.notFound();
        return task;
    }

    @Override
    public boolean isTeamImprovementGenerationTask(UUID tenantId, UUID workspaceId, UUID taskId) {
        return qualityRunSources.isTeamImprovementTask(tenantId, workspaceId, taskId);
    }

    @Override
    public TaskSnapshot getScenarioEvaluationTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("样本正文只允许直接 HUMAN 发起人读取。");
        workspaces.require(actor, workspaceId, "evaluation:read");
        if (qualityRunSources.scenarioTaskOwner(actor.tenantId(), workspaceId, taskId)
                .filter(actor.actorId()::equals).isEmpty()) throw EafException.notFound();
        return readTask(actor, workspaceId, taskId);
    }

    @Override
    public ScenarioTaskTiming getScenarioEvaluationTiming(ActorContext actor, UUID workspaceId, UUID taskId, int attempt) {
        var task = getScenarioEvaluationTask(actor, workspaceId, taskId);
        if (task.attempt() != attempt) throw EafException.conflict("SCENARIO_TASK_ATTEMPT_CHANGED", "Task attempt 已变化。");
        return jdbc.query("select case when a.started_at is not null then greatest(0, extract(epoch from (a.started_at - t.created_at)) * 1000)::bigint "
                        + "when a.ended_at is not null then greatest(0, extract(epoch from (a.ended_at - t.created_at)) * 1000)::bigint end as queue_ms, "
                        + "case when a.started_at is not null and a.ended_at is not null then greatest(0, extract(epoch from (a.ended_at - a.started_at)) * 1000)::bigint end as execution_ms "
                        + "from task.task t join task.task_attempt a on a.task_id = t.id and a.attempt = t.attempt "
                        + "where t.id = ? and t.tenant_id = ? and t.workspace_id = ? and t.attempt = ?",
                rs -> rs.next() ? new ScenarioTaskTiming(rs.getObject("queue_ms", Long.class),
                        rs.getObject("execution_ms", Long.class)) : null,
                taskId, actor.tenantId(), workspaceId, attempt);
    }

    @Override
    public void requireScenarioEvaluationTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        var snapshot = getScenarioEvaluationTask(actor, workspaceId, taskId);
        var row = jdbc.query("select quality_run_id from task.task where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, taskId, actor.tenantId(), workspaceId);
        if (!scenarioTaskCurrent(actor.tenantId(), workspaceId, row, snapshot))
            throw EafException.conflict("SCENARIO_RUN_NOT_CURRENT", "样本运行已停止、超期或资料/资产清单已变化。");
    }

    private boolean scenarioTaskCurrent(UUID tenantId, UUID workspaceId, UUID qualityRunId, TaskSnapshot task) {
        return qualityRunId != null && qualityRunSources.scenarioTaskMatches(tenantId, workspaceId, task.actorId(),
                task.id(), task.rootTaskId(), qualityRunId, task.attempt(), task.agentId(), task.agentVersion(),
                task.source(), task.runKind(), io.eaf.shared.Hashing.sha256(task.inputText()), task.assetBinding());
    }

    @Override
    @Transactional
    public void cancelScenarioEvaluationTask(UUID tenantId, UUID workspaceId, UUID ownerId, UUID taskId) {
        if (!qualityRunSources.isScenarioTask(tenantId, workspaceId, taskId)) throw EafException.notFound();
        var actor = jdbc.query("select actor_id, row_version from task.task where id = ? and tenant_id = ? and workspace_id = ? and quality_run_id is not null",
                rs -> rs.next() && ownerId.equals(rs.getObject("actor_id", UUID.class))
                        ? new Object[]{rs.getLong("row_version")} : null, taskId, tenantId, workspaceId);
        if (actor == null) throw EafException.notFound();
        cancelTaskInternal(tenantId, workspaceId, ownerId, taskId, (Long) actor[0], false);
    }

    @Override
    public boolean isScenarioEvaluationTask(UUID tenantId, UUID workspaceId, UUID taskId) {
        if (tenantId != null && workspaceId == null && taskId != null) {
            workspaceId = jdbc.query("select workspace_id from task.task where id = ? and tenant_id = ?",
                    rs -> rs.next() ? rs.getObject("workspace_id", UUID.class) : null, taskId, tenantId);
        }
        return qualityRunSources.isScenarioTask(tenantId, workspaceId, taskId);
    }

    @Override
    public boolean isTeamPreparationEvaluationTask(UUID tenantId, UUID workspaceId, UUID taskId) {
        return qualityRunSources.isTeamPreparationTask(tenantId, workspaceId, taskId);
    }

    private TaskSnapshot readTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        var protocolScope = IdentityService.MCP_AUDIENCE.equals(actor.delegationAudience())
                ? " and entry_protocol = 'MCP'" : " and entry_protocol in ('REST','A2A')";
        var visibility = actor.delegated() ? " and actor_id = ? and principal_id = ? and delegation_id = ? and authorization_hash = ?" + protocolScope : "";
        var args = actor.delegated()
                ? new Object[]{taskId, actor.tenantId(), workspaceId, actor.actorId(), actor.principalId(), actor.delegationId(), actor.authorizationHash()}
                : new Object[]{taskId, actor.tenantId(), workspaceId};
        var task = jdbc.query("select id, tenant_id, workspace_id, actor_id, agent_id, agent_version, prompt_version, status, attempt, row_version, trace_id, input_text, result_json::text, error_code, error_detail, source, created_at, updated_at, capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash, root_task_id, parent_task_id, entry_protocol, run_kind, external_effect_status, external_effect_operation_id from task.task where id = ? and tenant_id = ? and workspace_id = ?" + visibility,
                (ResultSetExtractor<Optional<TaskSnapshot>>) rs -> rs.next() ? Optional.of(mapSnapshot(rs)) : Optional.empty(), args).orElseThrow(EafException::notFound);
        if (IdentityService.MCP_AUDIENCE.equals(actor.delegationAudience())
                && !validMcpReadonlyTask(actor, workspaceId, task.entryProtocol(), task.source(), null, null,
                task.agentId(), task.agentVersion(), task.assetBinding())) throw EafException.notFound();
        return task;
    }

    private TaskSnapshot redactScenarioTask(TaskSnapshot task) {
        return new TaskSnapshot(task.id(), task.tenantId(), task.workspaceId(), task.actorId(), task.agentId(),
                task.agentVersion(), task.promptVersion(), task.status(), task.attempt(), task.version(), task.traceId(),
                null, null, task.errorCode(), task.errorDetail(), task.source(), task.createdAt(), task.updatedAt(),
                task.assetBinding(), task.rootTaskId(), task.parentTaskId(), task.entryProtocol(), task.runKind(),
                task.externalEffectStatus(), task.externalEffectPending());
    }

    @Override
    public TaskPage list(ActorContext actor, UUID workspaceId, UUID rootTaskId, Set<TaskStatus> statuses,
                         Instant statusUpdatedAfter, TaskPageCursor cursor, int pageSize) {
        if (pageSize < 1 || pageSize > 100 || (cursor != null && (cursor.updatedAt() == null || cursor.taskId() == null)))
            throw EafException.invalid("Task 列表分页参数无效。");
        requireCurrentActor(actor, workspaceId, "task:read");
        var where = new StringBuilder(" where t.tenant_id = ? and t.workspace_id = ?");
        where.append(" and t.workflow_id is distinct from '58000000-0000-4000-8000-00000000001d'::uuid");
        var filters = new java.util.ArrayList<Object>();
        filters.add(actor.tenantId());
        filters.add(workspaceId);
        // 委托列表严格沿用单 Task 查询的原 actor、principal、delegation 与授权摘要快照。
        if (actor.delegated()) {
            where.append(" and t.actor_id = ? and t.principal_id = ? and t.delegation_id = ? and t.authorization_hash = ?")
                    .append(IdentityService.MCP_AUDIENCE.equals(actor.delegationAudience())
                            ? " and t.entry_protocol = 'MCP'" : " and t.entry_protocol in ('REST','A2A')");
            filters.add(actor.actorId());
            filters.add(actor.principalId());
            filters.add(actor.delegationId());
            filters.add(actor.authorizationHash());
        }
        if (rootTaskId != null) {
            where.append(" and t.root_task_id = ?");
            filters.add(rootTaskId);
        }
        if (statuses != null) {
            if (statuses.isEmpty()) where.append(" and 1 = 0");
            else {
                where.append(" and t.status in (").append(String.join(",", java.util.Collections.nCopies(statuses.size(), "?"))).append(')');
                statuses.forEach(status -> filters.add(status.name()));
            }
        }
        if (statusUpdatedAfter != null) {
            where.append(" and t.updated_at >= ?");
            filters.add(Timestamp.from(statusUpdatedAfter));
        }
        // 私人会话 Task 只出现在创建者的非委托 HUMAN 列表；普通 Task 的可见规则不变。
        where.append(" and not exists (select 1 from task.conversation_turn ct join task.conversation c on c.id = ct.conversation_id "
                + "where ct.task_id = t.id and (c.owner_id <> ? or ? <> 'HUMAN' or ? = true))");
        filters.add(actor.actorId()); filters.add(actor.type().name()); filters.add(actor.delegated());
        where.append(" and not exists (select 1 from task.experience_draft_binding ed where ed.task_id = t.id "
                + "and (ed.owner_id <> ? or ? <> 'HUMAN' or ? = true))");
        filters.add(actor.actorId()); filters.add(actor.type().name()); filters.add(actor.delegated());
        where.append(" and (t.workflow_id is distinct from '58000000-0000-4000-8000-000000000018'::uuid "
                + "or (t.actor_id = ? and ? = 'HUMAN' and ? = false))");
        filters.add(actor.actorId()); filters.add(actor.type().name()); filters.add(actor.delegated());
        where.append(" and not exists (select 1 from task.automation_task_binding ab where ab.task_id = t.id "
                + "and (ab.owner_id <> ? or ? <> 'HUMAN' or ? = true))");
        filters.add(actor.actorId()); filters.add(actor.type().name()); filters.add(actor.delegated());
        var totalSize = jdbc.queryForObject("select count(*) from task.task t" + where, Long.class, filters.toArray());
        var pageWhere = new StringBuilder(where);
        var pageArgs = new java.util.ArrayList<>(filters);
        if (cursor != null) {
            pageWhere.append(" and (t.updated_at, t.id) < (?, ?)");
            pageArgs.add(Timestamp.from(cursor.updatedAt()));
            pageArgs.add(cursor.taskId());
        }
        pageArgs.add(pageSize + 1);
        var selected = jdbc.query("select t.id, t.tenant_id, t.workspace_id, t.actor_id, t.agent_id, t.agent_version, t.prompt_version, t.status, t.attempt, t.row_version, t.trace_id, t.input_text, t.result_json::text, t.error_code, t.error_detail, t.source, t.created_at, t.updated_at, t.capability_id, t.capability_version, t.capability_hash, t.skill_id, t.skill_version, t.skill_hash, t.root_task_id, t.parent_task_id, t.entry_protocol, t.run_kind, t.external_effect_status, t.external_effect_operation_id from task.task t"
                        + pageWhere + " order by t.updated_at desc, t.id desc limit ?",
                (rs, row) -> mapSnapshot(rs), pageArgs.toArray());
        selected = selected.stream().map(task -> qualityRunSources.isScenarioTask(actor.tenantId(), workspaceId, task.id())
                || qualityRunSources.isTeamImprovementTask(actor.tenantId(), workspaceId, task.id())
                ? redactScenarioTask(task) : task).toList();
        selected = selected.stream().filter(task -> {
            var attempt = automationTaskAttempt(task.id(), actor.tenantId(), workspaceId);
            if (attempt == null) return true;
            try {
                if (p30Sources == null) return false;
                p30Sources.requireTaskResultCurrent(actor, workspaceId, task.id(), attempt);
                return true;
            } catch (EafException unavailable) { return false; }
        }).toList();
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var nextCursor = hasNext ? new TaskPageCursor(items.get(items.size() - 1).updatedAt(),
                items.get(items.size() - 1).id()) : null;
        return new TaskPage(items, totalSize == null ? 0 : totalSize, nextCursor);
    }

    @Override
    public UserTaskResultPage listMyRootResults(ActorContext actor, UUID workspaceId, Instant cursorUpdatedAt,
            UUID cursorId, int pageSize) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("员工任务成果列表只接受本人直接操作的 HUMAN 身份。");
        requireCurrentActor(actor, workspaceId, "task:read");
        if (pageSize < 1 || pageSize > 50 || (cursorUpdatedAt == null) != (cursorId == null))
            throw EafException.invalid("本人任务成果分页参数无效。");
        var args = new java.util.ArrayList<Object>(List.of(actor.tenantId(), workspaceId, actor.actorId()));
        var where = new StringBuilder(" where t.tenant_id = ? and t.workspace_id = ? and t.actor_id = ? "
                + "and t.parent_task_id is null and t.root_task_id = t.id and t.source = 'USER' "
                + "and t.workflow_id is null and t.quality_run_id is null and t.run_kind = 'AGENT' "
                + "and t.business_entity_type is null and t.status in ('SUCCEEDED','FAILED','TIMED_OUT','CANCELLED') "
                + "and not exists (select 1 from task.conversation_turn ct where ct.task_id = t.id) "
                + "and not exists (select 1 from task.experience_draft_binding ed where ed.task_id = t.id)");
        if (cursorUpdatedAt != null) {
            where.append(" and (t.updated_at, t.id) < (?, ?)");
            args.add(Timestamp.from(cursorUpdatedAt));
            args.add(cursorId);
        }
        args.add(pageSize + 1);
        var selected = jdbc.query("select t.id, t.agent_id, t.agent_version, t.status, t.updated_at from task.task t"
                        + where + " order by t.updated_at desc, t.id desc limit ?",
                (rs, row) -> new UserTaskResultPage.Item(rs.getObject("id", UUID.class),
                        rs.getObject("agent_id", UUID.class), rs.getString("agent_version"), rs.getString("status"),
                        rs.getTimestamp("updated_at").toInstant()), args.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        return new UserTaskResultPage(items, last == null ? null : last.updatedAt(), last == null ? null : last.id());
    }

    @Override
    @Transactional
    public TaskSnapshot cancel(ActorContext actor, UUID workspaceId, UUID taskId, long expectedVersion) {
        requireCurrentActor(actor, workspaceId, "task:cancel");
        requireConversationTaskOwner(actor, workspaceId, taskId);
        if (isP21BatchTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.conflict("BATCH_TASK_MANAGED", "分支 Task 只能通过所属批次取消。");
        if (qualityRunSources.isTeamImprovementTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.conflict("TEAM_IMPROVEMENT_TASK_MANAGED", "Task 只能通过所属改进运行停止。");
        var result = cancelTaskInternal(actor.tenantId(), workspaceId, actor.actorId(), taskId, expectedVersion, false);
        if (result.externalEffectPending() && result.status() != TaskStatus.CANCELLING_REMOTE)
            throw EafException.conflict("EXTERNAL_EFFECT_PENDING", "外部写入仍待核验，Task 不能提前取消。");
        return get(actor, workspaceId, taskId);
    }

    @Override
    @Transactional
    public TaskSnapshot cancelTeamImprovementGenerationTask(ActorContext actor, UUID workspaceId, UUID taskId,
            long expectedVersion) {
        requireCurrentActor(actor, workspaceId, "task:cancel");
        if (actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated()
                || !qualityRunSources.isTeamImprovementTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.notFound();
        var current = readTask(actor, workspaceId, taskId);
        if (!actor.actorId().equals(current.actorId())) throw EafException.notFound();
        if (current.version() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "Task 版本已变化。");
        cancelTaskInternal(actor.tenantId(), workspaceId, actor.actorId(), taskId, expectedVersion, false);
        return get(actor, workspaceId, taskId);
    }

    @Override
    @Transactional
    public WorkflowTaskCancellation cancelWorkflowTask(UUID tenantId, UUID workspaceId, UUID actorId, String dispatchKey) {
        if (tenantId == null || workspaceId == null || actorId == null || dispatchKey == null
                || dispatchKey.isBlank() || dispatchKey.length() > 200)
            throw EafException.invalid("Workflow 子 Task 取消键无效。");
        jdbc.update("insert into task.workflow_dispatch(tenant_id, workspace_id, actor_id, dispatch_key, state) values (?, ?, ?, ?, 'CANCELLED') on conflict do nothing",
                tenantId, workspaceId, actorId, dispatchKey);
        var dispatch = jdbc.query("select request_hash, state, task_id from task.workflow_dispatch where tenant_id = ? and workspace_id = ? and actor_id = ? and dispatch_key = ? for update",
                rs -> rs.next() ? new WorkflowDispatch(rs.getString("request_hash"), rs.getString("state"), rs.getObject("task_id", UUID.class)) : null,
                tenantId, workspaceId, actorId, dispatchKey);
        if (dispatch == null) throw EafException.notFound();
        if (dispatch.taskId() == null) {
            jdbc.update("update task.workflow_dispatch set state = 'CANCELLED', updated_at = now() where tenant_id = ? and workspace_id = ? and actor_id = ? and dispatch_key = ?",
                    tenantId, workspaceId, actorId, dispatchKey);
            return new WorkflowTaskCancellation(null, null, "NONE", false);
        }
        var result = cancelTaskInternal(tenantId, workspaceId, actorId, dispatch.taskId(), null, true);
        jdbc.update("update task.workflow_dispatch set state = ?, updated_at = now() where tenant_id = ? and workspace_id = ? and actor_id = ? and dispatch_key = ?",
                result.externalEffectPending() ? "CANCEL_REQUESTED" : "CANCELLED", tenantId, workspaceId, actorId, dispatchKey);
        return result;
    }

    private WorkflowTaskCancellation cancelTaskInternal(UUID tenantId, UUID workspaceId, UUID actorId, UUID taskId,
                                                         Long expectedVersion, boolean allowTerminal) {
        var rootTaskId = taskRoot(taskId);
        if (rootTaskId == null) throw EafException.notFound();
        lockBudgetScope(rootTaskId);
        var current = jdbc.query("select t.attempt, t.active_reserved_ms, t.active_budget_reservation_key, t.row_version, t.status, a.started_at, t.external_effect_operation_id, t.external_effect_status "
                        + "from task.task t left join task.task_attempt a on a.task_id = t.id and a.attempt = t.attempt and a.status in ('QUEUED','RUNNING','WAITING_APPROVAL','WAITING_VERIFICATION','WAITING_REMOTE') "
                        + "where t.id = ? and t.tenant_id = ? and t.workspace_id = ? and t.actor_id = ? for update of t",
                rs -> rs.next() ? new CancelTaskState(rs.getInt("attempt"), rs.getLong("active_reserved_ms"),
                        rs.getString("active_budget_reservation_key"), rs.getLong("row_version"), rs.getString("status"),
                        rs.getTimestamp("started_at") == null ? null : rs.getTimestamp("started_at").toInstant(),
                        rs.getObject("external_effect_operation_id", UUID.class), rs.getString("external_effect_status")) : null,
                taskId, tenantId, workspaceId, actorId);
        if (current == null) throw EafException.notFound();
        if (expectedVersion != null && current.rowVersion() != expectedVersion)
            throw EafException.conflict("VERSION_CONFLICT", "Task 版本已变化。");
        if ("CANCELLING_REMOTE".equals(current.status()) && current.externalOperationId() != null
                && List.of("REMOTE_CANCEL_REQUESTED", "REMOTE_CANCEL_UNKNOWN").contains(current.externalEffectStatus()))
            return new WorkflowTaskCancellation(taskId, TaskStatus.CANCELLING_REMOTE, current.externalEffectStatus(), true);
        if (current.externalOperationId() != null && "REMOTE_PENDING".equals(current.externalEffectStatus())
                && List.of("WAITING_REMOTE", "QUEUED", "RUNNING").contains(current.status())) {
            var now = Timestamp.from(Instant.now(clock));
            var elapsed = "RUNNING".equals(current.status()) && current.startedAt() != null
                    ? Math.max(0, Duration.between(current.startedAt(), now.toInstant()).toMillis()) : 0;
            var changed = jdbc.update("update task.task set active_used_ms = active_used_ms + ?, active_reserved_ms = 0, active_budget_reservation_key = null, lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, status = 'CANCELLING_REMOTE', external_effect_status = 'REMOTE_CANCEL_REQUESTED', row_version = row_version + 1, updated_at = ? where id = ? and tenant_id = ? and workspace_id = ? and actor_id = ? and row_version = ? and status in ('WAITING_REMOTE','QUEUED','RUNNING') and external_effect_operation_id = ? and external_effect_status = 'REMOTE_PENDING'",
                    elapsed, now, taskId, tenantId, workspaceId, actorId, current.rowVersion(), current.externalOperationId());
            if (changed != 1) throw EafException.conflict("TASK_CANCEL_RACE", "远端操作状态已变化，重新读取 Task 后再取消。");
            if (current.activeReservationKey() != null)
                settleActiveBudget(rootTaskId, current.activeReservationKey(), taskId, current.attempt(),
                        current.activeReservedMs(), elapsed, now);
            jdbc.update("update task.task_attempt set status = 'CANCELLING_REMOTE', ended_at = ? where task_id = ? and attempt = ? and status in ('WAITING_REMOTE','QUEUED','RUNNING')",
                    now, taskId, current.attempt());
            audit.append(new AuditFact("remote-task-cancel-requested:" + current.externalOperationId(), tenantId, workspaceId,
                    actorId, taskId, "REMOTE_AGENT_CANCEL_REQUESTED", "CANCELLING_REMOTE", "{}", currentTrace(taskId)));
            return new WorkflowTaskCancellation(taskId, TaskStatus.CANCELLING_REMOTE, "REMOTE_CANCEL_REQUESTED", true);
        }
        if (current.externalOperationId() != null)
            return new WorkflowTaskCancellation(taskId, TaskStatus.valueOf(current.status()), current.externalEffectStatus(), true);
        if (!List.of("QUEUED", "RUNNING", "WAITING_APPROVAL", "WAITING_VERIFICATION", "WAITING_REMOTE", "CANCELLING_REMOTE").contains(current.status())) {
            if (allowTerminal)
                return new WorkflowTaskCancellation(taskId, TaskStatus.valueOf(current.status()), current.externalEffectStatus(), false);
            throw EafException.conflict("INVALID_STATE", "Task 当前状态不能取消。");
        }
        var now = Timestamp.from(Instant.now(clock));
        var elapsed = "RUNNING".equals(current.status()) && current.startedAt() != null
                ? Math.max(0, Duration.between(current.startedAt(), now.toInstant()).toMillis()) : 0;
        var changed = jdbc.update("update task.task set active_used_ms = active_used_ms + ?, active_reserved_ms = 0, active_budget_reservation_key = null, lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, status = 'CANCELLED', row_version = row_version + 1, updated_at = ? where id = ? and tenant_id = ? and workspace_id = ? and actor_id = ? and row_version = ? and status in ('QUEUED', 'RUNNING', 'WAITING_APPROVAL', 'WAITING_VERIFICATION', 'WAITING_REMOTE') and external_effect_operation_id is null",
                elapsed, now, taskId, tenantId, workspaceId, actorId, current.rowVersion());
        if (changed == 0) throw EafException.conflict("TASK_CANCEL_RACE", "Task 正在提交外部操作或状态已变化。");
        if (current.activeReservationKey() != null)
            settleActiveBudget(rootTaskId, current.activeReservationKey(), taskId, current.attempt(), current.activeReservedMs(), elapsed, now);
        jdbc.update("update task.task_attempt set status = 'CANCELLED', ended_at = ? where task_id = ? and attempt = ? and status in ('QUEUED','RUNNING','WAITING_APPROVAL','WAITING_VERIFICATION','WAITING_REMOTE')",
                now, taskId, current.attempt());
        audit.append(new AuditFact("task-cancelled:" + taskId + ":" + current.rowVersion(), tenantId, workspaceId,
                actorId, taskId, "TASK_CANCELLED", "ACCEPTED", "{}", currentTrace(taskId)));
        return new WorkflowTaskCancellation(taskId, TaskStatus.CANCELLED, current.externalEffectStatus(), false);
    }

    @Override
    @Transactional
    public void markExternalEffect(UUID taskId, int attempt, UUID operationId, String status) {
        if (taskId == null || attempt < 1 || operationId == null
                || !List.of("IN_PROGRESS", "UNKNOWN", "REMOTE_PENDING", "REMOTE_CANCEL_UNKNOWN", "SUCCEEDED", "FAILED", "VERIFICATION_FAILED").contains(status))
            throw EafException.invalid("Task 外部写入状态参数无效。");
        int changed;
        if ("IN_PROGRESS".equals(status)) {
            changed = jdbc.update("update task.task set external_effect_operation_id = ?, external_effect_status = 'IN_PROGRESS' "
                            + "where id = ? and attempt = ? and status = 'RUNNING' and lease_until > now() "
                            + "and (external_effect_operation_id is null or external_effect_operation_id = ?)",
                    operationId, taskId, attempt, operationId);
            if (changed != 1) throw EafException.conflict("TASK_CANCELLED_OR_EFFECT_PENDING", "Task 已取消或有其他外部操作占用取消栅栏。");
            return;
        }
        if (List.of("UNKNOWN", "REMOTE_PENDING", "REMOTE_CANCEL_UNKNOWN").contains(status))
            changed = jdbc.update("update task.task set external_effect_status = ? where id = ? and attempt = ? and external_effect_operation_id = ?",
                    status, taskId, attempt, operationId);
        else
            changed = jdbc.update("update task.task set external_effect_operation_id = null, external_effect_status = ? where id = ? and attempt = ? and external_effect_operation_id = ?",
                    status, taskId, attempt, operationId);
        if (changed > 0 && !List.of("UNKNOWN", "REMOTE_PENDING", "REMOTE_CANCEL_UNKNOWN").contains(status))
            jdbc.update("update task.workflow_dispatch set state = 'CANCEL_REQUESTED', updated_at = now() where task_id = ? and state = 'CANCEL_REQUESTED'",
                    taskId);
    }

    @Override
    @Transactional
    public void confirmRemoteCancellation(UUID tenantId, UUID workspaceId, UUID taskId, int attempt, UUID operationId) {
        // 只接受与当前 Task/attempt 绑定的远端确认；重复或迟到确认不能改写其他状态。
        var rootTaskId = taskRoot(taskId);
        if (rootTaskId == null) return;
        lockBudgetScope(rootTaskId);
        var current = jdbc.query("select actor_id, row_version from task.task where id = ? and tenant_id = ? and workspace_id = ? and attempt = ? and status = 'CANCELLING_REMOTE' and external_effect_operation_id = ? for update",
                rs -> rs.next() ? new Object[]{rs.getObject("actor_id", UUID.class), rs.getLong("row_version")} : null,
                taskId, tenantId, workspaceId, attempt, operationId);
        if (current == null) return;
        var now = Timestamp.from(Instant.now(clock));
        var changed = jdbc.update("update task.task set status = 'CANCELLED', external_effect_operation_id = null, external_effect_status = 'REMOTE_CANCELLED', result_json = null, error_code = null, error_detail = null, row_version = row_version + 1, updated_at = ? where id = ? and row_version = ? and status = 'CANCELLING_REMOTE' and external_effect_operation_id = ?",
                now, taskId, current[1], operationId);
        if (changed != 1) return;
        jdbc.update("update task.task_attempt set status = 'CANCELLED', ended_at = ? where task_id = ? and attempt = ? and status = 'CANCELLING_REMOTE'",
                now, taskId, attempt);
        audit.append(new AuditFact("remote-task-cancel-confirmed:" + operationId, tenantId, workspaceId,
                (UUID) current[0], taskId, "TASK_CANCELLED", "REMOTE_CANCELLED",
                "{\"operationId\":\"" + operationId + "\"}", currentTrace(taskId)));
    }

    @Override
    @Transactional
    public TaskSnapshot retry(ActorContext actor, UUID workspaceId, UUID taskId, long expectedVersion, String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) throw EafException.invalid("重试 Idempotency-Key 必填。");
        requireCurrentActor(actor, workspaceId, "task:create");
        requireConversationTaskOwner(actor, workspaceId, taskId);
        requireAutomationTaskCurrent(actor, workspaceId, taskId, false);
        if (isP30AutomationTask(taskId))
            throw EafException.conflict("AUTOMATION_TASK_RETRY_UNSUPPORTED", "自动摘要只允许原运行中的一次生成；请创建新的订阅运行。 ");
        if (qualityRunSources.isScenarioTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.conflict("SCENARIO_TASK_RETRY_UNSUPPORTED", "样本 Task 不支持单独重试；请显式创建新评测运行。");
        if (qualityRunSources.isTeamImprovementTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.conflict("TEAM_IMPROVEMENT_TASK_RETRY_UNSUPPORTED", "生成 Task 不支持重试；请保留原运行结果或显式新建运行。");
        if (isP21BatchTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.conflict("BATCH_TASK_RETRY_UNSUPPORTED", "分支 Task 不支持单独重试；请新建分析批次。");
        if (isConversationTask(actor, workspaceId, taskId))
            throw EafException.conflict("CONVERSATION_RETRY_UNSUPPORTED", "会话轮次不支持原 Task 重试；请在会话中创建新轮次。 ");
        if (isExperienceDraftTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.conflict("EXPERIENCE_DRAFT_RETRY_UNSUPPORTED", "经验整理 Task 不支持重试；请从原反馈创建新的整理请求。 ");
        var taskAdmissionOpen = operationalControl.taskAdmissionOpen(actor.tenantId(), workspaceId);
        var current = get(actor, workspaceId, taskId);
        if (current.version() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "Task 版本已变化。");
        var priorRequest = jdbc.queryForObject("select count(*) from task.retry_request where task_id = ? and idempotency_key = ?",
                Integer.class, taskId, idempotencyKey);
        if (priorRequest != null && priorRequest > 0) return current;
        if (!taskAdmissionOpen) throw taskAdmissionStopped();
        var inserted = jdbc.update("insert into task.retry_request(task_id, idempotency_key) values (?, ?) on conflict do nothing", taskId, idempotencyKey);
        if (inserted == 0) return current;
        if (current.status() != TaskStatus.FAILED && current.status() != TaskStatus.TIMED_OUT)
            throw EafException.conflict("INVALID_STATE", "只有失败或超时 Task 可以显式重试。");
        if (jdbc.queryForObject("select count(*) from task.task t join task.budget_scope b on b.root_task_id = t.root_task_id where t.id = ? and t.deadline_at > now() and t.active_used_ms < ? and t.token_used + t.token_reserved < 8000 and t.steps_used < t.max_steps and b.active_used_ms + b.active_reserved_ms < b.max_active_ms and b.token_used + b.token_reserved < b.max_tokens and b.steps_used < b.max_steps", Integer.class, taskId, activeDuration.toMillis()) == 0)
            throw EafException.conflict("BUDGET_EXCEEDED", "Task 的累计预算或总期限已耗尽。");
        requireQueueSlot(actor.tenantId(), workspaceId);
        var now = Instant.now(clock);
        var changed = jdbc.update("update task.task set status = 'QUEUED', attempt = attempt + 1, lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, row_version = row_version + 1, error_code = null, error_detail = null, result_json = null, active_deadline_at = (?::timestamptz + greatest(0, ? - active_used_ms) * interval '1 millisecond'), updated_at = ? where id = ? and row_version = ? and status in ('FAILED', 'TIMED_OUT')",
                Timestamp.from(now), activeDuration.toMillis(), Timestamp.from(now), taskId, expectedVersion);
        if (changed == 0) throw EafException.conflict("VERSION_CONFLICT", "Task 重试竞争失败。");
        jdbc.update("insert into task.task_attempt(task_id, attempt, status) select id, attempt, 'QUEUED' from task.task where id = ?", taskId);
        audit.append(new AuditFact("task-retried:" + taskId + ":" + idempotencyKey, actor.tenantId(), workspaceId, actor.actorId(), taskId, "TASK_RETRIED", "ACCEPTED", "{}", currentTrace(taskId)));
        return get(actor, workspaceId, taskId);
    }

    @Override
    @Transactional
    public TaskSnapshot resume(ActorContext actor, UUID workspaceId, UUID taskId, long expectedVersion, String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) throw EafException.invalid("恢复 Idempotency-Key 必填。");
        requireCurrentActor(actor, workspaceId, "task:resume");
        requireConversationTaskOwner(actor, workspaceId, taskId);
        requireAutomationTaskCurrent(actor, workspaceId, taskId, false);
        if (isP30AutomationTask(taskId))
            throw EafException.conflict("AUTOMATION_TASK_RETRY_UNSUPPORTED", "自动摘要不支持人工恢复；请从订阅查看原运行。 ");
        if (qualityRunSources.isScenarioTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.conflict("SCENARIO_TASK_RETRY_UNSUPPORTED", "样本 Task 不支持单独恢复。");
        if (qualityRunSources.isTeamImprovementTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.conflict("TEAM_IMPROVEMENT_TASK_RETRY_UNSUPPORTED", "生成 Task 不支持单独恢复。");
        if (isP21BatchTask(actor.tenantId(), workspaceId, taskId))
            throw EafException.conflict("BATCH_TASK_RETRY_UNSUPPORTED", "分支 Task 不支持单独恢复。");
        var current = get(actor, workspaceId, taskId);
        if (current.version() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "Task 版本已变化。");
        var inserted = jdbc.update("insert into task.resume_request(task_id, idempotency_key) values (?, ?) on conflict do nothing", taskId, idempotencyKey);
        if (inserted == 0) return current;
        if (current.status() != TaskStatus.WAITING_APPROVAL && current.status() != TaskStatus.WAITING_VERIFICATION)
            throw EafException.conflict("INVALID_STATE", "只有等待中的 Task 可以恢复。");
        requireQueueSlot(actor.tenantId(), workspaceId);
        var now = Instant.now(clock);
        var changed = jdbc.update("update task.task set status = 'QUEUED', lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, row_version = row_version + 1, error_code = null, error_detail = null, active_deadline_at = (?::timestamptz + greatest(0, ? - active_used_ms) * interval '1 millisecond'), updated_at = ? where id = ? and tenant_id = ? and workspace_id = ? and row_version = ? and status in ('WAITING_APPROVAL', 'WAITING_VERIFICATION')",
                Timestamp.from(now), activeDuration.toMillis(), Timestamp.from(now), taskId, actor.tenantId(), workspaceId, expectedVersion);
        if (changed == 0) throw EafException.conflict("VERSION_CONFLICT", "Task 恢复竞争失败。");
        jdbc.update("update task.task_attempt set status = 'QUEUED', started_at = null, ended_at = null where task_id = ? and attempt = ?", taskId, current.attempt());
        audit.append(new AuditFact("task-resumed:" + taskId + ":" + idempotencyKey, actor.tenantId(), workspaceId, actor.actorId(), taskId, "TASK_RESUMED", "ACCEPTED", "{}", currentTrace(taskId)));
        return get(actor, workspaceId, taskId);
    }

    @Override
    public Optional<TaskWorkItem> claimOne() {
        expireQueuedBatch();
        if (!fairDispatchEnabled) return transactions.execute(status -> claimInternal(" and source = 'USER'"));
        registerMissingDispatchScopes();
        for (var i = 0; i < claimScanLimit; i++) {
            var claimed = transactions.execute(status -> claimFromNextWorkspace());
            if (claimed != null && claimed.isPresent()) {
                metrics.claimAttempt("claimed");
                return claimed;
            }
        }
        metrics.claimAttempt("empty");
        return Optional.empty();
    }

    @Override
    public Optional<TaskWorkItem> claim(UUID taskId) {
        expireQueuedBatch();
        return transactions.execute(status -> claimInternal(" and source = 'EVALUATION' and id = ?", taskId));
    }

    @Override
    @Transactional
    public RemoteTaskWakeStatus wakeRemote(UUID tenantId, UUID workspaceId, UUID taskId, int attempt, UUID operationId) {
        // 根预算锁先于 Task 锁；只唤醒同 attempt、同 operationId 的等待项，并重新计算剩余活动期限。
        var rootTaskId = taskRoot(taskId);
        if (rootTaskId == null) return RemoteTaskWakeStatus.NOT_WAITING;
        var scope = lockBudgetScope(rootTaskId);
        var current = jdbc.query("select status, attempt, actor_id, external_effect_operation_id, deadline_at, row_version "
                        + "from task.task where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? new RemoteWake(rs.getString("status"), rs.getInt("attempt"),
                        rs.getObject("actor_id", UUID.class), rs.getObject("external_effect_operation_id", UUID.class),
                        rs.getTimestamp("deadline_at").toInstant(), rs.getLong("row_version")) : null,
                taskId, tenantId, workspaceId);
        if (current == null || current.attempt() != attempt || !operationId.equals(current.operationId()))
            return RemoteTaskWakeStatus.NOT_WAITING;
        if (List.of("SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED").contains(current.status()))
            return RemoteTaskWakeStatus.TERMINAL;
        if (!"WAITING_REMOTE".equals(current.status())) return RemoteTaskWakeStatus.NOT_WAITING;
        var now = Instant.now(clock);
        if (!now.isBefore(current.deadline()) || scope.activeUsedMs() + scope.activeReservedMs() >= scope.maxActiveMs()) {
            var code = now.isBefore(current.deadline()) ? "ROOT_ACTIVE_BUDGET_EXCEEDED" : "REMOTE_TASK_DEADLINE_EXCEEDED";
            jdbc.update("update task.task set status = 'TIMED_OUT', error_code = ?, error_detail = '远端等待超过 Task 期限或本地活动预算。', row_version = row_version + 1, updated_at = ? where id = ? and row_version = ? and status = 'WAITING_REMOTE'",
                    code, Timestamp.from(now), taskId, current.rowVersion());
            jdbc.update("update task.task_attempt set status = 'TIMED_OUT', ended_at = ? where task_id = ? and attempt = ? and status = 'WAITING_REMOTE'",
                    Timestamp.from(now), taskId, attempt);
            audit.append(new AuditFact("task-remote-expired:" + taskId + ":" + attempt, tenantId, workspaceId,
                    current.actorId(), taskId, "TASK_REMOTE_WAIT_EXPIRED", "TIMED_OUT", "{}", currentTrace(taskId)));
            return RemoteTaskWakeStatus.EXPIRED;
        }
        if (!tryQueueSlot(tenantId, workspaceId)) return RemoteTaskWakeStatus.CAPACITY_DEFERRED;
        var budgetDeadline = now.plusMillis(Math.max(0, scope.maxActiveMs() - scope.activeUsedMs() - scope.activeReservedMs()));
        var activeDeadline = budgetDeadline.isBefore(current.deadline()) ? budgetDeadline : current.deadline();
        var changed = jdbc.update("update task.task set status = 'QUEUED', active_deadline_at = ?, row_version = row_version + 1, updated_at = ? where id = ? and row_version = ? and attempt = ? and status = 'WAITING_REMOTE' and external_effect_operation_id = ?",
                Timestamp.from(activeDeadline), Timestamp.from(now), taskId, current.rowVersion(), attempt, operationId);
        if (changed == 0) return RemoteTaskWakeStatus.NOT_WAITING;
        jdbc.update("update task.task_attempt set status = 'QUEUED', started_at = null, ended_at = null where task_id = ? and attempt = ? and status = 'WAITING_REMOTE'",
                taskId, attempt);
        audit.append(new AuditFact("task-remote-resumed:" + taskId + ":" + attempt, tenantId, workspaceId,
                current.actorId(), taskId, "TASK_REMOTE_POLL_QUEUED", "ACCEPTED", "{}", currentTrace(taskId)));
        return RemoteTaskWakeStatus.WOKEN;
    }

    private void requireQueueSlot(UUID tenantId, UUID workspaceId) {
        if (!tryQueueSlot(tenantId, workspaceId)) throw taskCapacityExceeded();
    }

    private boolean tryQueueSlot(UUID tenantId, UUID workspaceId) {
        if (!tryQueueAdmissionLock()) {
            metrics.admissionRejected("lock_busy");
            return false;
        }
        return hasQueueCapacity(tenantId, workspaceId);
    }

    private boolean hasQueueCapacity(UUID tenantId, UUID workspaceId) {
        var queued = jdbc.queryForObject("select count(*) from task.task where status = 'QUEUED'", Long.class);
        if (queued != null && queued >= maxQueued) {
            metrics.admissionRejected("queue_full");
            return false;
        }
        var workspaceQueued = jdbc.queryForObject("select count(*) from task.task where tenant_id = ? and workspace_id = ? and status = 'QUEUED'",
                Long.class, tenantId, workspaceId);
        if (workspaceQueued != null && workspaceQueued >= maxQueuedPerWorkspace) {
            metrics.admissionRejected("workspace_full");
            return false;
        }
        return true;
    }

    private Optional<TaskWorkItem> claimInternal(String filter, Object... filterArgs) {
        var sql = "select id from task.task where status = 'QUEUED' and active_deadline_at > now() and deadline_at > now()" + filter + " order by created_at limit 1";
        var candidateId = jdbc.query(sql, rs -> rs.next() ? rs.getObject("id", UUID.class) : null, filterArgs);
        if (candidateId == null) return Optional.empty();
        var candidateRoot = taskRoot(candidateId);
        if (candidateRoot == null) return Optional.empty();
        if (tryLockBudgetScope(candidateRoot) == null) {
            metrics.claimAttempt("root_busy");
            return Optional.empty();
        }
        // 先锁根预算再锁候选 Task，保持与取消、结算相同的锁顺序并避免并发透支。
        return claimLockedCandidate(candidateId);
    }

    private Optional<TaskWorkItem> claimLockedCandidate(UUID candidateId) {
        var work = jdbc.query("select id, tenant_id, workspace_id, actor_id, principal_id, delegate_id, delegation_id, authorization_hash, agent_id, agent_version, prompt_id, prompt_version, model_profile_id, input_text, business_entity_type, business_entity_id, trace_id, attempt, row_version, active_deadline_at, deadline_at, source, capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash, root_task_id, parent_task_id, entry_protocol, run_kind, tool_name, tool_version, tool_binding_ref, tool_arguments_json::text, model_selection::text, quality_run_id from task.task where id = ? and status = 'QUEUED' and active_deadline_at > now() and deadline_at > now() for update skip locked",
                (ResultSetExtractor<Optional<TaskWorkItem>>) rs -> rs.next() ? Optional.of(mapWork(rs)) : Optional.empty(), candidateId);
        if (work.isEmpty()) {
            metrics.claimAttempt("task_busy");
            return Optional.empty();
        }
        var w = work.get();
        // 已排队任务在停止期间保持 QUEUED，恢复入口后再由正常租约流程领取。
        if (!operationalControl.taskAdmissionOpen(w.tenantId(), w.workspaceId())) {
            metrics.claimAttempt("paused");
            return Optional.empty();
        }
        if (!isExecutionAuthorized(w)) {
            var now = Timestamp.from(Instant.now(clock));
            jdbc.update("update task.task set status = 'FAILED', error_code = 'AUTHORIZATION_REVOKED', error_detail = '任务开始前 Workspace 授权已撤销。', row_version = row_version + 1, updated_at = ? where id = ? and status = 'QUEUED'",
                    now, w.id());
            jdbc.update("update task.task_attempt set status = 'FAILED', ended_at = ? where task_id = ? and status = 'QUEUED'", now, w.id());
            audit.append(new AuditFact("task-authorization-revoked:" + w.id(), w.tenantId(), w.workspaceId(), w.actorId(), w.id(), "TASK_AUTHORIZATION_REVOKED", "FAILED", "{}", w.traceId()));
            return Optional.empty();
        }
        var now = Instant.now(clock);
        var activeReserved = reserveActive(w, now);
        if (activeReserved <= 0) {
            jdbc.update("update task.task set status = 'TIMED_OUT', error_code = 'ROOT_ACTIVE_BUDGET_EXCEEDED', error_detail = '根 Task 活跃时间预算已耗尽。', row_version = row_version + 1, updated_at = ? where id = ? and status = 'QUEUED'", Timestamp.from(now), w.id());
            jdbc.update("update task.task_attempt set status = 'TIMED_OUT', ended_at = ? where task_id = ? and attempt = ? and status = 'QUEUED'", Timestamp.from(now), w.id(), w.attempt());
            return Optional.empty();
        }
        var activeDeadline = now.plusMillis(activeReserved).isBefore(w.deadline()) ? now.plusMillis(activeReserved) : w.deadline();
        var databaseNow = jdbc.queryForObject("select clock_timestamp()", Timestamp.class).toInstant();
        var leaseUntil = databaseNow.plus(leaseDuration).isBefore(activeDeadline) ? databaseNow.plus(leaseDuration) : activeDeadline;
        var activeKey = "active:" + UUID.randomUUID();
        var leaseOwnerId = UUID.randomUUID();
        jdbc.update("update task.budget_scope set active_reserved_ms = active_reserved_ms + ? where root_task_id = ?", activeReserved, w.rootTaskId());
        jdbc.update("insert into task.budget_reservation(root_task_id, reservation_key, task_id, attempt, kind, reserved_amount, state) values (?, ?, ?, ?, 'ACTIVE', ?, 'RESERVED')",
                w.rootTaskId(), activeKey, w.id(), w.attempt(), activeReserved);
        var leaseFence = jdbc.queryForObject("update task.task set status = 'RUNNING', active_deadline_at = ?, active_budget_reservation_key = ?, active_reserved_ms = ?, lease_owner_id = ?, lease_fence = lease_fence + 1, lease_until = ?, row_version = row_version + 1, updated_at = ? where id = ? and row_version = ? returning lease_fence",
                Long.class, Timestamp.from(activeDeadline), activeKey, activeReserved, leaseOwnerId, Timestamp.from(leaseUntil), Timestamp.from(now), w.id(), w.rowVersion());
        jdbc.update("update task.task_attempt set status = 'RUNNING', started_at = ? where task_id = ? and attempt = ?", Timestamp.from(now), w.id(), w.attempt());
        return Optional.of(new TaskWorkItem(w.id(), w.tenantId(), w.workspaceId(), w.actorId(), w.agentId(), w.agentVersion(), w.promptId(), w.promptVersion(), w.modelProfileId(), w.inputText(), w.businessEntityType(), w.businessEntityId(), w.traceId(), w.attempt(), w.rowVersion() + 1, activeDeadline, w.deadline(), w.source(), w.assetBinding(), w.principalId(), w.delegationId(), w.authorizationHash(), w.rootTaskId(), w.parentTaskId(), w.entryProtocol(), w.runKind(), w.toolName(), w.toolVersion(), w.toolBindingRef(), w.toolArgumentsJson(), leaseOwnerId, leaseFence, w.qualityRunId(), w.modelSelection()));
    }

    private void registerMissingDispatchScopes() {
        transactions.executeWithoutResult(status -> jdbc.update("insert into task.dispatch_scope(tenant_id, workspace_id) "
                        + "select t.tenant_id, t.workspace_id from task.task t where t.status = 'QUEUED' and t.source = 'USER' "
                        + "and not exists (select 1 from task.dispatch_scope d where d.tenant_id = t.tenant_id and d.workspace_id = t.workspace_id) "
                        + "group by t.tenant_id, t.workspace_id order by t.tenant_id, t.workspace_id limit 64 on conflict do nothing"));
    }

    private Optional<TaskWorkItem> claimFromNextWorkspace() {
        var scope = jdbc.query("select d.tenant_id, d.workspace_id, d.candidate_created_at, d.candidate_id "
                        + "from task.dispatch_scope d where exists (select 1 from task.task t where t.tenant_id = d.tenant_id "
                        + "and t.workspace_id = d.workspace_id and t.source = 'USER' and t.status = 'QUEUED' "
                        + "and t.active_deadline_at > now() and t.deadline_at > now()) "
                        + "order by d.last_turn, d.tenant_id, d.workspace_id limit 1 for update of d skip locked",
                rs -> rs.next() ? new DispatchScope(rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                        rs.getTimestamp("candidate_created_at"), rs.getObject("candidate_id", UUID.class)) : null);
        if (scope == null) return Optional.empty();
        jdbc.update("update task.dispatch_scope set last_turn = nextval('task.dispatch_turn_seq'), updated_at = now() "
                        + "where tenant_id = ? and workspace_id = ?", scope.tenantId(), scope.workspaceId());

        for (var i = 0; i < claimScanLimit; i++) {
            var candidate = jdbc.query("select id, root_task_id, created_at from task.task where tenant_id = ? and workspace_id = ? "
                            + "and status = 'QUEUED' and source = 'USER' and active_deadline_at > now() and deadline_at > now() "
                            + "and (?::timestamptz is null or (created_at, id) > (?, ?)) order by created_at, id limit 1",
                    rs -> rs.next() ? new DispatchCandidate(rs.getObject("id", UUID.class), rs.getObject("root_task_id", UUID.class),
                            rs.getTimestamp("created_at")) : null,
                    scope.tenantId(), scope.workspaceId(), scope.candidateCreatedAt(), scope.candidateCreatedAt(), scope.candidateId());
            if (candidate == null) {
                jdbc.update("update task.dispatch_scope set candidate_created_at = null, candidate_id = null, updated_at = now() "
                                + "where tenant_id = ? and workspace_id = ?", scope.tenantId(), scope.workspaceId());
                return Optional.empty();
            }
            // 持久 keyset 游标越过被锁候选，避免固定队首长期遮住后续工作。
            jdbc.update("update task.dispatch_scope set candidate_created_at = ?, candidate_id = ?, updated_at = now() "
                            + "where tenant_id = ? and workspace_id = ?", candidate.createdAt(), candidate.id(),
                    scope.tenantId(), scope.workspaceId());
            if (tryLockBudgetScope(candidate.rootTaskId()) == null) {
                metrics.claimAttempt("root_busy");
                continue;
            }
            var work = claimLockedCandidate(candidate.id());
            if (work.isPresent()) return work;
        }
        return Optional.empty();
    }

    private record DispatchScope(UUID tenantId, UUID workspaceId, Timestamp candidateCreatedAt, UUID candidateId) { }
    private record DispatchCandidate(UUID id, UUID rootTaskId, Timestamp createdAt) { }

    @Override
    @Transactional
    public boolean renewLease(TaskWorkItem workItem) {
        if (workItem == null) return false;
        return jdbc.update("update task.task set lease_until = least(clock_timestamp() + (? * interval '1 millisecond'), active_deadline_at) "
                        + "where id = ? and tenant_id = ? and status = 'RUNNING' and attempt = ? and lease_owner_id = ? and lease_fence = ? "
                        + "and lease_until > clock_timestamp() and active_deadline_at > clock_timestamp() and deadline_at > clock_timestamp()",
                leaseDuration.toMillis(), workItem.id(), workItem.tenantId(), workItem.attempt(), workItem.leaseOwnerId(), workItem.leaseFence()) == 1;
    }

    @Override
    public boolean isExecutionAuthorized(TaskWorkItem workItem) {
        if (workItem == null) return false;
        // EVALUATION 是受限服务端来源；必须保留显式评测权限，且禁止携带委托身份。
        if ("EVALUATION".equals(workItem.source()))
            return workItem.principalId() == null && workItem.delegationId() == null && workItem.authorizationHash() == null
                    && workspaces.isAuthorized(workItem.tenantId(), workItem.actorId(), workItem.workspaceId(), "task:create")
                    && workspaces.isAuthorized(workItem.tenantId(), workItem.actorId(), workItem.workspaceId(), "evaluation:run");
        if (!"USER".equals(workItem.source())) return false;
        if (workItem.delegationId() == null)
            return workItem.principalId() == null && workItem.authorizationHash() == null
                    && workspaces.isAuthorized(workItem.tenantId(), workItem.actorId(), workItem.workspaceId(), "task:create");
        return currentDelegation(workItem).filter(actor -> actor.can("task:create"))
                .filter(actor -> workspaces.isAuthorized(actor.tenantId(), actor.principalId(), workItem.workspaceId(), "task:create"))
                .filter(actor -> workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workItem.workspaceId(), "task:create"))
                .filter(actor -> !IdentityService.MCP_AUDIENCE.equals(actor.delegationAudience())
                        || validMcpReadonlyTask(actor, workItem.workspaceId(), workItem.entryProtocol(), workItem.source(),
                        workItem.businessEntityType(), workItem.businessEntityId(), workItem.agentId(), workItem.agentVersion(),
                        workItem.assetBinding())).isPresent();
    }

    @Override
    @Transactional
    public TaskExecutionCheck checkExecution(ActorContext actor, UUID workspaceId, UUID taskId, int attempt) {
        if (actor == null) return new TaskExecutionCheck(false, "AUTHORIZATION_REVOKED", "执行身份缺失。", null);
        var rootTaskId = taskRoot(taskId);
        if (rootTaskId == null) return new TaskExecutionCheck(false, "TASK_NOT_FOUND", "Task 不存在或不可见。", null);
        // 同根先锁预算、后锁 Task，与领取/取消/结算保持统一顺序。
        // ponytail: 根预算锁在 Execution 外部调用期间也保持，首版以单根串行换取取消与提交的明确先后；有并行写入需求再拆专用 permit 行。
        lockBudgetScope(rootTaskId);
        var row = jdbc.query("select status, attempt, active_deadline_at, deadline_at, source, actor_id, principal_id, delegate_id, delegation_id, authorization_hash, lease_until, entry_protocol, agent_id, agent_version, business_entity_type, business_entity_id, capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash from task.task where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? new Object[]{rs.getString("status"), rs.getInt("attempt"), rs.getTimestamp("active_deadline_at").toInstant(), rs.getTimestamp("deadline_at").toInstant(), rs.getString("source"), rs.getObject("actor_id", UUID.class), rs.getObject("principal_id", UUID.class), rs.getObject("delegate_id", UUID.class), rs.getObject("delegation_id", UUID.class), rs.getString("authorization_hash"), rs.getTimestamp("lease_until"), rs.getString("entry_protocol"), rs.getObject("agent_id", UUID.class), rs.getString("agent_version"), rs.getString("business_entity_type"), rs.getString("business_entity_id"), rs.getObject("capability_id", UUID.class), rs.getString("capability_version"), rs.getString("capability_hash"), rs.getObject("skill_id", UUID.class), rs.getString("skill_version"), rs.getString("skill_hash")} : null,
                taskId, actor.tenantId(), workspaceId);
        if (row == null) return new TaskExecutionCheck(false, "TASK_NOT_FOUND", "Task 不存在或不可见。", null);
        var source = (String) row[4];
        var taskAudience = audienceForEntryProtocol((String) row[11]);
        var identityMatches = actor.delegated()
                ? actor.type() == io.eaf.shared.ActorType.AGENT && actor.actorId().equals(row[5])
                    && actor.principalId().equals(row[6]) && actor.actorId().equals(row[7])
                    && actor.delegationId().equals(row[8]) && actor.authorizationHash().equals(row[9])
                    && taskAudience != null && taskAudience.equals(actor.delegationAudience())
                : !actor.delegated() && actor.actorId().equals(row[5]) && row[6] == null && row[7] == null && row[8] == null && row[9] == null;
        if (!identityMatches) return new TaskExecutionCheck(false, "AUTHORIZATION_REVOKED", "Task 身份快照与当前执行身份不一致。", source);
        if (!"RUNNING".equals(row[0]) || ((Integer) row[1]) != attempt) return new TaskExecutionCheck(false, "TASK_NOT_RUNNING", "Task 当前不允许继续执行。", source);
        var now = Instant.now(clock);
        if (((Instant) row[2]).isBefore(now) || ((Instant) row[3]).isBefore(now)) return new TaskExecutionCheck(false, "DEADLINE_EXCEEDED", "Task 截止时间已到。", source);
        if (row[10] == null || !((Timestamp) row[10]).toInstant().isAfter(now)) return new TaskExecutionCheck(false, "WORKER_LEASE_EXPIRED", "Task Worker 租约已失效。", source);
        if (actor.delegated()) {
            var current = taskAudience != null && taskAudience.equals(actor.delegationAudience())
                    && identities.resolveDelegation(actor.tenantId(), actor.principalId(), actor.actorId(), actor.delegationId(), workspaceId, taskAudience)
                    .filter(a -> a.authorizationHash().equals(actor.authorizationHash())).filter(a -> a.can("task:create")).isPresent();
            var scopedMcpTask = !IdentityService.MCP_AUDIENCE.equals(taskAudience)
                    || validMcpReadonlyTask(actor, workspaceId, (String) row[11], source,
                    (String) row[14], (String) row[15], (UUID) row[12], (String) row[13],
                    new TaskAssetBinding((UUID) row[16], (String) row[17], (String) row[18],
                            (UUID) row[19], (String) row[20], (String) row[21]));
            if (!current || !scopedMcpTask || !workspaces.isAuthorized(actor.tenantId(), actor.principalId(), workspaceId, "task:create")
                    || !workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "task:create"))
                return new TaskExecutionCheck(false, "AUTHORIZATION_REVOKED", "委托、Workspace 或 Agent 授权已撤销或变化。", source);
        } else if ("EVALUATION".equals(source)) {
            // 领取后仍复核评测动作；权限撤销时评测 Task 不能继续运行。
            if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "task:create")
                    || !workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "evaluation:run"))
                return new TaskExecutionCheck(false, "AUTHORIZATION_REVOKED", "评测 Workspace 授权已撤销。", source);
        } else if (!"USER".equals(source)
                || !workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "task:create"))
            return new TaskExecutionCheck(false, "AUTHORIZATION_REVOKED", "Workspace 执行授权已撤销。", source);
        try {
            requireAutomationTaskCurrent(actor, workspaceId, taskId, true);
        } catch (EafException denied) {
            return new TaskExecutionCheck(false, denied.code(), denied.getMessage(), source);
        }
        return new TaskExecutionCheck(true, null, "OK", source);
    }

    @Override
    @Transactional
    public BudgetReservation reserveModel(UUID taskId, int attempt, String reservationKey) {
        // 根预算行串行化同一请求的竞争；已用 reservationKey 不会再次触发模型调用。
        if (reservationKey == null || reservationKey.isBlank() || reservationKey.length() > 200)
            throw EafException.invalid("模型预算 reservationKey 必填且不超过 200 字符。");
        var rootTaskId = taskRoot(taskId);
        if (rootTaskId == null) return new BudgetReservation(false, 0, "TASK_NOT_FOUND");
        var scope = lockBudgetScope(rootTaskId);
        var task = lockRunningTask(taskId, attempt);
        if (task == null) return new BudgetReservation(false, 0, "TASK_NOT_RUNNING");
        if (!rootTaskId.equals(task.rootTaskId())) throw EafException.conflict("TASK_ROOT_CHANGED", "Task 根预算关系发生变化。");
        var existing = reservation(rootTaskId, reservationKey);
        if (existing != null) {
            if (!taskId.equals(existing.taskId()) || attempt != existing.attempt() || !"MODEL".equals(existing.kind()))
                throw EafException.conflict("BUDGET_RESERVATION_CONFLICT", "预算 reservationKey 已绑定到其他操作。");
            // 模型没有通用幂等协议；已存在的键不能再次触发外部调用，未知费用按已预留额保守保留。
            return new BudgetReservation(false, 0, "MODEL_RESERVATION_EXISTS");
        }
        if (task.stepsUsed() >= task.maxSteps() || scope.stepsUsed() >= scope.maxSteps()) return new BudgetReservation(false, 0, "MAX_STEPS_EXCEEDED");
        if (task.modelCalls() >= 8 || scope.modelCalls() >= scope.maxModelCalls()) return new BudgetReservation(false, 0, "MODEL_CALL_LIMIT");
        var budget = (int) Math.min(Integer.MAX_VALUE, scope.maxTokens() - scope.tokenUsed() - scope.tokenReserved());
        if (budget <= 0) return new BudgetReservation(false, 0, "BUDGET_EXCEEDED");
        jdbc.update("update task.budget_scope set steps_used = steps_used + 1, model_calls = model_calls + 1, token_reserved = token_reserved + ? where root_task_id = ?",
                budget, rootTaskId);
        jdbc.update("update task.task set steps_used = steps_used + 1, model_calls = model_calls + 1, token_reserved = token_reserved + ? where id = ? and attempt = ? and status = 'RUNNING'",
                budget, taskId, attempt);
        jdbc.update("insert into task.budget_reservation(root_task_id, reservation_key, task_id, attempt, kind, reserved_amount, state) values (?, ?, ?, ?, 'MODEL', ?, 'RESERVED')",
                rootTaskId, reservationKey, taskId, attempt, budget);
        return new BudgetReservation(true, budget, null);
    }

    @Override
    @Transactional
    public BudgetReservation reserveTool(UUID taskId, int attempt, String reservationKey) {
        // 工具预算在 Execution 之前消耗，避免失败重试绕过根级调用上限。
        if (reservationKey == null || reservationKey.isBlank() || reservationKey.length() > 200)
            throw EafException.invalid("工具预算 reservationKey 必填且不超过 200 字符。");
        var rootTaskId = taskRoot(taskId);
        if (rootTaskId == null) return new BudgetReservation(false, 0, "TASK_NOT_FOUND");
        var scope = lockBudgetScope(rootTaskId);
        var task = lockRunningTask(taskId, attempt);
        if (task == null) return new BudgetReservation(false, 0, "TASK_NOT_RUNNING");
        if (!rootTaskId.equals(task.rootTaskId())) throw EafException.conflict("TASK_ROOT_CHANGED", "Task 根预算关系发生变化。");
        var existing = reservation(rootTaskId, reservationKey);
        if (existing != null) {
            if (!taskId.equals(existing.taskId()) || attempt != existing.attempt() || !"TOOL".equals(existing.kind()))
                throw EafException.conflict("BUDGET_RESERVATION_CONFLICT", "预算 reservationKey 已绑定到其他操作。");
            return new BudgetReservation(true, 0, null);
        }
        if (task.stepsUsed() >= task.maxSteps() || scope.stepsUsed() >= scope.maxSteps()) return new BudgetReservation(false, 0, "MAX_STEPS_EXCEEDED");
        if (task.toolCalls() >= 16 || scope.toolCalls() >= scope.maxToolCalls()) return new BudgetReservation(false, 0, "TOOL_CALL_LIMIT");
        jdbc.update("update task.budget_scope set steps_used = steps_used + 1, tool_calls = tool_calls + 1 where root_task_id = ?",
                rootTaskId);
        jdbc.update("update task.task set steps_used = steps_used + 1, tool_calls = tool_calls + 1 where id = ? and attempt = ? and status = 'RUNNING'",
                taskId, attempt);
        jdbc.update("insert into task.budget_reservation(root_task_id, reservation_key, task_id, attempt, kind, reserved_amount, settled_amount, state, settled_at) values (?, ?, ?, ?, 'TOOL', 0, 0, 'SETTLED', ?)",
                rootTaskId, reservationKey, taskId, attempt, Timestamp.from(Instant.now(clock)));
        return new BudgetReservation(true, 0, null);
    }

    @Override
    @Transactional
    public BudgetReservation reserveRemotePoll(UUID taskId, int attempt, UUID operationId, String reservationKey) {
        // GetTask 是一次受限 Tool 调用；每次轮询沿用根 Task 预算并以唯一键防止同一轮重复计数。
        if (operationId == null || reservationKey == null || reservationKey.isBlank() || reservationKey.length() > 200)
            throw EafException.invalid("远端轮询预算参数无效。");
        var rootTaskId = taskRoot(taskId);
        if (rootTaskId == null) return new BudgetReservation(false, 0, "TASK_NOT_FOUND");
        var scope = lockBudgetScope(rootTaskId);
        var task = lockRunningTask(taskId, attempt);
        var activeOperation = jdbc.query("select external_effect_operation_id from task.task where id = ? and attempt = ?",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, taskId, attempt);
        if (task == null || !rootTaskId.equals(task.rootTaskId()) || !operationId.equals(activeOperation))
            return new BudgetReservation(false, 0, "REMOTE_TASK_NOT_RUNNING");
        var existing = reservation(rootTaskId, reservationKey);
        if (existing != null) {
            if (!taskId.equals(existing.taskId()) || attempt != existing.attempt() || !"TOOL".equals(existing.kind()))
                throw EafException.conflict("BUDGET_RESERVATION_CONFLICT", "远端轮询预算键已绑定到其他操作。");
            return new BudgetReservation(true, 0, null);
        }
        if (task.stepsUsed() >= task.maxSteps() || scope.stepsUsed() >= scope.maxSteps())
            return new BudgetReservation(false, 0, "MAX_STEPS_EXCEEDED");
        if (task.toolCalls() >= 16 || scope.toolCalls() >= scope.maxToolCalls())
            return new BudgetReservation(false, 0, "TOOL_CALL_LIMIT");
        jdbc.update("update task.budget_scope set steps_used = steps_used + 1, tool_calls = tool_calls + 1 where root_task_id = ?", rootTaskId);
        jdbc.update("update task.task set steps_used = steps_used + 1, tool_calls = tool_calls + 1 where id = ? and attempt = ? and status = 'RUNNING'",
                taskId, attempt);
        jdbc.update("insert into task.budget_reservation(root_task_id, reservation_key, task_id, attempt, kind, reserved_amount, settled_amount, state, settled_at) values (?, ?, ?, ?, 'TOOL', 0, 0, 'SETTLED', ?)",
                rootTaskId, reservationKey, taskId, attempt, Timestamp.from(Instant.now(clock)));
        return new BudgetReservation(true, 0, null);
    }

    @Override
    @Transactional
    public void settleModel(UUID taskId, int attempt, String reservationKey, Integer inputTokens, Integer outputTokens) {
        var rootTaskId = taskRoot(taskId);
        if (rootTaskId == null) throw EafException.notFound();
        lockBudgetScope(rootTaskId);
        var current = jdbc.query("select task_id, attempt, kind, reserved_amount, state from task.budget_reservation where root_task_id = ? and reservation_key = ? for update",
                rs -> rs.next() ? new Reservation(rs.getObject("task_id", UUID.class), rs.getInt("attempt"), rs.getString("kind"), rs.getLong("reserved_amount"), rs.getString("state")) : null,
                rootTaskId, reservationKey);
        if (current == null || !taskId.equals(current.taskId()) || attempt != current.attempt() || !"MODEL".equals(current.kind()))
            throw EafException.conflict("BUDGET_RESERVATION_MISSING", "模型预算结算没有匹配的预留记录。");
        if ("SETTLED".equals(current.state())) return;
        // Provider 用量未知时按整笔预留结算；已知用量大于预算时保留真实消耗，后续预留会自然失败。
        long used = inputTokens != null && outputTokens != null ? Math.max(0L, (long) inputTokens + outputTokens) : current.reservedAmount();
        jdbc.update("update task.budget_scope set token_reserved = greatest(0, token_reserved - ?), token_used = token_used + ? where root_task_id = ?",
                current.reservedAmount(), used, rootTaskId);
        jdbc.update("update task.task set token_reserved = greatest(0, token_reserved - ?), token_used = token_used + ? where id = ?",
                current.reservedAmount(), used, taskId);
        jdbc.update("update task.budget_reservation set state = 'SETTLED', settled_amount = ?, settled_at = ? where root_task_id = ? and reservation_key = ? and state = 'RESERVED'",
                used, Timestamp.from(Instant.now(clock)), rootTaskId, reservationKey);
    }

    @Override
    @Transactional
    public void recordToolExecution(UUID taskId, int attempt) {
        var rootTaskId = taskRoot(taskId);
        if (rootTaskId == null) return;
        lockBudgetScope(rootTaskId);
        jdbc.update("update task.task set tool_executions = tool_executions + 1 where id = ? and attempt = ?", taskId, attempt);
        jdbc.update("update task.budget_scope set tool_executions = tool_executions + 1 where root_task_id = ?", rootTaskId);
    }

    private TaskBudget lockRunningTask(UUID taskId, int attempt) {
        return jdbc.query("select root_task_id, max_steps, steps_used, model_calls, tool_calls, status from task.task where id = ? and attempt = ? and lease_until > now() and active_deadline_at > now() and deadline_at > now() for update",
                rs -> rs.next() && "RUNNING".equals(rs.getString("status"))
                        ? new TaskBudget(rs.getObject("root_task_id", UUID.class), rs.getInt("max_steps"), rs.getInt("steps_used"), rs.getInt("model_calls"), rs.getInt("tool_calls")) : null,
                taskId, attempt);
    }

    private BudgetScope lockBudgetScope(UUID rootTaskId) {
        var scope = jdbc.query("select max_steps, steps_used, max_model_calls, model_calls, max_tool_calls, tool_calls, max_tokens, token_used, token_reserved, max_active_ms, active_used_ms, active_reserved_ms from task.budget_scope where root_task_id = ? for update",
                rs -> rs.next() ? new BudgetScope(rs.getInt("max_steps"), rs.getInt("steps_used"), rs.getInt("max_model_calls"), rs.getInt("model_calls"),
                        rs.getInt("max_tool_calls"), rs.getInt("tool_calls"), rs.getLong("max_tokens"), rs.getLong("token_used"), rs.getLong("token_reserved"),
                        rs.getLong("max_active_ms"), rs.getLong("active_used_ms"), rs.getLong("active_reserved_ms")) : null,
                rootTaskId);
        if (scope == null) throw new IllegalStateException("Task 根预算不存在。");
        return scope;
    }

    private BudgetScope tryLockBudgetScope(UUID rootTaskId) {
        return jdbc.query("select max_steps, steps_used, max_model_calls, model_calls, max_tool_calls, tool_calls, max_tokens, token_used, token_reserved, max_active_ms, active_used_ms, active_reserved_ms "
                        + "from task.budget_scope where root_task_id = ? for update skip locked",
                rs -> rs.next() ? new BudgetScope(rs.getInt("max_steps"), rs.getInt("steps_used"), rs.getInt("max_model_calls"), rs.getInt("model_calls"),
                        rs.getInt("max_tool_calls"), rs.getInt("tool_calls"), rs.getLong("max_tokens"), rs.getLong("token_used"), rs.getLong("token_reserved"),
                        rs.getLong("max_active_ms"), rs.getLong("active_used_ms"), rs.getLong("active_reserved_ms")) : null,
                rootTaskId);
    }

    private Reservation reservation(UUID rootTaskId, String key) {
        return jdbc.query("select task_id, attempt, kind, reserved_amount, state from task.budget_reservation where root_task_id = ? and reservation_key = ?",
                rs -> rs.next() ? new Reservation(rs.getObject("task_id", UUID.class), rs.getInt("attempt"), rs.getString("kind"), rs.getLong("reserved_amount"), rs.getString("state")) : null,
                rootTaskId, key);
    }

    private UUID taskRoot(UUID taskId) {
        return jdbc.query("select root_task_id from task.task where id = ?", rs -> rs.next() ? rs.getObject(1, UUID.class) : null, taskId);
    }

    private UUID budgetScopeRoot(UUID tenantId, UUID workspaceId, UUID scopeId) {
        return jdbc.query("select root_task_id from task.budget_scope where scope_id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, scopeId, tenantId, workspaceId);
    }

    private long reserveActive(TaskWorkItem work, Instant now) {
        var scope = lockBudgetScope(work.rootTaskId());
        var available = scope.maxActiveMs() - scope.activeUsedMs() - scope.activeReservedMs();
        var slice = jdbc.query("select max_active_slice_ms, active_used_ms from task.task where id = ? for update",
                rs -> rs.next() ? new long[]{rs.getLong("max_active_slice_ms"), rs.wasNull() ? -1 : rs.getLong("active_used_ms")} : null,
                work.id());
        if (slice != null && slice[0] > 0) available = Math.min(available, slice[0] - slice[1]);
        var remainingMillis = Math.min(activeDuration.toMillis(), Duration.between(now, work.deadline()).toMillis());
        return Math.max(0, Math.min(available, remainingMillis));
    }

    private boolean isP21BatchTask(UUID tenantId, UUID workspaceId, UUID taskId) {
        return jdbc.queryForObject("select count(*) from task.task where id = ? and tenant_id = ? and workspace_id = ? "
                        + "and workflow_id = '58000000-0000-4000-8000-000000000018'::uuid",
                Integer.class, taskId, tenantId, workspaceId) > 0;
    }

    private Integer automationTaskAttempt(UUID taskId, UUID tenantId, UUID workspaceId) {
        return jdbc.query("select t.attempt from task.automation_task_binding b join task.task t on t.id = b.task_id "
                        + "where b.task_id = ? and b.tenant_id = ? and b.workspace_id = ?",
                rs -> rs.next() ? rs.getInt(1) : null, taskId, tenantId, workspaceId);
    }

    private boolean isP30AutomationTask(UUID taskId) {
        return Boolean.TRUE.equals(jdbc.query("select exists(select 1 from task.automation_task_binding where task_id = ?)",
                rs -> rs.next() && rs.getBoolean(1), taskId));
    }

    private void requireAutomationTaskCurrent(ActorContext actor, UUID workspaceId, UUID taskId, boolean execution) {
        var attempt = automationTaskAttempt(taskId, actor.tenantId(), workspaceId);
        if (attempt == null) return;
        if (p30Sources == null) throw EafException.conflict("AUTOMATION_SOURCE_UNAVAILABLE", "自动化来源校验器未就绪。");
        if (execution) p30Sources.requireTaskExecutionCurrent(actor, workspaceId, taskId, attempt);
        else p30Sources.requireTaskResultCurrent(actor, workspaceId, taskId, attempt);
    }

    private void requireP21BatchTaskOwner(ActorContext actor, UUID workspaceId, UUID taskId) {
        var owner = jdbc.query("select actor_id, workflow_id from task.task where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("actor_id", UUID.class), rs.getObject("workflow_id", UUID.class)} : null,
                taskId, actor.tenantId(), workspaceId);
        if (owner != null && UUID.fromString("58000000-0000-4000-8000-000000000018").equals(owner[1])
                && (actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated() || !actor.actorId().equals(owner[0])))
            throw EafException.notFound();
    }

    private record TaskBudget(UUID rootTaskId, int maxSteps, int stepsUsed, int modelCalls, int toolCalls) { }
    private record RemoteWake(String status, int attempt, UUID actorId, UUID operationId, Instant deadline,
                              long rowVersion) { }
    private record BudgetScope(int maxSteps, int stepsUsed, int maxModelCalls, int modelCalls,
                               int maxToolCalls, int toolCalls, long maxTokens, long tokenUsed, long tokenReserved,
                               long maxActiveMs, long activeUsedMs, long activeReservedMs) { }
    private record Reservation(UUID taskId, int attempt, String kind, long reservedAmount, String state) { }
    private record ExistingBudgetScope(UUID id, String requestHash) { }
    private record WorkflowDispatch(String requestHash, String state, UUID taskId) { }
    private record CancelTaskState(int attempt, long activeReservedMs, String activeReservationKey,
                                   long rowVersion, String status, Instant startedAt, UUID externalOperationId,
                                   String externalEffectStatus) { }

    @Override
    public TaskEvidence evidence(UUID tenantId, UUID workspaceId, UUID taskId) {
        var result = jdbc.query("select id, tenant_id, workspace_id, actor_id, agent_id, agent_version, prompt_id, prompt_version, status, attempt, row_version, trace_id, input_text, result_json::text, error_code, error_detail, source, created_at, updated_at, capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash, root_task_id, parent_task_id, entry_protocol, run_kind, external_effect_status, external_effect_operation_id, quality_run_id from task.task where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new TaskEvidence(mapSnapshotWithPrompt(rs), rs.getObject("prompt_id", UUID.class), rs.getString("prompt_version"), rs.getObject("quality_run_id", UUID.class)) : null, taskId, tenantId, workspaceId);
        if (result == null) throw EafException.notFound();
        if (qualityRunSources.isScenarioTask(tenantId, workspaceId, taskId)
                || qualityRunSources.isTeamImprovementTask(tenantId, workspaceId, taskId))
            return new TaskEvidence(redactScenarioTask(result.snapshot()), result.promptId(), result.promptVersion(), result.qualityRunId());
        return result;
    }

    @Override
    @Transactional
    public void complete(TaskWorkItem w, TaskRunner.RunOutcome outcome) {
        // 只有仍持有同一 lease fence 且租约有效的 Worker 能结束 Task。
        var ended = Timestamp.from(Instant.now(clock));
        lockBudgetScope(w.rootTaskId());
        var active = jdbc.query("select active_reserved_ms, active_budget_reservation_key from task.task where id = ? and tenant_id = ? and row_version = ? and status = 'RUNNING' and lease_owner_id = ? and lease_fence = ? and lease_until > now() for update",
                rs -> rs.next() ? new Object[]{rs.getLong("active_reserved_ms"), rs.getString("active_budget_reservation_key")} : null,
                w.id(), w.tenantId(), w.rowVersion(), w.leaseOwnerId(), w.leaseFence());
        if (active == null) return;
        var activeElapsed = jdbc.queryForObject("select coalesce(greatest(0, extract(epoch from (?::timestamptz - started_at)) * 1000)::bigint, 0) from task.task_attempt where task_id = ? and attempt = ?",
                Long.class, ended, w.id(), w.attempt());
        var changed = jdbc.update("update task.task set active_used_ms = active_used_ms + ?, active_reserved_ms = 0, active_budget_reservation_key = null, lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, status = ?, result_json = ?::jsonb, error_code = ?, error_detail = ?, row_version = row_version + 1, updated_at = ? where id = ? and tenant_id = ? and row_version = ? and status = 'RUNNING' and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                activeElapsed, outcome.status().name(), outcome.resultJson(), outcome.errorCode(), outcome.errorDetail(), ended, w.id(), w.tenantId(), w.rowVersion(), w.leaseOwnerId(), w.leaseFence());
        if (changed == 1) {
            settleActiveBudget(w.rootTaskId(), (String) active[1], w.id(), w.attempt(), (Long) active[0], activeElapsed, ended);
            jdbc.update("update task.task_attempt set status = ?, ended_at = ? where task_id = ? and attempt = ?", outcome.status().name(), ended, w.id(), w.attempt());
            audit.append(new AuditFact("task-completed:" + w.id() + ":" + w.rowVersion(), w.tenantId(), w.workspaceId(), w.actorId(), w.id(), "TASK_COMPLETED", outcome.status().name(), "{}", w.traceId()));
        }
    }

    @Override
    public List<TaskAttemptRecovery> recoverOnStartup() {
        var recovered = new java.util.ArrayList<TaskAttemptRecovery>();
        var skippedRoots = new java.util.HashSet<UUID>();
        for (var i = 0; i < 64; i++) {
            var scan = transactions.execute(status -> recoverOneExpired(skippedRoots));
            if (scan == null || !scan.candidateFound()) break;
            if (scan.blockedRoot() != null) skippedRoots.add(scan.blockedRoot());
            else if (scan.recovery() != null) recovered.add(scan.recovery());
        }
        expireQueuedBatch();
        metrics.recoveryProcessed(recovered.size());
        return List.copyOf(recovered);
    }

    private RecoveryScan recoverOneExpired(Set<UUID> skippedRoots) {
        var exclusions = rootExclusion(skippedRoots);
        var candidate = jdbc.query("select t.id, t.root_task_id from task.task t join task.budget_scope b on b.root_task_id = t.root_task_id "
                        + "where t.status = 'RUNNING' and (t.lease_until is null or t.lease_until <= clock_timestamp()) "
                        + exclusions.sql() + " order by t.root_task_id, t.id limit 1 for update of b skip locked",
                rs -> rs.next() ? new UUID[]{rs.getObject("id", UUID.class), rs.getObject("root_task_id", UUID.class)} : null,
                exclusions.args());
        if (candidate == null) return new RecoveryScan(false, null, null);
        var taskId = candidate[0];
        var rootTaskId = candidate[1];
        var running = jdbc.query("select t.attempt, t.active_reserved_ms, t.active_budget_reservation_key, t.external_effect_status, "
                        + "t.external_effect_operation_id, t.tenant_id, t.workspace_id, t.actor_id, a.started_at, clock_timestamp() as db_now "
                        + "from task.task t left join task.task_attempt a on a.task_id = t.id and a.attempt = t.attempt and a.status = 'RUNNING' "
                        + "where t.id = ? and t.status = 'RUNNING' and (t.lease_until is null or t.lease_until <= clock_timestamp()) "
                        + "for update of t skip locked",
                rs -> rs.next() ? new RunningBudget(rs.getInt("attempt"), rs.getLong("active_reserved_ms"),
                        rs.getString("active_budget_reservation_key"), rs.getString("external_effect_status"),
                        rs.getObject("external_effect_operation_id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("actor_id", UUID.class),
                        rs.getTimestamp("started_at") == null ? null : rs.getTimestamp("started_at").toInstant(),
                        rs.getTimestamp("db_now")) : null, taskId);
        if (running == null) return new RecoveryScan(true, rootTaskId, null);
        var now = running.databaseNow();
        var elapsed = running.startedAt() == null ? 0 : Math.max(0, Duration.between(running.startedAt(), now.toInstant()).toMillis());
        // 已保存远端 Task 标识的崩溃保留 WAITING_REMOTE；未知外部结果仍只沿原身份核验。
        var remotePending = "REMOTE_PENDING".equals(running.externalEffectStatus()) && running.externalOperationId() != null;
        var nextStatus = remotePending ? "WAITING_REMOTE" : "FAILED";
        var changed = jdbc.update("update task.task set active_used_ms = active_used_ms + ?, active_reserved_ms = 0, "
                        + "active_budget_reservation_key = null, lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, "
                        + "status = ?, error_code = ?, error_detail = ?, row_version = row_version + 1, updated_at = ? "
                        + "where id = ? and status = 'RUNNING' and (lease_until is null or lease_until <= clock_timestamp())",
                elapsed, nextStatus, remotePending ? null : "WORKER_LEASE_EXPIRED",
                remotePending ? null : "Worker 租约过期，运行结果不能覆盖后续 attempt。", now, taskId);
        if (changed != 1) return new RecoveryScan(true, rootTaskId, null);
        settleActiveBudget(rootTaskId, running.activeReservationKey(), taskId, running.attempt(),
                running.activeReservedMs(), elapsed, now);
        jdbc.update("update task.task_attempt set status = ?, ended_at = ? where task_id = ? and attempt = ? and status = 'RUNNING'",
                nextStatus, remotePending ? null : now, taskId, running.attempt());
        if (remotePending) audit.append(new AuditFact("task-remote-recovered:" + taskId + ":" + running.attempt(),
                running.tenantId(), running.workspaceId(), running.actorId(), taskId,
                "TASK_REMOTE_WAIT_RECOVERED", "WAITING_REMOTE", "{}", currentTrace(taskId)));
        return new RecoveryScan(true, null, new TaskAttemptRecovery(taskId, running.attempt()));
    }

    private record RecoveryScan(boolean candidateFound, UUID blockedRoot, TaskAttemptRecovery recovery) { }
    private record RootExclusion(String sql, Object[] args) { }

    private RootExclusion rootExclusion(Set<UUID> roots) {
        if (roots.isEmpty()) return new RootExclusion("", new Object[0]);
        var placeholders = java.util.stream.IntStream.range(0, roots.size()).mapToObj(index -> "?")
                .collect(java.util.stream.Collectors.joining(","));
        return new RootExclusion("and t.root_task_id not in (" + placeholders + ") ", roots.toArray());
    }

    private void settleActiveBudget(UUID rootTaskId, String key, UUID taskId, int attempt, long reserved, long elapsed, Timestamp ended) {
        if (key == null) return;
        jdbc.update("update task.budget_scope set active_reserved_ms = greatest(0, active_reserved_ms - ?), active_used_ms = active_used_ms + ? where root_task_id = ?",
                reserved, elapsed, rootTaskId);
        jdbc.update("update task.budget_reservation set state = 'SETTLED', settled_amount = ?, settled_at = ? where root_task_id = ? and reservation_key = ? and task_id = ? and attempt = ? and kind = 'ACTIVE' and state = 'RESERVED'",
                elapsed, ended, rootTaskId, key, taskId, attempt);
    }

    private record RunningBudget(int attempt, long activeReservedMs, String activeReservationKey,
                                 String externalEffectStatus, UUID externalOperationId, UUID tenantId,
                                 UUID workspaceId, UUID actorId, Instant startedAt, Timestamp databaseNow) { }

    private void expireQueuedBatch() {
        var skippedRoots = new java.util.HashSet<UUID>();
        var processed = 0;
        for (var i = 0; i < 64; i++) {
            var scan = transactions.execute(status -> expireOneQueued(skippedRoots));
            if (scan == null || !scan.candidateFound()) break;
            if (scan.blockedRoot() != null) skippedRoots.add(scan.blockedRoot());
            else if (scan.expired()) processed++;
        }
        metrics.recoveryProcessed(processed);
    }

    private ExpirationScan expireOneQueued(Set<UUID> skippedRoots) {
        var exclusions = rootExclusion(skippedRoots);
        var candidate = jdbc.query("select t.id, t.root_task_id from task.task t join task.budget_scope b on b.root_task_id = t.root_task_id "
                        + "where t.status = 'QUEUED' and (t.active_deadline_at <= clock_timestamp() or t.deadline_at <= clock_timestamp()) "
                        + exclusions.sql() + " order by t.root_task_id, t.id limit 1 for update of b skip locked",
                rs -> rs.next() ? new UUID[]{rs.getObject("id", UUID.class), rs.getObject("root_task_id", UUID.class)} : null,
                exclusions.args());
        if (candidate == null) return new ExpirationScan(false, null, false);
        var expired = jdbc.query("select attempt, clock_timestamp() as db_now from task.task where id = ? and status = 'QUEUED' "
                            + "and (active_deadline_at <= clock_timestamp() or deadline_at <= clock_timestamp()) for update skip locked",
                rs -> rs.next() ? new Object[]{rs.getInt("attempt"), rs.getTimestamp("db_now")} : null, candidate[0]);
        if (expired == null) return new ExpirationScan(true, candidate[1], false);
        var now = (Timestamp) expired[1];
        if (jdbc.update("update task.task set status = 'TIMED_OUT', error_code = 'DEADLINE_EXCEEDED', "
                            + "error_detail = 'Task 已超过截止时间。', row_version = row_version + 1, updated_at = ? "
                            + "where id = ? and status = 'QUEUED'", now, candidate[0]) == 1)
            jdbc.update("update task.task_attempt set status = 'TIMED_OUT', ended_at = ? where task_id = ? and attempt = ? and status = 'QUEUED'",
                    now, candidate[0], expired[0]);
        return new ExpirationScan(true, null, true);
    }

    private record ExpirationScan(boolean candidateFound, UUID blockedRoot, boolean expired) { }

    private Optional<Idempotent> findByIdempotency(ActorContext actor, UUID workspaceId, String key) {
        return jdbc.query("select id, request_hash from task.task where tenant_id = ? and workspace_id = ? and actor_id = ? and idempotency_key = ?",
                rs -> rs.next() ? Optional.of(new Idempotent(rs.getObject("id", UUID.class), rs.getString("request_hash"))) : Optional.empty(), actor.tenantId(), workspaceId, actor.actorId(), key);
    }

    private String storedIdempotencyKey(ActorContext actor, UUID workspaceId, String key) {
        if (!actor.delegated()) return key;
        return Hashing.sha256(String.join("\u001f", actor.tenantId().toString(), workspaceId.toString(),
                actor.principalId().toString(), actor.delegationId().toString(), key));
    }

    private boolean validDelegation(ActorContext actor, UUID workspaceId) {
        var entryProtocol = IdentityService.MCP_AUDIENCE.equals(actor.delegationAudience()) ? "MCP" : "REST";
        return validDelegation(actor, workspaceId, entryProtocol);
    }

    private boolean validDelegation(ActorContext actor, UUID workspaceId, String entryProtocol) {
        var audience = audienceForEntryProtocol(entryProtocol);
        return audience != null && audience.equals(actor.delegationAudience())
                && actor.type() == io.eaf.shared.ActorType.AGENT && workspaceId.equals(actor.delegationWorkspaceId())
                && identities.resolveDelegation(actor.tenantId(), actor.principalId(), actor.actorId(), actor.delegationId(),
                workspaceId, audience).filter(current -> current.authorizationHash().equals(actor.authorizationHash())).isPresent();
    }

    private String audienceForEntryProtocol(String entryProtocol) {
        return switch (entryProtocol == null ? "" : entryProtocol) {
            case "MCP" -> IdentityService.MCP_AUDIENCE;
            case "REST", "A2A" -> IdentityService.REST_AUDIENCE;
            default -> null;
        };
    }

    private boolean validMcpReadonlyTask(ActorContext actor, UUID workspaceId, String entryProtocol, String source,
                                         String businessEntityType, String businessEntityId, UUID agentId,
                                         String agentVersion,
                                         TaskAssetBinding binding) {
        var scope = identities.mcpReadonlyScope(actor).orElse(null);
        return "MCP".equals(entryProtocol) && "USER".equals(source) && businessEntityType == null
                && businessEntityId == null && scope != null && scope.workspaceId().equals(workspaceId)
                && SERVICE_REQUEST_PLAN_AGENT_ID.equals(agentId) && "1.0.0".equals(agentVersion)
                && binding != null && scope.capabilityId().equals(binding.capabilityId())
                && scope.capabilityVersion().equals(binding.capabilityVersion())
                && scope.capabilityHash().equals(binding.capabilityHash());
    }

    private boolean isP27Tool(String name) {
        return java.util.Set.of("oa.todo.list", "oa.todo.get", "service.request.status.get",
                "service.request.result.record").contains(name);
    }

    private void requireP27WorkflowTaskCreation(CreateWorkflowTaskCommand command) {
        var provenance = command.workflowProvenance();
        var expectedWorkflow = switch (command.toolName()) {
            case "oa.todo.list" -> "58000000-0000-4000-8000-000000000019";
            case "oa.todo.get" -> "58000000-0000-4000-8000-00000000001a";
            case "service.request.status.get" -> "58000000-0000-4000-8000-00000000001b";
            case "service.request.result.record" -> "58000000-0000-4000-8000-00000000001c";
            default -> null;
        };
        var args = parseJsonNode(command.toolArgumentsJson());
        var validArgs = switch (command.toolName()) {
            case "oa.todo.list" -> args != null && args.isObject() && args.path("limit").canConvertToInt()
                    && args.path("limit").asInt() >= 1 && args.path("limit").asInt() <= 50;
            case "oa.todo.get" -> args != null && args.isObject() && args.path("todoId").isTextual()
                    && args.path("todoId").asText().matches("[A-Za-z0-9._:-]{1,160}");
            case "service.request.status.get" -> args != null && args.isObject() && validUuidText(args.path("workItemId").asText(null));
            case "service.request.result.record" -> args != null && args.isObject() && validUuidText(args.path("syncId").asText(null));
            default -> false;
        };
        if (provenance == null || !provenance.complete() || expectedWorkflow == null
                || !expectedWorkflow.equals(provenance.workflowId().toString()) || !"1.0.0".equals(provenance.workflowVersion())
                || !("read".equals(provenance.stepId()) || "record".equals(provenance.stepId()))
                || "service.request.result.record".equals(command.toolName()) != "record".equals(provenance.stepId())
                || !"1.0.0".equals(command.toolVersion()) || command.actor().type() != io.eaf.shared.ActorType.HUMAN
                || command.actor().delegated() || !"USER".equals(command.source()) || command.qualityRunId() != null
                || !P27_AGENT_ID.equals(command.agentId())
                || !P27_CAPABILITY_ID.equals(command.assetBinding().capabilityId())
                || !"1.0.0".equals(command.assetBinding().capabilityVersion()) || !validArgs)
            throw EafException.forbidden("P27 Tool Task 只能由对应固定 Workflow 步骤创建。");
    }

    private com.fasterxml.jackson.databind.JsonNode parseJsonNode(String value) {
        try { return value == null ? null : JSON.readTree(value); }
        catch (Exception invalid) { return null; }
    }

    private boolean validUuidText(String value) {
        try { return value != null && java.util.UUID.fromString(value).toString().equals(value); }
        catch (IllegalArgumentException invalid) { return false; }
    }

    // Task 边界再次验证委托快照，避免绕过 HTTP Filter 的内部调用继续使用旧 ActorContext。
    private void requireCurrentActor(ActorContext actor, UUID workspaceId, String action) {
        if (actor == null || actor.delegated() && !validDelegation(actor, workspaceId))
            throw EafException.forbidden("委托身份已撤销、过期或授权已变化。");
        workspaces.require(actor, workspaceId, action);
    }

    private EafException taskAdmissionStopped() {
        return EafException.conflict("WORKSPACE_TASK_ADMISSION_STOPPED", "Workspace 已停止接收新 Task。");
    }

    private Optional<ActorContext> currentDelegation(TaskWorkItem workItem) {
        if (workItem.principalId() == null || workItem.delegationId() == null || workItem.authorizationHash() == null)
            return Optional.empty();
        var audience = audienceForEntryProtocol(workItem.entryProtocol());
        if (audience == null) return Optional.empty();
        return identities.resolveDelegation(workItem.tenantId(), workItem.principalId(), workItem.actorId(),
                workItem.delegationId(), workItem.workspaceId(), audience)
                .filter(actor -> actor.authorizationHash().equals(workItem.authorizationHash()));
    }

    private String currentTrace(UUID taskId) {
        return jdbc.queryForObject("select trace_id from task.task where id = ?", String.class, taskId);
    }

    private TaskSnapshot mapSnapshot(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new TaskSnapshot(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class), rs.getObject("actor_id", UUID.class), rs.getObject("agent_id", UUID.class), rs.getString("agent_version"), rs.getString("prompt_version"), TaskStatus.valueOf(rs.getString("status")), rs.getInt("attempt"), rs.getLong("row_version"), rs.getString("trace_id"), rs.getString("input_text"), rs.getString("result_json"), rs.getString("error_code"), rs.getString("error_detail"), rs.getString("source"), rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant(), mapAssetBinding(rs), rs.getObject("root_task_id", UUID.class), rs.getObject("parent_task_id", UUID.class), rs.getString("entry_protocol"), rs.getString("run_kind"), rs.getString("external_effect_status"), rs.getObject("external_effect_operation_id", UUID.class) != null);
    }

    private TaskSnapshot mapSnapshotWithPrompt(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new TaskSnapshot(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class), rs.getObject("actor_id", UUID.class), rs.getObject("agent_id", UUID.class), rs.getString("agent_version"), rs.getString("prompt_version"), TaskStatus.valueOf(rs.getString("status")), rs.getInt("attempt"), rs.getLong("row_version"), rs.getString("trace_id"), rs.getString("input_text"), rs.getString("result_json"), rs.getString("error_code"), rs.getString("error_detail"), rs.getString("source"), rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant(), mapAssetBinding(rs), rs.getObject("root_task_id", UUID.class), rs.getObject("parent_task_id", UUID.class), rs.getString("entry_protocol"), rs.getString("run_kind"), rs.getString("external_effect_status"), rs.getObject("external_effect_operation_id", UUID.class) != null);
    }

    private TaskWorkItem mapWork(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new TaskWorkItem(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class), rs.getObject("actor_id", UUID.class), rs.getObject("agent_id", UUID.class), rs.getString("agent_version"), rs.getObject("prompt_id", UUID.class), rs.getString("prompt_version"), rs.getObject("model_profile_id", UUID.class), rs.getString("input_text"), rs.getString("business_entity_type"), rs.getString("business_entity_id"), rs.getString("trace_id"), rs.getInt("attempt"), rs.getLong("row_version"), rs.getTimestamp("active_deadline_at").toInstant(), rs.getTimestamp("deadline_at").toInstant(), rs.getString("source"), mapAssetBinding(rs), rs.getObject("principal_id", UUID.class), rs.getObject("delegation_id", UUID.class), rs.getString("authorization_hash"), rs.getObject("root_task_id", UUID.class), rs.getObject("parent_task_id", UUID.class), rs.getString("entry_protocol"), rs.getString("run_kind"), rs.getString("tool_name"), rs.getString("tool_version"), rs.getString("tool_binding_ref"), rs.getString("tool_arguments_json"), null, 0, rs.getObject("quality_run_id", UUID.class), readModelSelection(rs.getString("model_selection")));
    }

    private ModelProfileSelection readModelSelection(String value) {
        if (value == null || value.isBlank()) return null;
        try { return JSON.readValue(value, ModelProfileSelection.class); }
        catch (com.fasterxml.jackson.core.JsonProcessingException invalid) {
            throw EafException.conflict("MODEL_PROFILE_SNAPSHOT_INVALID", "任务模型选择快照无效。");
        }
    }

    private TaskAssetBinding mapAssetBinding(java.sql.ResultSet rs) throws java.sql.SQLException {
        var capabilityId = rs.getObject("capability_id", UUID.class);
        if (capabilityId == null) return null;
        return new TaskAssetBinding(capabilityId, rs.getString("capability_version"), rs.getString("capability_hash"),
                rs.getObject("skill_id", UUID.class), rs.getString("skill_version"), rs.getString("skill_hash"));
    }

    private void validateAssetBinding(TaskAssetBinding binding, String source) {
        validateAssetBinding(binding, source, false);
    }

    private void validateAssetBinding(TaskAssetBinding binding, String source, boolean workflow) {
        if (binding == null) return;
        if (!"USER".equals(source) && !(workflow && "EVALUATION".equals(source))
                || binding.capabilityId() == null || binding.capabilityVersion() == null
                || binding.skillId() == null || binding.skillVersion() == null
                || binding.capabilityHash() == null || !binding.capabilityHash().matches("[0-9a-f]{64}")
                || binding.skillHash() == null || !binding.skillHash().matches("[0-9a-f]{64}"))
            throw EafException.invalid("Capability Task 资产快照不完整或来源无效。");
    }

    private String bindingHash(TaskAssetBinding binding) {
        if (binding == null) return "direct-agent";
        return String.join(":", binding.capabilityId().toString(), binding.capabilityVersion(), binding.capabilityHash(),
                binding.skillId().toString(), binding.skillVersion(), binding.skillHash());
    }

    private Object bindingValue(TaskAssetBinding binding, int field) {
        if (binding == null) return null;
        return switch (field) {
            case 0 -> binding.capabilityId();
            case 1 -> binding.capabilityVersion();
            case 2 -> binding.capabilityHash();
            case 3 -> binding.skillId();
            case 4 -> binding.skillVersion();
            case 5 -> binding.skillHash();
            default -> throw new IllegalArgumentException("未知资产快照字段。");
        };
    }

    private record Idempotent(UUID id, String requestHash) { }
    private record ServiceRequestTaskBinding(UUID actorId, String source, String runKind, int attempt,
            UUID agentId, String agentVersion, String toolName, String toolVersion, String argumentsJson,
            UUID workflowInstanceId, UUID workflowId, String workflowVersion, String workflowStepId) { }
    private record EvaluationReviewerTaskBinding(String source, UUID qualityRunId, String runKind, String toolName,
                                                 String toolVersion, String toolBindingRef, UUID workflowInstanceId, UUID workflowId,
                                                 String workflowVersion, String workflowStepId, UUID rootTaskId, UUID parentTaskId) { }
    private record EvaluationReviewerRootBinding(UUID id, UUID rootTaskId, String source, UUID qualityRunId,
                                                 String runKind, UUID workflowInstanceId, UUID workflowId,
                                                 String workflowVersion, String workflowStepId) { }
}
// 本文件负责实现 EAF 的 JdbcTaskService.java 相关代码。
