package io.eaf.workflow.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.connector.api.P27BusinessConnectionService;
import io.eaf.context.api.ContextService;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.api.PublishedKnowledgeChunk;
import io.eaf.memory.api.TeamExperienceService.ExperienceRef;
import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.identity.api.IdentityService;
import io.eaf.identity.api.IdentityDirectory;
import io.eaf.execution.api.ExecutionService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.skill.api.SkillService;
import io.eaf.task.api.CreateBudgetScopeCommand;
import io.eaf.task.api.CreateWorkflowTaskCommand;
import io.eaf.task.api.P29BriefTaskSourceVerifier;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.P27BusinessSourceVerifier;
import io.eaf.task.api.P27BusinessTaskSource;
import io.eaf.task.api.WorkflowTaskCancellation;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.tool.api.ToolDefinition;
import io.eaf.workflow.api.CreateWorkflowCommand;
import io.eaf.workflow.api.CreateWorkflowInstanceCommand;
import io.eaf.workflow.api.CreateQualityRunWorkflowCommand;
import io.eaf.workflow.api.CreateWorkflowVersionCommand;
import io.eaf.workflow.api.CreateServiceRequestBatchCommand;
import io.eaf.workflow.api.WorkflowDefinition;
import io.eaf.workflow.api.WorkflowDependency;
import io.eaf.workflow.api.WorkflowInstance;
import io.eaf.workflow.api.ServiceRequestHandlingSnapshot;
import io.eaf.workflow.api.HumanWorkItem;
import io.eaf.workflow.api.HumanWorkItemPage;
import io.eaf.workflow.api.P16DigestSource;
import io.eaf.workflow.api.WorkflowOperationsCursor;
import io.eaf.workflow.api.WorkflowOperationsItem;
import io.eaf.workflow.api.WorkflowOperationsPage;
import io.eaf.workflow.api.WorkflowSourcePage;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.api.P27BusinessWorkflowService;
import io.eaf.workflow.api.WorkflowStepSpec;
import io.eaf.workflow.api.WorkflowStepType;
import io.eaf.workflow.api.ServiceRequestBatchSnapshot;
import io.eaf.workflow.api.TeamExperienceRef;
import io.eaf.workflow.api.CreateProjectBriefCommand;
import io.eaf.workflow.api.ProjectBriefArtifact;
import io.eaf.workflow.api.ProjectBriefInboxPage;
import io.eaf.workflow.api.ProjectBriefSnapshot;
import io.eaf.workflow.api.ProjectBriefTaskSource;
import io.eaf.workflow.api.ProjectBriefWorkItem;
import io.eaf.workflow.api.WorkflowHumanInboxPage;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.time.Instant;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcWorkflowService implements WorkflowService, P27BusinessWorkflowService, P27BusinessSourceVerifier,
        P29BriefTaskSourceVerifier {
    private static final UUID P12_RESULT_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000000d");
    private static final String P12_RESULT_WORKFLOW_VERSION = "1.0.0";
    private static final UUID P15_SERVICE_REQUEST_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000000e");
    private static final String P15_SERVICE_REQUEST_WORKFLOW_VERSION = "1.0.0";
    private static final UUID P16_SERVICE_REQUEST_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000000f");
    private static final UUID P21_SERVICE_REQUEST_BATCH_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-000000000018");
    private static final UUID P27_OA_LIST_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-000000000019");
    private static final UUID P27_OA_ITEM_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000001a");
    private static final UUID P27_SERVICE_STATE_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000001b");
    private static final UUID P27_RESULT_SYNC_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000001c");
    private static final UUID P29_PROJECT_BRIEF_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000001d");
    private static final String P29_PROJECT_BRIEF_WORKFLOW_VERSION = "1.0.0";
    private static final UUID P29_PROJECT_BRIEF_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000023");
    private static final UUID P29_PROJECT_BRIEF_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000023");
    private static final String P29_PROJECT_BRIEF_PROFILE = "PROJECT_BRIEF_PREPARE_V1";
    private static final String P29_EVIDENCE_SCHEMA = "P29_BRIEF_EVIDENCE_V1";
    private static final String P29_HUMAN_TASK_INPUT_SCHEMA = "{\"type\":\"object\",\"required\":[\"assigneeId\"],\"additionalProperties\":false,\"properties\":{\"assigneeId\":{\"type\":\"string\",\"minLength\":36,\"maxLength\":36}}}";
    private static final String P27_WORKFLOW_VERSION = "1.0.0";
    private static final String P27_OA_BINDING = "p27-oa.todo";
    private static final String P27_SERVICE_BINDING = "p27-service-desk.result";
    private static final String P16_SERVICE_REQUEST_WORKFLOW_VERSION = "1.0.0";
    private static final String P17_SERVICE_REQUEST_WORKFLOW_VERSION = "1.1.0";
    private static final String P18_SERVICE_REQUEST_WORKFLOW_VERSION = "1.2.0";
    private static final UUID P16_PREPARE_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000014");
    private static final UUID P16_SUMMARY_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000015");
    private static final UUID P21_KNOWLEDGE_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000016");
    private static final UUID P21_EXPERIENCE_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000017");
    private static final UUID P15_REGISTERING_WORKFLOW_ID = P15_SERVICE_REQUEST_WORKFLOW_ID;
    private static final Set<String> HUMAN_ITEM_STATUSES = Set.of("OPEN", "COMPLETED", "CANCELLED", "TIMED_OUT", "FAILED");
    private static final Set<String> HUMAN_OUTCOMES = Set.of("COMPLETED", "BLOCKED", "NEEDS_FOLLOWUP");
    //  1.1/1.2 仅映射服务器生成的可选用量投影； 1.0 的人工任务必填字段保持不变。
    private static final String HUMAN_TASK_INPUT_SCHEMA = "{\"type\":\"object\",\"required\":[\"assigneeId\",\"sharedBrief\",\"handlingAdvice\",\"cautions\"],\"additionalProperties\":false,\"properties\":{\"assigneeId\":{\"type\":\"string\",\"minLength\":36,\"maxLength\":36},\"sharedBrief\":{\"type\":\"string\",\"minLength\":1,\"maxLength\":2000},\"handlingAdvice\":{\"type\":\"string\",\"minLength\":1,\"maxLength\":2000},\"cautions\":{\"type\":\"string\",\"maxLength\":1000},\"teamExperienceUsage\":{\"type\":\"object\"}}}";
    private static final String HUMAN_TASK_OUTPUT_SCHEMA = "{\"type\":\"object\",\"required\":[\"outcome\",\"summary\",\"nextAction\",\"completedBy\",\"completedAt\"],\"additionalProperties\":false,\"properties\":{\"outcome\":{\"type\":\"string\",\"enum\":[\"COMPLETED\",\"BLOCKED\",\"NEEDS_FOLLOWUP\"]},\"summary\":{\"type\":\"string\",\"minLength\":1,\"maxLength\":2000},\"nextAction\":{\"type\":\"string\",\"maxLength\":1000},\"completedBy\":{\"type\":\"string\",\"minLength\":36,\"maxLength\":36},\"completedAt\":{\"type\":\"string\",\"minLength\":1,\"maxLength\":50}}}";
    private static final String P29_REVIEW_OUTPUT_SCHEMA = "{\"type\":\"object\",\"required\":[\"decision\",\"notes\",\"nextAction\",\"completedBy\",\"completedAt\",\"artifactVersion\"],\"additionalProperties\":false,\"properties\":{\"decision\":{\"type\":\"string\",\"enum\":[\"CONFIRMED\",\"NEEDS_FOLLOWUP\"]},\"notes\":{\"type\":\"string\",\"maxLength\":2000},\"nextAction\":{\"type\":\"string\",\"maxLength\":1000},\"completedBy\":{\"type\":\"string\",\"minLength\":36,\"maxLength\":36},\"completedAt\":{\"type\":\"string\",\"minLength\":1,\"maxLength\":50},\"artifactVersion\":{\"type\":\"string\",\"enum\":[\"2\"]}}}";
    private static final String P29_HANDOFF_OUTPUT_SCHEMA = "{\"type\":\"object\",\"required\":[\"disposition\",\"note\",\"completedBy\",\"completedAt\",\"artifactVersion\"],\"additionalProperties\":false,\"properties\":{\"disposition\":{\"type\":\"string\",\"enum\":[\"RECEIVED\",\"NEEDS_FOLLOWUP\"]},\"note\":{\"type\":\"string\",\"maxLength\":1000},\"completedBy\":{\"type\":\"string\",\"minLength\":36,\"maxLength\":36},\"completedAt\":{\"type\":\"string\",\"minLength\":1,\"maxLength\":50},\"artifactVersion\":{\"type\":\"string\",\"enum\":[\"2\"]}}}";
    private static final int MAX_STEPS = 24;
    private static final Duration WORKER_LEASE = Duration.ofSeconds(30);
    private static final Set<String> OPERATION_STATUSES = Set.of("QUEUED", "RUNNING", "WAITING_CHILD", "WAITING_HUMAN", "CANCELLING",
            "SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT");
    private static final Pattern INPUT_PATH = Pattern.compile("\\$\\.input\\.([A-Za-z][A-Za-z0-9_]*)");
    private static final Pattern STEP_PATH = Pattern.compile("\\$\\.steps\\.([a-z][a-z0-9-]{0,63})\\.output\\.([A-Za-z][A-Za-z0-9_]*)");
    private static final Pattern VERSION = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?");
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final CapabilityService capabilities;
    private final SkillService skills;
    private final ToolCatalog tools;
    private final TaskService tasks;
    private final IdentityService identities;
    private final IdentityDirectory identityDirectory;
    private final AuditPort audit;
    private final ExecutionService executions;
    private final ObjectMapper json;
    private final WorkflowSchemas schemas;
    private final ContextService contexts;
    private final P27BusinessConnectionService p27Connections;
    private KnowledgeService knowledge;
    @Value("${eaf.workflow.batch-enabled:true}")
    private boolean batchEnabled = true;

    public JdbcWorkflowService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces,
                               CapabilityService capabilities, SkillService skills, ToolCatalog tools,
                               TaskService tasks, IdentityService identities, ObjectMapper json, AuditPort audit,
                               ExecutionService executions, IdentityDirectory identityDirectory, ContextService contexts,
                               P27BusinessConnectionService p27Connections) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.capabilities = capabilities;
        this.skills = skills;
        this.tools = tools;
        this.tasks = tasks;
        this.identities = identities;
        this.json = json;
        this.schemas = new WorkflowSchemas(json);
        this.audit = audit;
        this.executions = executions;
        this.identityDirectory = identityDirectory;
        this.contexts = contexts;
        this.p27Connections = p27Connections;
    }

    @org.springframework.beans.factory.annotation.Autowired
    void knowledgeService(KnowledgeService service) { this.knowledge = service; }

    @Override
    @Transactional
    public WorkflowDefinition create(CreateWorkflowCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null || command.initialVersion() == null)
            throw EafException.invalid("Workflow 创建请求不完整。");
        var access = workspaces.require(command.actor(), command.workspaceId(), "workflow:write");
        var name = requireText(command.name(), 120, "name");
        if (!name.matches("[a-z0-9]+(?:[.-][a-z0-9]+)*")) throw EafException.invalid("name 只能包含小写字母、数字、点和连字符。");
        var description = requireText(command.description(), 500, "description");
        var id = UUID.randomUUID();
        try {
            jdbc.update("insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description) values (?, ?, ?, ?, ?, ?)",
                    id, access.tenantId(), access.workspaceId(), command.actor().actorId(), name, description);
        } catch (DuplicateKeyException e) {
            throw EafException.conflict("VERSION_CONFLICT", "Workflow 名称已存在。");
        }
        insertVersion(command.actor(), access.workspaceId(), id, name, description, command.initialVersion());
        return load(access.tenantId(), access.workspaceId(), id, command.initialVersion().version());
    }

    @Override
    @Transactional
    public WorkflowDefinition addVersion(ActorContext actor, UUID workspaceId, UUID workflowId,
                                         CreateWorkflowVersionCommand command) {
        var access = workspaces.require(actor, workspaceId, "workflow:write");
        var definition = loadDefinitionRow(access.tenantId(), workspaceId, workflowId);
        requireOwner(actor, definition.ownerId());
        insertVersion(actor, workspaceId, workflowId, definition.name(), definition.description(), command);
        return load(access.tenantId(), workspaceId, workflowId, command.version());
    }

    @Override
    public List<WorkflowDefinition> list(ActorContext actor, UUID workspaceId) {
        var access = workspaces.require(actor, workspaceId, "workflow:read");
        var keys = jdbc.query("select workflow_id, asset_version from workflow.version where tenant_id = ? and workspace_id = ? and status = 'PUBLISHED' order by workflow_id, asset_version",
                (rs, row) -> new VersionKey(rs.getObject("workflow_id", UUID.class), rs.getString("asset_version")),
                access.tenantId(), workspaceId);
        return keys.stream().map(key -> load(access.tenantId(), workspaceId, key.id(), key.version())).toList();
    }

    @Override
    public WorkflowDefinition get(ActorContext actor, UUID workspaceId, UUID workflowId, String version) {
        var access = workspaces.require(actor, workspaceId, "workflow:read");
        var result = load(access.tenantId(), workspaceId, workflowId, version);
        if (!"PUBLISHED".equals(result.status()) && !result.ownerId().equals(actor.actorId())) throw EafException.notFound();
        return result;
    }

    @Override
    @Transactional
    public WorkflowDefinition publish(ActorContext actor, UUID workspaceId, UUID workflowId,
                                      String version, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "workflow:publish");
        var definition = load(access.tenantId(), workspaceId, workflowId, version);
        requireOwner(actor, definition.ownerId());
        lockDraftVersion(access.tenantId(), workspaceId, workflowId, version, expectedVersion);
        // 锁定版本行后重读定义与依赖，避免并发草稿改动越过发布校验。
        definition = load(access.tenantId(), workspaceId, workflowId, version);
        var resolved = validateDefinition(actor, workspaceId, definition);
        var dependencies = resolved.values().stream().map(this::dependency)
                .sorted(Comparator.comparing(value -> value.capabilityId().toString() + "@" + value.capabilityVersion())).toList();
        for (var dependency : dependencies)
            jdbc.update("update workflow.capability_dependency set content_hash = ? where tenant_id = ? and workspace_id = ? and workflow_id = ? and workflow_version = ? and capability_id = ? and capability_version = ?",
                    dependency.contentHash(), access.tenantId(), workspaceId, workflowId, version,
                    dependency.capabilityId(), dependency.capabilityVersion());
        var contentHash = contentHash(definition, dependencies);
        var changed = jdbc.update("update workflow.version set status = 'PUBLISHED', content_hash = ?, row_version = row_version + 1 where tenant_id = ? and workspace_id = ? and workflow_id = ? and asset_version = ? and status = 'DRAFT' and row_version = ?",
                contentHash, access.tenantId(), workspaceId, workflowId, version, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "Workflow 发布竞争失败。");
        jdbc.update("insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id) values (?, ?, ?, ?, ?, 'PUBLISHED', ?)",
                UUID.randomUUID(), access.tenantId(), workspaceId, workflowId, version, actor.actorId());
        return load(access.tenantId(), workspaceId, workflowId, version);
    }

    @Override
    @Transactional
    public WorkflowDefinition withdraw(ActorContext actor, UUID workspaceId, UUID workflowId,
                                       String version, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "workflow:publish");
        var definition = load(access.tenantId(), workspaceId, workflowId, version);
        requireOwner(actor, definition.ownerId());
        if (!"PUBLISHED".equals(definition.status()) || definition.rowVersion() != expectedVersion)
            throw EafException.conflict("VERSION_CONFLICT", "只有当前已发布 Workflow 版本可以撤回。");
        var changed = jdbc.update("update workflow.version set status = 'WITHDRAWN', row_version = row_version + 1 where tenant_id = ? and workspace_id = ? and workflow_id = ? and asset_version = ? and status = 'PUBLISHED' and row_version = ?",
                access.tenantId(), workspaceId, workflowId, version, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "Workflow 撤回竞争失败。");
        jdbc.update("insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id) values (?, ?, ?, ?, ?, 'WITHDRAWN', ?)",
                UUID.randomUUID(), access.tenantId(), workspaceId, workflowId, version, actor.actorId());
        return load(access.tenantId(), workspaceId, workflowId, version);
    }

    @Override
    @Transactional
    public WorkflowInstance createInstance(CreateWorkflowInstanceCommand command) {
        if (command != null && (P12_RESULT_WORKFLOW_ID.equals(command.workflowId())
                || P15_SERVICE_REQUEST_WORKFLOW_ID.equals(command.workflowId())
                || P16_SERVICE_REQUEST_WORKFLOW_ID.equals(command.workflowId())
                || P21_SERVICE_REQUEST_BATCH_WORKFLOW_ID.equals(command.workflowId())
                || p27WorkflowId(command.workflowId())))
            throw EafException.forbidden("保留业务 Workflow 只能通过固定的业务入口启动。");
        return createInstance(command, null, false, false, false, false);
    }

    @Override
    @Transactional
    public ServiceRequestBatchSnapshot.Created createServiceRequestBatch(CreateServiceRequestBatchCommand command) {
        requireDirectHuman(command == null ? null : command.actor());
        if (!batchEnabled) throw EafException.conflict("BATCH_ADMISSION_DISABLED", "批次接入已关闭。");
        if (command.workspaceId() == null || command.idempotencyKey() == null
                || command.idempotencyKey().isBlank() || command.idempotencyKey().length() > 200
                || command.items() == null || command.items().isEmpty() || command.items().size() > 20
                || command.maxActiveItems() != null && (command.maxActiveItems() < 1 || command.maxActiveItems() > 4))
            throw EafException.invalid("批次需包含 1 到 20 项，maxActiveItems 范围为 1 到 4。");
        var actor = command.actor();
        var access = workspaces.require(actor, command.workspaceId(), "workflow:start");
        workspaces.require(actor, command.workspaceId(), "task:create");
        var ids = new HashSet<String>();
        var total = 0;
        var payload = json.createObjectNode();
        if (command.maxActiveItems() == null) payload.putNull("maxActiveItems");
        else payload.put("maxActiveItems", command.maxActiveItems());
        if (command.deadlineAt() == null) payload.putNull("deadlineAt");
        else payload.put("deadlineAt", command.deadlineAt().toString());
        var itemsJson = payload.putArray("items");
        for (var item : command.items()) {
            if (item == null || item.itemKey() == null || !item.itemKey().matches("[A-Za-z0-9_-]{1,64}")
                    || !ids.add(item.itemKey()) || item.requestText() == null || item.requestText().isBlank()
                    || item.requestText().length() > 8_000
                    || item.scenarioKey() != null && !item.scenarioKey().matches("[a-z][a-z0-9-]{0,63}")
                    || item.experienceRefs() == null || item.experienceRefs().size() > 3
                    || item.experienceRefs().stream().anyMatch(ref -> ref == null || ref.cardId() == null || ref.revision() < 1)
                    || item.experienceRefs().stream().map(TeamExperienceRef::cardId).distinct().count() != item.experienceRefs().size()
                    || !item.experienceRefs().isEmpty() && item.scenarioKey() == null)
                throw EafException.invalid("批次项目、经验引用或场景无效。");
            total += item.requestText().length();
            if (total > 64_000) throw EafException.invalid("批次请求文本合计不能超过 64000 个 Java 字符。");
            var row = itemsJson.addObject().put("itemKey", item.itemKey()).put("requestText", item.requestText());
            if (item.scenarioKey() == null) row.putNull("scenarioKey"); else row.put("scenarioKey", item.scenarioKey());
            row.set("experienceRefs", json.valueToTree(item.experienceRefs()));
        }
        var payloadHash = Hashing.sha256(canonical(payload));
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(), command.workspaceId().toString(),
                actor.actorId().toString(), command.idempotencyKey()));
        var prior = jdbc.query("select id, payload_hash from workflow.batch where tenant_id = ? and workspace_id = ? and initiator_id = ? and idempotency_key_hash = ?",
                rs -> rs.next() ? new ExistingBatch(rs.getObject("id", UUID.class), rs.getString("payload_hash")) : null,
                access.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) {
            if (!payloadHash.equals(prior.payloadHash())) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一批次幂等键对应了不同请求。");
            return new ServiceRequestBatchSnapshot.Created(getServiceRequestBatch(actor, command.workspaceId(), prior.id()), true);
        }
        var deadline = command.deadlineAt() == null
                ? jdbc.queryForObject("select now() + interval '24 hours'", Timestamp.class).toInstant()
                : command.deadlineAt().truncatedTo(ChronoUnit.MICROS);
        if (!Boolean.TRUE.equals(jdbc.queryForObject("select ?::timestamptz > now() and ?::timestamptz <= now() + interval '30 days'",
                Boolean.class, Timestamp.from(deadline), Timestamp.from(deadline))))
            throw EafException.invalid("批次截止时间必须晚于当前时间且不超过 30 天。");
        for (var item : command.items()) if (!item.experienceRefs().isEmpty()) {
            workspaces.require(actor, command.workspaceId(), "memory:read");
            contexts.resolveTeamExperiences(actor, command.workspaceId(), item.scenarioKey(), item.experienceRefs().stream()
                    .map(ref -> new ExperienceRef(ref.cardId(), ref.revision())).toList());
        }
        var definition = load(access.tenantId(), command.workspaceId(), P21_SERVICE_REQUEST_BATCH_WORKFLOW_ID, "1.0.0");
        if (!"PUBLISHED".equals(definition.status()) || definition.contentHash() == null)
            throw EafException.conflict("WORKFLOW_DEPENDENCY_CHANGED", "固定分析流程当前不可用。");
        var batchId = UUID.randomUUID();
        jdbc.update("insert into workflow.batch(id, tenant_id, workspace_id, initiator_id, idempotency_key_hash, payload_hash, definition_id, definition_version, definition_hash, item_count, max_active_items, deadline_at, status) values (?, ?, ?, ?, ?, ?, ?, '1.0.0', ?, ?, ?, ?, 'PENDING')",
                batchId, access.tenantId(), command.workspaceId(), actor.actorId(), keyHash, payloadHash,
                P21_SERVICE_REQUEST_BATCH_WORKFLOW_ID, definition.contentHash(), command.items().size(),
                command.maxActiveItems() == null ? 4 : command.maxActiveItems(), Timestamp.from(deadline));
        for (int ordinal = 0; ordinal < command.items().size(); ordinal++) {
            var item = command.items().get(ordinal);
            var refs = canonical(json.valueToTree(item.experienceRefs()));
            var inputHash = Hashing.sha256(canonical(json.valueToTree(item)));
            jdbc.update("insert into workflow.batch_item(batch_id, tenant_id, workspace_id, item_key, ordinal, request_text, scenario_key, experience_refs_json, input_hash, stable_create_key, status) values (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, 'PENDING')",
                    batchId, access.tenantId(), command.workspaceId(), item.itemKey(), ordinal + 1, item.requestText(),
                    item.scenarioKey(), refs, inputHash, batchId + ":" + item.itemKey());
        }
        return new ServiceRequestBatchSnapshot.Created(getServiceRequestBatch(actor, command.workspaceId(), batchId), false);
    }

    @Override
    public ServiceRequestBatchSnapshot getServiceRequestBatch(ActorContext actor, UUID workspaceId, UUID batchId) {
        var batch = requireBatch(actor, workspaceId, batchId, "workflow:read");
        var rows = jdbc.query("select i.item_key, i.ordinal, i.status, i.row_version, i.workflow_instance_id, i.error_code, "
                        + "(select p.child_task_id from workflow.parallel_branch p where p.instance_id = i.workflow_instance_id and p.role = 'knowledge') knowledge_task_id, "
                        + "(select p.child_task_id from workflow.parallel_branch p where p.instance_id = i.workflow_instance_id and p.role = 'experience') experience_task_id "
                        + "from workflow.batch_item i where i.batch_id = ? order by i.ordinal",
                (rs, row) -> new ServiceRequestBatchSnapshot.Item(rs.getString("item_key"), rs.getInt("ordinal"),
                        rs.getString("status"), rs.getLong("row_version"), rs.getObject("workflow_instance_id", UUID.class),
                        rs.getString("error_code"), rs.getObject("knowledge_task_id", UUID.class),
                        rs.getObject("experience_task_id", UUID.class)), batchId);
        var counts = new java.util.TreeMap<String, Integer>();
        rows.forEach(item -> counts.merge(item.status(), 1, Integer::sum));
        return new ServiceRequestBatchSnapshot(batch.id(), workspaceId, batch.status(), batch.version(), batch.createdAt(),
                batch.deadlineAt(), batch.maxActiveItems(), batch.itemCount(), counts, rows);
    }

    @Override
    public ServiceRequestBatchSnapshot.Detail getServiceRequestBatchItem(ActorContext actor, UUID workspaceId,
            UUID batchId, String itemKey) {
        var batch = requireBatch(actor, workspaceId, batchId, "workflow:read");
        var item = jdbc.query("select item_key, ordinal, status, row_version, workflow_instance_id, error_code, request_text, scenario_key, experience_refs_json::text refs_json, "
                        + "(select p.child_task_id from workflow.parallel_branch p where p.instance_id = i.workflow_instance_id and p.role = 'knowledge') knowledge_task_id, "
                        + "(select p.child_task_id from workflow.parallel_branch p where p.instance_id = i.workflow_instance_id and p.role = 'experience') experience_task_id "
                        + "from workflow.batch_item i where batch_id = ? and item_key = ?",
                rs -> rs.next() ? new BatchItemRow(new ServiceRequestBatchSnapshot.Item(rs.getString("item_key"),
                        rs.getInt("ordinal"), rs.getString("status"), rs.getLong("row_version"),
                        rs.getObject("workflow_instance_id", UUID.class), rs.getString("error_code"),
                        rs.getObject("knowledge_task_id", UUID.class), rs.getObject("experience_task_id", UUID.class)),
                        rs.getString("request_text"), rs.getString("scenario_key"), rs.getString("refs_json")) : null,
                batchId, itemKey);
        if (item == null) throw EafException.notFound();
        return new ServiceRequestBatchSnapshot.Detail(item.item(), item.requestText(), item.scenarioKey(), parseRefs(item.refsJson()));
    }

    @Override
    @Transactional
    public ServiceRequestBatchSnapshot cancelServiceRequestBatch(ActorContext actor, UUID workspaceId,
            UUID batchId, long expectedVersion) {
        var row = requireBatch(actor, workspaceId, batchId, "workflow:write");
        if (Set.of("SUCCEEDED", "PARTIAL", "FAILED", "CANCELLED", "TIMED_OUT").contains(row.status()))
            return getServiceRequestBatch(actor, workspaceId, batchId);
        if (row.version() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "批次版本已变化。");
        jdbc.update("update workflow.batch set status = 'CANCELLING', cancel_requested = true, termination_target = 'CANCELLED', row_version = row_version + 1, updated_at = now() where id = ? and row_version = ?",
                batchId, expectedVersion);
        jdbc.update("update workflow.batch_item set status = 'CANCELLED', cancel_requested = true, row_version = row_version + 1, updated_at = now() where batch_id = ? and status = 'PENDING'", batchId);
        jdbc.update("update workflow.batch_item set status = 'CANCELLING', cancel_requested = true, row_version = row_version + 1, updated_at = now() where batch_id = ? and status = 'ACTIVE'", batchId);
        return getServiceRequestBatch(actor, workspaceId, batchId);
    }

    @Override
    @Transactional
    public ServiceRequestBatchSnapshot.Item cancelServiceRequestBatchItem(ActorContext actor, UUID workspaceId,
            UUID batchId, String itemKey, long expectedVersion) {
        requireBatch(actor, workspaceId, batchId, "workflow:write");
        var row = jdbc.query("select row_version, status, workflow_instance_id from workflow.batch_item where batch_id = ? and item_key = ? for update",
                rs -> rs.next() ? new Object[]{rs.getLong("row_version"), rs.getString("status"), rs.getObject("workflow_instance_id", UUID.class)} : null,
                batchId, itemKey);
        if (row == null) throw EafException.notFound();
        if (Set.of("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT").contains(row[1]))
            return itemSnapshot(batchId, itemKey);
        if ((Long) row[0] != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "批次项目版本已变化。");
        var next = row[2] == null ? "CANCELLED" : "CANCELLING";
        jdbc.update("update workflow.batch_item set status = ?, cancel_requested = true, row_version = row_version + 1, updated_at = now() where batch_id = ? and item_key = ?",
                next, batchId, itemKey);
        return itemSnapshot(batchId, itemKey);
    }

    @Override
    @Transactional
    public WorkflowInstance createOaTodoListQuery(ActorContext actor, UUID workspaceId, String status,
            String cursor, Integer requestedLimit, String idempotencyKey) {
        requireDirectHuman(actor);
        if (status != null && !Set.of("OPEN", "IN_PROGRESS", "DONE").contains(status)
                || cursor != null && cursor.length() > 512)
            throw EafException.invalid("OA 待办筛选字段无效。");
        var limit = requestedLimit == null ? 20 : requestedLimit;
        if (limit < 1 || limit > 50) throw EafException.invalid("OA 待办每页限制为 1 到 50。");
        // RUN_TOOL 映射不支持可空来源，因此固定字段用空串表示未筛选。
        var input = json.createObjectNode().put("limit", limit)
                .put("status", status == null ? "" : status).put("cursor", cursor == null ? "" : cursor);
        return createP27Query(actor, workspaceId, P27_OA_LIST_WORKFLOW_ID, input, idempotencyKey, "oa:todo:read");
    }

    @Override
    @Transactional
    public WorkflowInstance createOaTodoItemQuery(ActorContext actor, UUID workspaceId, String todoId,
            String idempotencyKey) {
        requireDirectHuman(actor);
        if (todoId == null || !todoId.matches("[A-Za-z0-9._:-]{1,160}"))
            throw EafException.invalid("OA todoId 无效。");
        return createP27Query(actor, workspaceId, P27_OA_ITEM_WORKFLOW_ID,
                json.createObjectNode().put("todoId", todoId), idempotencyKey, "oa:todo:read");
    }

    @Override
    @Transactional
    public WorkflowInstance createServiceRequestStateQuery(ActorContext actor, UUID workspaceId, UUID workItemId,
            String idempotencyKey) {
        requireDirectHuman(actor);
        workspaces.require(actor, workspaceId, "service-request:status:read");
        workspaces.require(actor, workspaceId, "work-item:read");
        if (workItemId == null) throw EafException.invalid("工作项 ID 必填。");
        var source = requireP27HandlingSource(actor, workspaceId, workItemId);
        var input = json.createObjectNode().put("workItemId", workItemId.toString());
        return createP27Query(actor, workspaceId, P27_SERVICE_STATE_WORKFLOW_ID, input, idempotencyKey,
                "service-request:status:read");
    }

    private WorkflowInstance createP27Query(ActorContext actor, UUID workspaceId, UUID workflowId,
            JsonNode input, String idempotencyKey, String permission) {
        if (workspaceId == null || idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200)
            throw EafException.invalid("P27 查询请求不完整。");
        workspaces.require(actor, workspaceId, permission);
        var request = new CreateWorkflowInstanceCommand(actor, workspaceId, workflowId, P27_WORKFLOW_VERSION,
                canonical(input), idempotencyKey, "USER");
        return createInstance(request, null, false, false, false, false, true);
    }

    @Override
    @Transactional
    public P27BusinessWorkflowService.ResultSyncSnapshot createResultSync(ActorContext actor, UUID workspaceId,
            UUID workItemId, long expectedWorkItemVersion, UUID stateQueryId, String idempotencyKey) {
        requireDirectHuman(actor);
        workspaces.require(actor, workspaceId, "service-request:result:sync");
        workspaces.require(actor, workspaceId, "work-item:read");
        if (workItemId == null || stateQueryId == null || expectedWorkItemVersion < 1
                || idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200)
            throw EafException.invalid("服务请求结果同步申请字段无效。");
        var source = requireP27HandlingSource(actor, workspaceId, workItemId);
        if (!actor.actorId().equals(source.completedBy()) || expectedWorkItemVersion != source.workItemVersion())
            throw EafException.conflict("SERVICE_REQUEST_RESULT_SOURCE_CHANGED", "只有完成人员可按当前工作项版本申请同步。");
        var stateQuery = loadInstance(actor.tenantId(), workspaceId, stateQueryId);
        if (!P27_SERVICE_STATE_WORKFLOW_ID.equals(stateQuery.workflowId()) || !actor.actorId().equals(stateQuery.actorId())
                || !"USER".equals(stateQuery.source()) || stateQuery.qualityRunId() != null
                || !"SUCCEEDED".equals(stateQuery.status()))
            throw EafException.forbidden("同步申请必须引用本人成功的固定服务台状态查询。");
        var queryInput = parseJson(stateQuery.inputJson(), "p27.stateQueryInput");
        if (!workItemId.toString().equals(queryInput.path("workItemId").asText()))
            throw EafException.forbidden("服务台状态查询与工作项不匹配。");
        var stateResult = parseJson(stateQuery.resultJson(), "p27.stateQueryResult").path("state");
        var externalVersion = stateResult.path("externalVersion").asText(null);
        if (!"P15_INTERNAL_SERVICE_DESK_FIXTURE".equals(stateResult.path("sourceId").asText())
                || !source.requestId().equals(stateResult.path("requestId").asText())
                || !source.registrationOperationId().equals(stateResult.path("registrationOperationId").asText())
                || externalVersion == null || externalVersion.isBlank() || externalVersion.length() > 120
                || "RESOLVED".equals(stateResult.path("status").asText()))
            throw EafException.conflict("SERVICE_REQUEST_STATE_STALE", "服务台状态查询不能用于当前结果同步。");
        var mapping = p27Connections.requireEmployeeMapping(actor, workspaceId, P27_SERVICE_BINDING);
        var queryBindingVersion = parseJson(stateQuery.resultJson(), "p27.stateQueryResult")
                .path("bindingVersion").asText(null);
        if (!mapping.bindingVersion().equals(queryBindingVersion))
            throw EafException.conflict("CONNECTOR_BINDING_CHANGED", "状态查询后员工映射或连接绑定已变化，请重新查询。");

        var keyHash = Hashing.sha256(String.join("\u001f", actor.tenantId().toString(), workspaceId.toString(),
                actor.actorId().toString(), idempotencyKey));
        var requestHash = Hashing.sha256(String.join("\u001f", workItemId.toString(),
                Long.toString(source.workItemVersion()), source.sourceResultHash(), stateQueryId.toString(),
                externalVersion, mapping.bindingVersion()));
        var replay = jdbc.query("select sync_id, request_hash from workflow.p27_result_sync where tenant_id = ? and workspace_id = ? and actor_id = ? and idempotency_key_hash = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("sync_id", UUID.class), rs.getString("request_hash")} : null,
                actor.tenantId(), workspaceId, actor.actorId(), keyHash);
        if (replay != null) {
            if (!requestHash.equals(replay[1])) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一同步幂等键对应了不同来源或状态。");
            return resultSyncSnapshot(actor, workspaceId, (UUID) replay[0]);
        }
        jdbc.update("insert into workflow.p27_request_lock(tenant_id, workspace_id, request_id) values (?, ?, ?) on conflict do nothing",
                actor.tenantId(), workspaceId, source.requestId());
        jdbc.query("select request_id from workflow.p27_request_lock where tenant_id = ? and workspace_id = ? and request_id = ? for update",
                rs -> rs.next() ? rs.getString(1) : null, actor.tenantId(), workspaceId, source.requestId());
        var active = jdbc.query("select sync_id from workflow.p27_result_sync where tenant_id = ? and workspace_id = ? "
                        + "and request_id = ? and status in ('PENDING','UNKNOWN','SUCCEEDED') order by created_at desc limit 1",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, actor.tenantId(), workspaceId, source.requestId());
        if (active != null) {
            reconcileP27Sync(actor, workspaceId, active);
            var refreshed = p27SyncRow(actor.tenantId(), workspaceId, active);
            if (Set.of("PENDING", "UNKNOWN", "SUCCEEDED").contains(refreshed.status())) {
                if (source.sourceResultHash().equals(refreshed.sourceResultHash())
                        && externalVersion.equals(refreshed.expectedExternalVersion()))
                    return resultSyncSnapshot(actor, workspaceId, active);
                throw EafException.conflict("SERVICE_REQUEST_SYNC_UNRESOLVED", "同一外部請求已有未澄清或已完成的同步记录。");
            }
        }

        var syncId = UUID.randomUUID();
        var workflowInput = json.createObjectNode().put("syncId", syncId.toString());
        var workflow = createInstance(new CreateWorkflowInstanceCommand(actor, workspaceId,
                P27_RESULT_SYNC_WORKFLOW_ID, P27_WORKFLOW_VERSION, canonical(workflowInput), syncId.toString(), "USER"),
                null, false, false, false, false, true);
        jdbc.update("insert into workflow.p27_result_sync(sync_id, tenant_id, workspace_id, actor_id, work_item_id, "
                        + "work_item_version, source_result_hash, request_id, registration_operation_id, state_query_id, "
                        + "expected_external_version, binding_version, external_subject_id, completed_by, completed_at, "
                        + "outcome, summary, next_action, workflow_instance_id, idempotency_key_hash, request_hash, status) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')",
                syncId, actor.tenantId(), workspaceId, actor.actorId(), workItemId, source.workItemVersion(),
                source.sourceResultHash(), source.requestId(), source.registrationOperationId(), stateQueryId,
                externalVersion, mapping.bindingVersion(), mapping.externalSubjectId(), source.completedBy(),
                Timestamp.from(source.completedAt()), source.outcome(), source.summary(), source.nextAction(),
                workflow.id(), keyHash, requestHash);
        audit.append(new AuditFact("p27-result-sync-created:" + syncId, actor.tenantId(), workspaceId,
                actor.actorId(), syncId, "SERVICE_REQUEST_RESULT_SYNC_CREATED", "PENDING", "{}", null));
        return resultSyncSnapshot(actor, workspaceId, syncId);
    }

    @Override
    public P27BusinessWorkflowService.QuerySnapshot getQuery(ActorContext actor, UUID workspaceId, UUID queryId) {
        requireDirectHuman(actor);
        var instance = loadInstance(actor.tenantId(), workspaceId, queryId);
        if (!actor.actorId().equals(instance.actorId()) || !"USER".equals(instance.source())
                || instance.qualityRunId() != null || !p27QueryWorkflowId(instance.workflowId())) throw EafException.notFound();
        var action = P27_SERVICE_STATE_WORKFLOW_ID.equals(instance.workflowId())
                ? "service-request:status:read" : "oa:todo:read";
        workspaces.require(actor, workspaceId, action);
        var query = jdbc.query("select row_version, updated_at, (select child_task_id from workflow.step where instance_id = ? and step_id = 'read') task_id from workflow.instance where id = ?",
                rs -> rs.next() ? new P27QueryRow(rs.getLong("row_version"), rs.getTimestamp("updated_at").toInstant(),
                        rs.getObject("task_id", UUID.class)) : null, queryId, queryId);
        JsonNode result = null;
        if ("SUCCEEDED".equals(instance.status()) && query.taskId() != null) {
            var taskResult = tasks.evidence(actor.tenantId(), workspaceId, query.taskId()).snapshot().resultJson();
            if (taskResult != null) result = parseJson(taskResult, "p27.queryTaskResult");
        }
        if (result == null && instance.resultJson() != null) result = parseJson(instance.resultJson(), "p27.queryResult");
        if (result != null) {
            var bindingVersion = result.path("bindingVersion").asText(null);
            if (bindingVersion == null) throw EafException.notFound();
            var input = parseJson(instance.inputJson(), "p27.queryInput");
            if (P27_OA_LIST_WORKFLOW_ID.equals(instance.workflowId())) {
                if (!p27Connections.canReadTodo(actor, workspaceId, P27_OA_BINDING, bindingVersion, null,
                        Instant.now().plusSeconds(10))) throw EafException.notFound();
                var items = result.path("page").path("items");
                if (!items.isArray()) throw EafException.notFound();
                for (var todo : items) {
                    var todoId = todo.path("todoId").asText(null);
                    if (todoId == null || !p27Connections.canReadTodo(actor, workspaceId, P27_OA_BINDING,
                            bindingVersion, todoId, Instant.now().plusSeconds(10))) throw EafException.notFound();
                }
            } else if (P27_OA_ITEM_WORKFLOW_ID.equals(instance.workflowId())) {
                var todoId = input.path("todoId").asText(null);
                if (todoId == null || !p27Connections.canReadTodo(actor, workspaceId, P27_OA_BINDING, bindingVersion,
                        todoId, Instant.now().plusSeconds(10))) throw EafException.notFound();
            } else {
                var state = result.path("state");
                if (!p27Connections.canReadServiceRequest(actor, workspaceId, P27_SERVICE_BINDING, bindingVersion,
                        state.path("requestId").asText(null), Instant.now().plusSeconds(10))) throw EafException.notFound();
            }
        }
        return new P27BusinessWorkflowService.QuerySnapshot(queryId, instance.status(), query.taskId(),
                result, instance.errorCode(), query.rowVersion(), query.updatedAt());
    }

    @Override
    public P27BusinessWorkflowService.QuerySnapshot getServiceRequestStateQuery(ActorContext actor, UUID workspaceId,
            UUID workItemId, UUID queryId) {
        requireDirectHuman(actor);
        var instance = loadInstance(actor.tenantId(), workspaceId, queryId);
        if (!P27_SERVICE_STATE_WORKFLOW_ID.equals(instance.workflowId())) throw EafException.notFound();
        var input = parseJson(instance.inputJson(), "p27.stateQueryInput");
        if (!workItemId.toString().equals(input.path("workItemId").asText())) throw EafException.notFound();
        return getQuery(actor, workspaceId, queryId);
    }

    @Override
    public P27BusinessWorkflowService.ResultSyncSnapshot getResultSync(ActorContext actor, UUID workspaceId,
            UUID workItemId, UUID syncId) {
        requireDirectHuman(actor);
        workspaces.require(actor, workspaceId, "work-item:read");
        var item = getHumanWorkItem(actor, workspaceId, workItemId);
        var row = p27SyncRow(actor.tenantId(), workspaceId, syncId);
        if (row == null || !workItemId.equals(row.workItemId())
                || !actor.actorId().equals(row.actorId()) && !actor.actorId().equals(item.creatorId()))
            throw EafException.notFound();
        if (actor.actorId().equals(row.actorId())) reconcileP27Sync(actor, workspaceId, syncId);
        return resultSyncSnapshot(actor, workspaceId, syncId);
    }

    @Override
    @Transactional
    public P27BusinessWorkflowService.ResultSyncPage listResultSyncs(ActorContext actor, UUID workspaceId,
            UUID workItemId, Instant cursorCreatedAt, UUID cursorSyncId, int pageSize) {
        requireDirectHuman(actor);
        workspaces.require(actor, workspaceId, "work-item:read");
        if (pageSize < 1 || pageSize > 50 || (cursorCreatedAt == null) != (cursorSyncId == null))
            throw EafException.invalid("同步记录分页参数无效。");
        var item = getHumanWorkItem(actor, workspaceId, workItemId);
        var rows = jdbc.query("select sync_id, created_at from workflow.p27_result_sync where tenant_id = ? and workspace_id = ? "
                        + "and work_item_id = ? and (actor_id = ? or ? = ?) "
                        + "and (?::timestamptz is null or (created_at, sync_id) < (?, ?)) "
                        + "order by created_at desc, sync_id desc limit ?",
                (rs, n) -> new P27SyncCursor(rs.getObject("sync_id", UUID.class), rs.getTimestamp("created_at").toInstant()),
                actor.tenantId(), workspaceId, workItemId, actor.actorId(), actor.actorId(), item.creatorId(),
                cursorCreatedAt == null ? null : Timestamp.from(cursorCreatedAt),
                cursorCreatedAt == null ? null : Timestamp.from(cursorCreatedAt), cursorSyncId, pageSize + 1);
        var hasNext = rows.size() > pageSize;
        var page = hasNext ? rows.subList(0, pageSize) : rows;
        var snapshots = page.stream().map(row -> getResultSync(actor, workspaceId, workItemId, row.id())).toList();
        var last = hasNext ? page.get(page.size() - 1) : null;
        return new P27BusinessWorkflowService.ResultSyncPage(snapshots,
                last == null ? null : last.createdAt(), last == null ? null : last.id());
    }

    @Override
    public P27BusinessWorkflowService.ResultSyncSnapshot verifyResultSync(ActorContext actor, UUID workspaceId,
            UUID workItemId, UUID syncId, String requestKey, String reason) {
        requireDirectHuman(actor);
        workspaces.require(actor, workspaceId, "service-request:result:sync");
        var row = p27SyncRow(actor.tenantId(), workspaceId, syncId);
        if (row == null || !workItemId.equals(row.workItemId()) || !actor.actorId().equals(row.actorId()))
            throw EafException.notFound();
        var snapshot = resultSyncSnapshot(actor, workspaceId, syncId);
        if (snapshot.taskId() != null) executions.findP27ResultSyncForTask(actor, workspaceId, snapshot.taskId()).ifPresent(execution ->
                executions.verifyOperational(actor, workspaceId, execution.id(), requestKey, reason));
        reconcileP27Sync(actor, workspaceId, syncId);
        return resultSyncSnapshot(actor, workspaceId, syncId);
    }

    private P27BusinessWorkflowService.ResultSyncSnapshot resultSyncSnapshot(ActorContext actor, UUID workspaceId, UUID syncId) {
        var row = p27SyncRow(actor.tenantId(), workspaceId, syncId);
        if (row == null) throw EafException.notFound();
        var instance = loadInstance(actor.tenantId(), workspaceId, row.workflowInstanceId());
        var taskId = jdbc.query("select child_task_id from workflow.step where instance_id = ? and step_id = 'record'",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, instance.id());
        var taskStatus = (String) null;
        var executionId = (UUID) null;
        var approvalId = (UUID) null;
        var executionStatus = (String) null;
        var allowedActions = new ArrayList<String>();
        if (taskId != null && actor.actorId().equals(row.actorId())) {
            var task = tasks.evidence(actor.tenantId(), workspaceId, taskId).snapshot();
            taskStatus = task.status().name();
            var execution = executions.findP27ResultSyncForTask(actor, workspaceId, taskId).orElse(null);
            if (execution != null) {
                executionId = execution.id(); approvalId = execution.approvalId(); executionStatus = execution.status();
                if (Set.of("UNKNOWN", "VERIFICATION_FAILED").contains(executionStatus)) allowedActions.add("verify");
            }
        }
        var result = actor.actorId().equals(row.actorId()) && taskId != null
                ? tasks.evidence(actor.tenantId(), workspaceId, taskId).snapshot().resultJson() : null;
        if (result == null && actor.actorId().equals(row.actorId()) && instance.resultJson() != null)
            result = instance.resultJson();
        var resultNode = result == null ? null : parseJson(result, "p27.syncResult");
        var stateQuery = loadInstance(actor.tenantId(), workspaceId, row.stateQueryId());
        var externalStatus = stateQuery.resultJson() == null ? null
                : parseJson(stateQuery.resultJson(), "p27.syncStateQuery").path("state").path("status").asText(null);
        if (resultNode != null && resultNode.path("resultingStatus").isTextual())
            externalStatus = resultNode.path("resultingStatus").asText();
        return new P27BusinessWorkflowService.ResultSyncSnapshot(syncId, row.workItemId(), row.sourceResultHash(),
                instance.id(), instance.status(), taskId, taskStatus, executionId, approvalId, executionStatus,
                row.status(), externalStatus, instance.errorCode(), resultNode, allowedActions, instance.rowVersion());
    }

    private void reconcileP27Sync(ActorContext actor, UUID workspaceId, UUID syncId) {
        var row = p27SyncRow(actor.tenantId(), workspaceId, syncId);
        if (row == null || "FAILED_SAFE".equals(row.status()) || "SUCCEEDED".equals(row.status())) return;
        var instance = loadInstance(actor.tenantId(), workspaceId, row.workflowInstanceId());
        if ("SUCCEEDED".equals(instance.status())) {
            jdbc.update("update workflow.p27_result_sync set status = 'SUCCEEDED', updated_at = now() where sync_id = ? and status <> 'SUCCEEDED'", syncId);
            return;
        }
        var taskId = jdbc.query("select child_task_id from workflow.step where instance_id = ? and step_id = 'record'",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, instance.id());
        if (taskId != null) {
            var task = tasks.evidence(actor.tenantId(), workspaceId, taskId).snapshot();
            if ("WAITING_VERIFICATION".equals(task.status().name())
                    || Set.of("UNKNOWN", "REMOTE_CANCEL_UNKNOWN").contains(task.externalEffectStatus()))
                jdbc.update("update workflow.p27_result_sync set status = 'UNKNOWN', updated_at = now() where sync_id = ? and status = 'PENDING'", syncId);
        }
        if (Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(instance.status()) && taskId == null) {
            jdbc.update("update workflow.p27_result_sync set status = 'FAILED_SAFE', updated_at = now() where sync_id = ? and status in ('PENDING','UNKNOWN')", syncId);
        } else if (Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(instance.status())) {
            var task = tasks.evidence(actor.tenantId(), workspaceId, taskId).snapshot();
            if (Set.of("NONE", "FAILED", "CANCELLED", "DENIED").contains(task.externalEffectStatus()))
                jdbc.update("update workflow.p27_result_sync set status = 'FAILED_SAFE', updated_at = now() where sync_id = ? and status in ('PENDING','UNKNOWN')", syncId);
            else jdbc.update("update workflow.p27_result_sync set status = 'UNKNOWN', updated_at = now() where sync_id = ? and status = 'PENDING'", syncId);
        }
    }

    @Override
    public P27BusinessTaskSource requireToolSource(ActorContext actor, UUID workspaceId, UUID taskId, int attempt,
            String toolName, String toolVersion, String toolArgumentsJson) {
        requireDirectHuman(actor);
        if (taskId == null || attempt < 1 || !"1.0.0".equals(toolVersion))
            throw EafException.forbidden("P27 Tool Task 来源无效。");
        var taskSource = tasks.findWorkflowExecutionSource(actor, workspaceId, taskId, attempt)
                .orElseThrow(() -> EafException.forbidden("P27 Tool Task 来源不存在。"));
        var provenance = taskSource.provenance();
        if (provenance == null || !provenance.complete())
            throw EafException.forbidden("P27 Tool Task Workflow 绑定不完整。");
        var row = jdbc.query("select i.id, i.tenant_id, i.actor_id, i.source, i.quality_run_id, i.status instance_status, "
                        + "i.current_step_id, i.workflow_id, i.workflow_version, s.step_id, s.status step_status, "
                        + "s.child_task_id, s.input_json::text step_input from workflow.instance i "
                        + "join workflow.step s on s.instance_id = i.id where i.id = ? and i.tenant_id = ? "
                        + "and i.workspace_id = ? and i.actor_id = ? and i.workflow_id = ? and i.workflow_version = ? "
                        + "and s.step_id = ?",
                rs -> rs.next() ? new P27ToolTask(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("actor_id", UUID.class), rs.getString("source"), rs.getObject("quality_run_id", UUID.class),
                        rs.getString("instance_status"), rs.getString("current_step_id"), rs.getObject("workflow_id", UUID.class),
                        rs.getString("workflow_version"), rs.getString("step_id"), rs.getString("step_status"),
                        rs.getObject("child_task_id", UUID.class), rs.getString("step_input"), taskSource.status().name()) : null,
                provenance.workflowInstanceId(), actor.tenantId(), workspaceId, actor.actorId(),
                provenance.workflowId(), provenance.workflowVersion(), provenance.stepId());
        var currentTaskSource = tasks.findWorkflowExecutionSource(actor, workspaceId, taskId, attempt).orElse(null);
        if (row == null || !taskSource.equals(currentTaskSource))
            throw EafException.forbidden("P27 Tool Task 来源在读取期间已变化。");
        if (row == null) throw EafException.forbidden("P27 Tool Task 来源不存在。");
        boolean activeStep = "WAITING_CHILD".equals(row.instanceStatus())
                && "WAITING_CHILD".equals(row.stepStatus()) && row.stepId().equals(row.currentStepId());
        boolean completedStep = "SUCCEEDED".equals(row.instanceStatus()) && "SUCCEEDED".equals(row.stepStatus());
        boolean terminalFailureStep = Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(row.instanceStatus())
                && Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(row.stepStatus());
        // Workflow 失败时可能保留 WAITING_CHILD；只允许读取精确绑定的终态子 Task 证据。
        boolean failedChild = Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(row.instanceStatus())
                && "WAITING_CHILD".equals(row.stepStatus())
                && Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(row.taskStatus());
        if (!"USER".equals(row.source()) || row.qualityRunId() != null
                || !row.actorId().equals(actor.actorId()) || !P27_WORKFLOW_VERSION.equals(row.workflowVersion())
                || !(activeStep || completedStep || terminalFailureStep || failedChild) || !row.workflowId().equals(p27WorkflowForTool(toolName))
                || !row.stepId().equals(p27StepForTool(toolName)) || !taskId.equals(row.childTaskId())
                || !sameJson(toolArgumentsJson, row.stepInput()))
            throw EafException.forbidden("P27 Tool 只能由固定 Workflow 的对应步骤调用。");
        if ("oa.todo.list".equals(toolName)) return new P27BusinessTaskSource("OA_LIST", P27_OA_BINDING,
                null, null, 0, null, null, null, null, null, null, null, null, null, null);
        if ("oa.todo.get".equals(toolName)) return new P27BusinessTaskSource("OA_ITEM", P27_OA_BINDING,
                null, null, 0, null, null, null, null, null, null, null, null, null, null);
        var args = parseJson(toolArgumentsJson, "p27.toolArguments");
        if ("service.request.status.get".equals(toolName)) {
            var workItemId = uuidOrNull(args.path("workItemId").asText(null));
            if (workItemId == null) throw EafException.forbidden("P27 状态查询未绑定工作项来源。");
            var source = requireP27HandlingSource(actor, workspaceId, workItemId);
            return p27TaskSource("SERVICE_STATE", source, null, null);
        }
        if (!"service.request.result.record".equals(toolName))
            throw EafException.forbidden("P27 Tool 不在固定来源校验清单中。");
        var syncId = uuidOrNull(args.path("syncId").asText(null));
        var sync = syncId == null ? null : p27SyncRow(actor.tenantId(), workspaceId, syncId);
        if (sync == null || !row.id().equals(sync.workflowInstanceId()) || !actor.actorId().equals(sync.actorId())
                || !Set.of("PENDING", "UNKNOWN", "SUCCEEDED", "FAILED_SAFE").contains(sync.status()))
            throw EafException.forbidden("P27 结果写回缺少有效的固定同步来源。");
        var currentSource = requireP27HandlingSource(actor, workspaceId, sync.workItemId());
        if (!actor.actorId().equals(currentSource.completedBy())
                || !currentSource.sourceResultHash().equals(sync.sourceResultHash())
                || currentSource.workItemVersion() != sync.workItemVersion())
            throw EafException.conflict("SERVICE_REQUEST_RESULT_SOURCE_CHANGED", "人工结果来源已变化，拒绝复用旧审批。");
        return new P27BusinessTaskSource("RESULT_SYNC", P27_SERVICE_BINDING, sync.bindingVersion(), sync.workItemId(),
                sync.workItemVersion(), sync.requestId(), sync.registrationOperationId(), sync.sourceResultHash(),
                sync.completedBy(), sync.completedAt(), sync.outcome(), sync.summary(), sync.nextAction(),
                sync.externalSubjectId(), sync.expectedExternalVersion());
    }

    private P27BusinessTaskSource p27TaskSource(String kind, P27HandlingSource source,
            String bindingVersion, String expectedExternalVersion) {
        return new P27BusinessTaskSource(kind, P27_SERVICE_BINDING, bindingVersion, source.workItemId(),
                source.workItemVersion(), source.requestId(), source.registrationOperationId(), source.sourceResultHash(),
                source.completedBy(), source.completedAt(), source.outcome(), source.summary(), source.nextAction(),
                null, expectedExternalVersion);
    }

    private P27HandlingSource requireP27HandlingSource(ActorContext actor, UUID workspaceId, UUID workItemId) {
        var item = getHumanWorkItem(actor, workspaceId, workItemId);
        var instance = loadInstance(actor.tenantId(), workspaceId, item.instanceId());
        if (!P16_SERVICE_REQUEST_WORKFLOW_ID.equals(instance.workflowId())
                || !Set.of(P16_SERVICE_REQUEST_WORKFLOW_VERSION, P17_SERVICE_REQUEST_WORKFLOW_VERSION,
                        P18_SERVICE_REQUEST_WORKFLOW_VERSION).contains(instance.workflowVersion())
                || !"USER".equals(instance.source()) || instance.qualityRunId() != null
                || !"handle".equals(item.stepId()) || !"COMPLETED".equals(item.status())
                || item.outcome() == null || item.summary() == null || item.completedBy() == null || item.completedAt() == null)
            throw EafException.conflict("SERVICE_REQUEST_RESULT_SOURCE_INVALID", "只接受固定 P16 工作流中已保存的人工完成结果。");
        var input = parseJson(instance.inputJson(), "p27.p16Input");
        var registrationId = uuidOrNull(input.path("registrationWorkflowId").asText(null));
        var registration = registrationId == null ? null : loadInstance(actor.tenantId(), workspaceId, registrationId);
        var requestId = input.path("requestId").asText(null);
        var registrationOperationId = input.path("operationId").asText(null);
        var registered = registration == null ? null : parseJson(registration.resultJson(), "p27.registrationResult");
        if (registration == null || !P15_SERVICE_REQUEST_WORKFLOW_ID.equals(registration.workflowId())
                || !P15_SERVICE_REQUEST_WORKFLOW_VERSION.equals(registration.workflowVersion())
                || !"USER".equals(registration.source()) || !"SUCCEEDED".equals(registration.status())
                || !registrationOperationId.equals(registered.path("operationId").asText())
                || !requestId.equals(registered.path("requestId").asText())
                || !"REGISTERED".equals(registered.path("status").asText()))
            throw EafException.conflict("SERVICE_REQUEST_REGISTRATION_UNVERIFIED", "P16 来源缺少仍可核验的 P15 登记回执。");
        var stable = json.createObjectNode().put("workItemId", item.id().toString())
                .put("instanceId", instance.id().toString()).put("workItemVersion", item.rowVersion())
                .put("requestId", requestId).put("registrationOperationId", registrationOperationId)
                .put("outcome", item.outcome()).put("summary", item.summary())
                .put("nextAction", item.nextAction() == null ? "" : item.nextAction())
                .put("completedBy", item.completedBy().toString()).put("completedAt", item.completedAt().toString());
        return new P27HandlingSource(item.id(), item.rowVersion(), requestId, registrationOperationId,
                Hashing.sha256(canonical(stable)), item.completedBy(), item.completedAt(), item.outcome(),
                item.summary(), item.nextAction());
    }

    private boolean p27WorkflowId(UUID id) {
        return P27_OA_LIST_WORKFLOW_ID.equals(id) || P27_OA_ITEM_WORKFLOW_ID.equals(id)
                || P27_SERVICE_STATE_WORKFLOW_ID.equals(id) || P27_RESULT_SYNC_WORKFLOW_ID.equals(id);
    }

    private boolean p27QueryWorkflowId(UUID id) {
        return P27_OA_LIST_WORKFLOW_ID.equals(id) || P27_OA_ITEM_WORKFLOW_ID.equals(id)
                || P27_SERVICE_STATE_WORKFLOW_ID.equals(id);
    }

    private boolean sameJson(String left, String right) {
        if (left == null || right == null) return false;
        return canonical(parseJson(left, "p27.arguments")).equals(canonical(parseJson(right, "p27.workflowInput")));
    }

    private UUID p27WorkflowForTool(String toolName) {
        return switch (toolName) {
            case "oa.todo.list" -> P27_OA_LIST_WORKFLOW_ID;
            case "oa.todo.get" -> P27_OA_ITEM_WORKFLOW_ID;
            case "service.request.status.get" -> P27_SERVICE_STATE_WORKFLOW_ID;
            case "service.request.result.record" -> P27_RESULT_SYNC_WORKFLOW_ID;
            default -> null;
        };
    }

    private String p27StepForTool(String toolName) { return "read".equals(toolName) ? "read" :
            "service.request.result.record".equals(toolName) ? "record" : "read"; }

    private P27ResultSyncRow p27SyncRow(UUID tenantId, UUID workspaceId, UUID syncId) {
        return jdbc.query("select sync_id, actor_id, work_item_id, work_item_version, source_result_hash, request_id, "
                        + "registration_operation_id, state_query_id, expected_external_version, binding_version, "
                        + "external_subject_id, completed_by, completed_at, outcome, summary, next_action, "
                        + "workflow_instance_id, status, idempotency_key_hash, request_hash from workflow.p27_result_sync "
                        + "where tenant_id = ? and workspace_id = ? and sync_id = ?",
                rs -> rs.next() ? new P27ResultSyncRow(rs.getObject("sync_id", UUID.class),
                        rs.getObject("actor_id", UUID.class), rs.getObject("work_item_id", UUID.class),
                        rs.getLong("work_item_version"), rs.getString("source_result_hash"), rs.getString("request_id"),
                        rs.getString("registration_operation_id"), rs.getObject("state_query_id", UUID.class),
                        rs.getString("expected_external_version"), rs.getString("binding_version"),
                        rs.getString("external_subject_id"), rs.getObject("completed_by", UUID.class),
                        rs.getTimestamp("completed_at").toInstant(), rs.getString("outcome"), rs.getString("summary"),
                        rs.getString("next_action"), rs.getObject("workflow_instance_id", UUID.class), rs.getString("status"),
                        rs.getString("idempotency_key_hash"), rs.getString("request_hash")) : null,
                tenantId, workspaceId, syncId);
    }

    @Override
    @Transactional
    public WorkflowInstance createCustomerFollowupResultWorkflow(ActorContext actor, UUID workspaceId,
            UUID followupId, UUID resultId, String idempotencyKey) {
        if (actor == null || workspaceId == null || followupId == null || resultId == null
                || idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200)
            throw EafException.invalid("结果同步 Workflow 请求不完整。");
        var input = json.createObjectNode().put("followupId", followupId.toString())
                .put("resultId", resultId.toString());
        var request = new CreateWorkflowInstanceCommand(actor, workspaceId, P12_RESULT_WORKFLOW_ID,
                P12_RESULT_WORKFLOW_VERSION, canonical(input), idempotencyKey, "USER");
        return createInstance(request, null, true, false, false, false);
    }

    @Override
    @Transactional
    public WorkflowInstance createServiceRequestRegistrationWorkflow(ActorContext actor, UUID workspaceId,
            UUID submissionId, String idempotencyKey) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated()
                || workspaceId == null || submissionId == null || idempotencyKey == null
                || idempotencyKey.isBlank() || idempotencyKey.length() > 200)
            throw EafException.invalid("服务请求登记 Workflow 请求不完整。");
        workspaces.require(actor, workspaceId, "service-request:submit");
        tasks.requireServiceRequestSubmission(actor, workspaceId, submissionId);
        var input = json.createObjectNode().put("submissionId", submissionId.toString());
        var request = new CreateWorkflowInstanceCommand(actor, workspaceId, P15_SERVICE_REQUEST_WORKFLOW_ID,
                P15_SERVICE_REQUEST_WORKFLOW_VERSION, canonical(input), idempotencyKey, "USER");
        return createInstance(request, null, false, true, false, false);
    }

    @Override
    @Transactional
    public ServiceRequestHandlingSnapshot createServiceRequestHandlingWorkflow(ActorContext actor, UUID workspaceId,
            UUID submissionId, UUID assigneeId, String sharedBrief, String idempotencyKey, Instant requestedDeadline) {
        return createServiceRequestHandlingWorkflow(actor, workspaceId, submissionId, assigneeId, sharedBrief,
                idempotencyKey, requestedDeadline, null, List.of());
    }

    @Override
    @Transactional
    public ServiceRequestHandlingSnapshot createServiceRequestHandlingWorkflow(ActorContext actor, UUID workspaceId,
            UUID submissionId, UUID assigneeId, String sharedBrief, String idempotencyKey, Instant requestedDeadline,
            String scenarioKey, List<TeamExperienceRef> requestedRefs) {
        requireDirectHuman(actor);
        if (workspaceId == null || submissionId == null || assigneeId == null || idempotencyKey == null
                || idempotencyKey.isBlank() || idempotencyKey.length() > 128)
            throw EafException.invalid("处理创建字段缺失或无效。");
        var brief = requireText(sharedBrief, 2_000, "sharedBrief");
        if (brief.length() < 1) throw EafException.invalid("sharedBrief 不能为空。");
        var rawRefs = requestedRefs == null ? List.<TeamExperienceRef>of() : new ArrayList<>(requestedRefs);
        if (rawRefs.stream().anyMatch(ref -> ref == null || ref.cardId() == null || ref.revision() < 1))
            throw EafException.invalid("团队经验引用字段无效。");
        var refs = rawRefs.stream()
                .sorted(Comparator.comparing(ref -> ref.cardId() + "@" + ref.revision())).toList();
        if (refs.isEmpty() != (scenarioKey == null) || refs.size() > 3
                || refs.stream().map(TeamExperienceRef::cardId).distinct().count() != refs.size()
                || scenarioKey != null && !scenarioKey.matches("[a-z][a-z0-9-]{0,63}"))
            throw EafException.invalid("场景与团队经验引用必须同时提供且符合数量、ID 和版本约束。");
        // 有经验选择的新流程采用；无选择继续使用原版本。
        var workflowVersion = refs.isEmpty() ? P16_SERVICE_REQUEST_WORKFLOW_VERSION : P18_SERVICE_REQUEST_WORKFLOW_VERSION;
        var access = workspaces.require(actor, workspaceId, "workflow:start");
        workspaces.require(actor, workspaceId, "work-item:read");
        workspaces.require(actor, workspaceId, "work-item:assign");
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(), workspaceId.toString(),
                actor.actorId().toString(), idempotencyKey));
        var replay = existingHandling(access.tenantId(), workspaceId, actor.actorId(), keyHash);
        if (replay != null) {
            requireHandlingReplayMatches(replay, submissionId, assigneeId, brief, requestedDeadline, scenarioKey, refs);
            return handlingSnapshot(replay, handlingItem(access.tenantId(), workspaceId, replay.id(), actor));
        }

        var deadline = requestedDeadline == null
                ? jdbc.queryForObject("select now() + interval '24 hours'", Timestamp.class).toInstant()
                : requestedDeadline.truncatedTo(ChronoUnit.MICROS);
        if (!Boolean.TRUE.equals(jdbc.queryForObject("select ?::timestamptz > now() and ?::timestamptz <= now() + interval '7 days'",
                Boolean.class, Timestamp.from(deadline), Timestamp.from(deadline))))
            throw EafException.invalid("截止时间必须晚于当前时间且不超过 7 天。");
        requireEligibleAssignee(access.tenantId(), workspaceId, assigneeId);
        if (!refs.isEmpty()) {
            workspaces.require(actor, workspaceId, "memory:read");
            if (!workspaces.isAuthorized(access.tenantId(), assigneeId, workspaceId, "memory:read"))
                throw EafException.forbidden("初始处理人没有读取同 Workspace 团队经验的权限。");
            contexts.resolveTeamExperiences(actor, workspaceId, scenarioKey, refs.stream()
                    .map(ref -> new ExperienceRef(ref.cardId(), ref.revision())).toList());
        }
        var source = tasks.requireServiceRequestSubmission(actor, workspaceId, submissionId);
        var registration = verifiedServiceRequestRegistration(actor, workspaceId, submissionId, source);
        var input = json.createObjectNode().put("submissionId", submissionId.toString())
                .put("registrationWorkflowId", registration.workflow().id().toString())
                .put("registrationExecutionId", registration.execution().id().toString())
                .put("operationId", registration.execution().operationId().toString())
                .put("requestId", registration.requestId()).put("sharedBrief", brief)
                .put("assigneeId", assigneeId.toString());
        if (!refs.isEmpty()) {
            input.put("scenarioKey", scenarioKey);
            input.put("teamExperienceRefsJson", canonical(json.valueToTree(refs)));
        }
        var request = new CreateWorkflowInstanceCommand(actor, workspaceId, P16_SERVICE_REQUEST_WORKFLOW_ID,
                workflowVersion, canonical(input), idempotencyKey, "USER", deadline);
        final WorkflowInstance created;
        try {
            created = createInstance(request, null, false, false, true, false);
        } catch (EafException conflict) {
            if (!"IDEMPOTENCY_CONFLICT".equals(conflict.code())) throw conflict;
            var raced = existingHandling(access.tenantId(), workspaceId, actor.actorId(), keyHash);
            if (raced == null) throw conflict;
            requireHandlingReplayMatches(raced, submissionId, assigneeId, brief, requestedDeadline, scenarioKey, refs);
            return handlingSnapshot(raced, handlingItem(access.tenantId(), workspaceId, raced.id(), actor));
        }
        audit.append(new AuditFact("p16-handling-created:" + created.id(), access.tenantId(), workspaceId,
                actor.actorId(), created.id(), "SERVICE_REQUEST_HANDLING_CREATED", "QUEUED", "{}", null));
        return handlingSnapshot(created, null);
    }

    /** 批次只在本地事务内锁住一个批次并推进一项；两个节点由 PostgreSQL 行锁互斥。 */
    @Transactional
    public boolean dispatchBatchOne() {
        var batch = jdbc.query("select id, tenant_id, workspace_id, initiator_id, status, termination_target, cancel_requested, max_active_items, item_count, deadline_at "
                        + "from workflow.batch where status in ('PENDING','RUNNING','CANCELLING') and next_poll_at <= now() "
                        + "order by next_poll_at, created_at, id for update skip locked limit 1",
                rs -> rs.next() ? new BatchDispatchRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("initiator_id", UUID.class),
                        rs.getString("status"), rs.getString("termination_target"), rs.getBoolean("cancel_requested"),
                        rs.getInt("max_active_items"), rs.getInt("item_count"), rs.getTimestamp("deadline_at").toInstant()) : null);
        if (batch == null) return false;

        var expired = Boolean.TRUE.equals(jdbc.queryForObject("select ?::timestamptz <= now()", Boolean.class,
                Timestamp.from(batch.deadlineAt())));
        var ownerAllowed = identityDirectory.isActiveHuman(batch.tenantId(), batch.initiatorId())
                && workspaces.isAuthorized(batch.tenantId(), batch.initiatorId(), batch.workspaceId(), "workflow:start")
                && workspaces.isAuthorized(batch.tenantId(), batch.initiatorId(), batch.workspaceId(), "task:create");
        var stopping = batch.cancelRequested() || "CANCELLING".equals(batch.status()) || expired || !ownerAllowed;
        var terminalTarget = batch.terminationTarget() != null ? batch.terminationTarget()
                : expired ? "TIMED_OUT" : "CANCELLED";
        if (stopping) {
            jdbc.update("update workflow.batch_item set status = ?, cancel_requested = true, row_version = row_version + 1, updated_at = now() "
                            + "where batch_id = ? and status = 'PENDING'",
                    terminalTarget, batch.id());
            jdbc.update("update workflow.batch_item set status = 'CANCELLING', cancel_requested = true, row_version = row_version + 1, updated_at = now() "
                            + "where batch_id = ? and status = 'ACTIVE'", batch.id());
            jdbc.update("update workflow.instance i set status = 'CANCELLING', cancel_target_status = ?, error_code = ?, waiting_reason = null, "
                            + "lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, next_poll_at = now(), "
                            + "row_version = row_version + 1, updated_at = now() from workflow.batch_item bi "
                            + "where bi.batch_id = ? and bi.workflow_instance_id = i.id and bi.status = 'CANCELLING' "
                            + "and i.status in ('QUEUED','RUNNING','WAITING_CHILD')",
                    terminalTarget, expired ? "WORKFLOW_DEADLINE_EXCEEDED" : "BATCH_CANCELLED", batch.id());
        }

        var active = jdbc.query("select bi.item_key, bi.workflow_instance_id, i.status workflow_status, i.error_code "
                        + "from workflow.batch_item bi left join workflow.instance i on i.id = bi.workflow_instance_id "
                        + "where bi.batch_id = ? and bi.status in ('ACTIVE','CANCELLING') order by bi.ordinal",
                (rs, row) -> new ActiveBatchItem(rs.getString("item_key"), rs.getObject("workflow_instance_id", UUID.class),
                        rs.getString("workflow_status"), rs.getString("error_code")), batch.id());
        for (var item : active) {
            var itemStatus = switch (item.workflowStatus()) {
                case "SUCCEEDED" -> "SUCCEEDED";
                case "FAILED" -> "FAILED";
                case "TIMED_OUT" -> "TIMED_OUT";
                case "CANCELLED" -> "CANCELLED";
                default -> null;
            };
            if (itemStatus != null) jdbc.update("update workflow.batch_item set status = ?, error_code = ?, row_version = row_version + 1, updated_at = now() "
                            + "where batch_id = ? and item_key = ? and status in ('ACTIVE','CANCELLING')",
                    itemStatus, "SUCCEEDED".equals(itemStatus) ? null : item.errorCode(), batch.id(), item.itemKey());
        }

        var counts = jdbc.query("select status, count(*) amount from workflow.batch_item where batch_id = ? group by status",
                rs -> { var result = new HashMap<String, Integer>(); while (rs.next()) result.put(rs.getString("status"), rs.getInt("amount")); return result; }, batch.id());
        int running = counts.getOrDefault("ACTIVE", 0) + counts.getOrDefault("CANCELLING", 0);
        int pending = counts.getOrDefault("PENDING", 0);
        if (!stopping && pending > 0 && running < batch.maxActiveItems()) {
            var owner = new ActorContext(batch.initiatorId(), batch.tenantId(), io.eaf.shared.ActorType.HUMAN,
                    workspaces.actions(batch.tenantId(), batch.initiatorId(), batch.workspaceId()));
            var next = jdbc.query("select item_key, request_text, scenario_key, experience_refs_json::text refs_json, stable_create_key "
                            + "from workflow.batch_item where batch_id = ? and status = 'PENDING' order by ordinal for update limit 1",
                    rs -> rs.next() ? new PendingBatchItem(rs.getString("item_key"), rs.getString("request_text"),
                            rs.getString("scenario_key"), rs.getString("refs_json"), rs.getString("stable_create_key")) : null,
                    batch.id());
            if (next != null) {
                var input = json.createObjectNode().put("requestText", next.requestText())
                        .put("scenarioKey", next.scenarioKey() == null ? "service-request" : next.scenarioKey())
                        .put("experienceRefsJson", canonical(parseJson(next.refsJson(), "experienceRefs")));
                try {
                    var workflow = createInstance(new CreateWorkflowInstanceCommand(owner, batch.workspaceId(),
                            P21_SERVICE_REQUEST_BATCH_WORKFLOW_ID, "1.0.0", canonical(input),
                            "p21-batch:" + next.stableCreateKey(), "USER", batch.deadlineAt()),
                            null, false, false, false, true);
                    jdbc.update("update workflow.batch_item set workflow_instance_id = ?, status = 'ACTIVE', row_version = row_version + 1, updated_at = now() "
                                    + "where batch_id = ? and item_key = ? and status = 'PENDING'",
                            workflow.id(), batch.id(), next.itemKey());
                } catch (EafException capacity) {
                    if (!"TASK_CAPACITY_EXCEEDED".equals(capacity.code())) throw capacity;
                    jdbc.update("update workflow.batch set next_poll_at = now() + interval '1 second', updated_at = now() where id = ?", batch.id());
                }
            }
        }

        var summary = jdbc.query("select status, count(*) amount from workflow.batch_item where batch_id = ? group by status",
                rs -> { var result = new HashMap<String, Integer>(); while (rs.next()) result.put(rs.getString("status"), rs.getInt("amount")); return result; }, batch.id());
        int unfinished = summary.getOrDefault("PENDING", 0) + summary.getOrDefault("ACTIVE", 0) + summary.getOrDefault("CANCELLING", 0);
        if (unfinished == 0) {
            int succeeded = summary.getOrDefault("SUCCEEDED", 0);
            String status = stopping ? terminalTarget : succeeded == batch.itemCount() ? "SUCCEEDED"
                    : succeeded > 0 ? "PARTIAL" : "FAILED";
            jdbc.update("update workflow.batch set status = ?, termination_target = ?, lease_owner_id = null, lease_until = null, next_poll_at = now(), row_version = row_version + 1, updated_at = now() where id = ?",
                    status, stopping ? terminalTarget : null, batch.id());
        } else {
            jdbc.update("update workflow.batch set status = case when ? or status = 'CANCELLING' then 'CANCELLING' else 'RUNNING' end, "
                            + "next_poll_at = now() + interval '1 second', row_version = row_version + 1, updated_at = now() where id = ?",
                    stopping, batch.id());
        }
        return true;
    }

    private record RegistrationReceipt(WorkflowInstance workflow,
                                       io.eaf.execution.api.ExecutionSnapshot execution, String requestId) { }

    private RegistrationReceipt verifiedServiceRequestRegistration(ActorContext actor, UUID workspaceId,
            UUID submissionId, io.eaf.task.api.ServiceRequestSubmission submission) {
        if (submission.workflowInstanceId() == null) throw EafException.conflict("SERVICE_REQUEST_NOT_VERIFIED", "登记 Workflow 尚未绑定。");
        var registration = loadInstance(actor.tenantId(), workspaceId, submission.workflowInstanceId());
        var registrationInput = parseJson(registration.inputJson(), "registration.input");
        if (!P15_REGISTERING_WORKFLOW_ID.equals(registration.workflowId())
                || !P15_SERVICE_REQUEST_WORKFLOW_VERSION.equals(registration.workflowVersion())
                || !"USER".equals(registration.source()) || !actor.actorId().equals(registration.actorId())
                || !submissionId.toString().equals(registrationInput.path("submissionId").asText())
                || !"SUCCEEDED".equals(registration.status()))
            throw EafException.conflict("SERVICE_REQUEST_NOT_VERIFIED", "服务请求尚未由固定登记流程核验成功。");
        var registered = parseJson(registration.resultJson(), "registration.result");
        var requestId = registered.path("requestId").asText(null);
        UUID operationId;
        try { operationId = UUID.fromString(registered.path("operationId").asText()); }
        catch (IllegalArgumentException invalid) { operationId = null; }
        var registerStep = stepRuntime(registration.id(), "register");
        if (!"REGISTERED".equals(registered.path("status").asText()) || requestId == null
                || requestId.isBlank() || requestId.trim().length() > 160 || operationId == null
                || registerStep == null || registerStep.childTaskId() == null)
            throw EafException.conflict("SERVICE_REQUEST_NOT_VERIFIED", "登记回执缺少有效 requestId 或 operationId。");
        var task = tasks.get(actor, workspaceId, registerStep.childTaskId());
        var execution = executions.findServiceRequestRegistration(actor, workspaceId, task.id(), task.attempt(), submissionId)
                .orElseThrow(() -> EafException.conflict("SERVICE_REQUEST_NOT_VERIFIED", "未找到原登记 Execution 回执。"));
        var executionResult = parseJson(execution.resultJson(), "registration.executionResult");
        if (task.status() != io.eaf.task.api.TaskStatus.SUCCEEDED || !"SUCCEEDED".equals(execution.status())
                || !operationId.equals(execution.operationId())
                || !requestId.equals(executionResult.path("requestId").asText()))
            throw EafException.conflict("SERVICE_REQUEST_NOT_VERIFIED", "Workflow、Task 与 Execution 登记回执不一致。");
        return new RegistrationReceipt(registration, execution, requestId.trim());
    }

    private WorkflowInstance existingHandling(UUID tenantId, UUID workspaceId, UUID actorId, String keyHash) {
        var id = jdbc.query("select id from workflow.instance where tenant_id = ? and workspace_id = ? and actor_id = ? "
                        + "and idempotency_key_hash = ? and workflow_id = ? and source = 'USER'",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null, tenantId, workspaceId, actorId,
                keyHash, P16_SERVICE_REQUEST_WORKFLOW_ID);
        return id == null ? null : loadInstance(tenantId, workspaceId, id);
    }

    private void requireHandlingReplayMatches(WorkflowInstance existing, UUID submissionId, UUID assigneeId,
            String brief, Instant requestedDeadline, String scenarioKey, List<TeamExperienceRef> refs) {
        var input = parseJson(existing.inputJson(), "handling.input");
        var storedRefs = input.path("teamExperienceRefsJson").asText(null);
        var requestedRefs = refs.isEmpty() ? null : canonical(json.valueToTree(refs));
        if (!submissionId.toString().equals(input.path("submissionId").asText())
                || !assigneeId.toString().equals(input.path("assigneeId").asText())
                || !brief.equals(input.path("sharedBrief").asText())
                || !java.util.Objects.equals(scenarioKey, input.path("scenarioKey").isMissingNode()
                        ? null : input.path("scenarioKey").asText())
                || !java.util.Objects.equals(requestedRefs, storedRefs)
                || requestedDeadline != null && !existing.deadlineAt().equals(requestedDeadline.truncatedTo(ChronoUnit.MICROS)))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一幂等键对应了不同处理请求。");
    }

    private void requireEligibleAssignee(UUID tenantId, UUID workspaceId, UUID assigneeId) {
        if (!identityDirectory.isActiveHuman(tenantId, assigneeId)
                || !workspaces.isAuthorized(tenantId, assigneeId, workspaceId, "work-item:read")
                || !workspaces.isAuthorized(tenantId, assigneeId, workspaceId, "work-item:complete"))
            throw EafException.invalid("处理人必须是当前有效且已获工作项读取与完成权限的 Workspace HUMAN 成员。");
    }

    private void requireDirectHuman(ActorContext actor) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("人工协作只接受直接 HUMAN 身份。");
    }

    private BatchRow requireBatch(ActorContext actor, UUID workspaceId, UUID batchId, String action) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, action);
        var row = jdbc.query("select id, initiator_id, status, row_version, created_at, deadline_at, max_active_items, item_count "
                        + "from workflow.batch where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new BatchRow(rs.getObject("id", UUID.class), rs.getObject("initiator_id", UUID.class),
                        rs.getString("status"), rs.getLong("row_version"), rs.getTimestamp("created_at").toInstant(),
                        rs.getTimestamp("deadline_at").toInstant(), rs.getInt("max_active_items"), rs.getInt("item_count")) : null,
                batchId, access.tenantId(), workspaceId);
        if (row == null || !actor.actorId().equals(row.initiatorId())) throw EafException.notFound();
        requireActiveHuman(actor);
        return row;
    }

    private List<TeamExperienceRef> parseRefs(String raw) {
        try { return json.readValue(raw, new TypeReference<>() { }); }
        catch (Exception invalid) { throw EafException.conflict("TEAM_EXPERIENCE_SELECTION_INVALID", "冻结的团队经验引用无法读取。"); }
    }

    private ServiceRequestBatchSnapshot.Item itemSnapshot(UUID batchId, String itemKey) {
        return jdbc.query("select i.item_key, i.ordinal, i.status, i.row_version, i.workflow_instance_id, i.error_code, "
                        + "(select p.child_task_id from workflow.parallel_branch p where p.instance_id = i.workflow_instance_id and p.role = 'knowledge') knowledge_task_id, "
                        + "(select p.child_task_id from workflow.parallel_branch p where p.instance_id = i.workflow_instance_id and p.role = 'experience') experience_task_id "
                        + "from workflow.batch_item i where i.batch_id = ? and i.item_key = ?",
                rs -> rs.next() ? new ServiceRequestBatchSnapshot.Item(rs.getString("item_key"), rs.getInt("ordinal"),
                        rs.getString("status"), rs.getLong("row_version"), rs.getObject("workflow_instance_id", UUID.class),
                        rs.getString("error_code"), rs.getObject("knowledge_task_id", UUID.class),
                        rs.getObject("experience_task_id", UUID.class)) : null, batchId, itemKey);
    }

    private ServiceRequestHandlingSnapshot handlingSnapshot(WorkflowInstance instance, HumanWorkItem item) {
        var input = parseJson(instance.inputJson(), "handling.input");
        var result = instance.resultJson() == null ? json.createObjectNode() : parseJson(instance.resultJson(), "handling.result");
        return new ServiceRequestHandlingSnapshot(instance.id(), instance.rowVersion(), input.path("requestId").asText(),
                instance.status(), instance.currentStepId(), instance.deadlineAt(), item == null ? null : item.id(),
                item == null ? result.path("outcome").asText(null) : item.outcome(),
                item == null ? result.path("summary").asText(null) : item.summary(),
                item == null ? result.path("nextAction").asText(null) : item.nextAction(),
                item == null ? uuidOrNull(result.path("completedBy").asText(null)) : item.completedBy(),
                item == null ? instantOrNull(result.path("completedAt").asText(null)) : item.completedAt(),
                result.path("resultSummary").asText(null), result.path("remainingWork").asText(null), instance.errorCode(),
                result.has("teamExperienceUsage") ? result.get("teamExperienceUsage") : null);
    }

    @Override
    public Optional<WorkflowInstance> findInstanceByIdempotencyKey(CreateWorkflowInstanceCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null
                || command.workflowId() == null || command.idempotencyKey() == null
                || command.idempotencyKey().isBlank() || command.idempotencyKey().length() > 200)
            throw EafException.invalid("Workflow 幂等查询请求不完整。");
        var actor = command.actor();
        var access = workspaces.require(actor, command.workspaceId(), "workflow:start");
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(),
                command.workspaceId().toString(), actor.actorId().toString(), command.idempotencyKey()));
        var existing = jdbc.query("select id, request_hash, workflow_id, workflow_version, definition_hash, source "
                        + "from workflow.instance where tenant_id = ? and workspace_id = ? and actor_id = ? and idempotency_key_hash = ?",
                rs -> rs.next() ? new IdempotentWorkflow(rs.getObject("id", UUID.class), rs.getString("request_hash"),
                        rs.getObject("workflow_id", UUID.class), rs.getString("workflow_version"),
                        rs.getString("definition_hash"), rs.getString("source")) : null,
                access.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (existing == null) return Optional.empty();
        var canonicalInput = canonical(parseJson(command.inputJson(), "inputJson"));
        var inputHash = Hashing.sha256(canonicalInput);
        var requestHash = Hashing.sha256(String.join("\u001f", command.workflowId().toString(),
                command.workflowVersion(), existing.definitionHash(), inputHash, command.source(),
                actor.actorId().toString(), actor.principalIdOrActorId().toString(),
                String.valueOf(actor.delegationId()), String.valueOf(actor.authorizationHash()),
                String.valueOf(command.deadlineAt())));
        if (!requestHash.equals(existing.requestHash()) || !existing.workflowId().equals(command.workflowId())
                || !existing.workflowVersion().equals(command.workflowVersion()) || !existing.source().equals(command.source()))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一 Workflow 幂等键对应了不同请求。");
        return Optional.of(loadInstance(access.tenantId(), command.workspaceId(), existing.id()));
    }

    @Override
    public Optional<WorkflowInstance> lookupInstanceByIdempotencyKey(ActorContext actor, UUID workspaceId,
            UUID workflowId, String workflowVersion, String idempotencyKey) {
        if (actor == null || workspaceId == null || workflowId == null || workflowVersion == null
                || idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200)
            throw EafException.invalid("Workflow 幂等查询请求不完整。");
        var access = workspaces.require(actor, workspaceId, "workflow:start");
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(), workspaceId.toString(),
                actor.actorId().toString(), idempotencyKey));
        var existing = jdbc.query("select id, workflow_id, workflow_version from workflow.instance "
                        + "where tenant_id = ? and workspace_id = ? and actor_id = ? and idempotency_key_hash = ?",
                rs -> rs.next() ? new IdempotentWorkflowIdentity(rs.getObject("id", UUID.class),
                        rs.getObject("workflow_id", UUID.class), rs.getString("workflow_version")) : null,
                access.tenantId(), workspaceId, actor.actorId(), keyHash);
        if (existing == null || !workflowId.equals(existing.workflowId())
                || !workflowVersion.equals(existing.workflowVersion())) return Optional.empty();
        return Optional.of(loadInstance(access.tenantId(), workspaceId, existing.id()));
    }

    private record IdempotentWorkflow(UUID id, String requestHash, UUID workflowId,
                                      String workflowVersion, String definitionHash, String source) { }
    private record IdempotentWorkflowIdentity(UUID id, UUID workflowId, String workflowVersion) { }

    @Override
    @Transactional
    public WorkflowInstance createQualityRunInstance(CreateQualityRunWorkflowCommand command) {
        if (command == null || command.qualityRunId() == null)
            throw EafException.invalid("质量 Workflow 缺少 Evaluation 服务端运行标记。");
        workspaces.require(command.actor(), command.workspaceId(), "evaluation:run");
        // Workflow 创建预算前经 Task→Evaluation 端口核对来源及协作清单绑定的固定版本。
        tasks.requireQualityRunWorkflow(command.actor(), command.workspaceId(), command.qualityRunId(),
                command.source(), command.workflowId(), command.workflowVersion());
        var request = new CreateWorkflowInstanceCommand(command.actor(), command.workspaceId(), command.workflowId(),
                command.workflowVersion(), command.inputJson(), command.idempotencyKey(), command.source(), command.deadlineAt());
        return createInstance(request, command.qualityRunId(), false, false, false, false);
    }

    private WorkflowInstance createInstance(CreateWorkflowInstanceCommand command, UUID qualityRunId,
                                           boolean p12ResultWorkflow, boolean p15ServiceRequestWorkflow,
                                           boolean p16ServiceRequestHandlingWorkflow, boolean p21BatchWorkflow) {
        return createInstance(command, qualityRunId, p12ResultWorkflow, p15ServiceRequestWorkflow,
                p16ServiceRequestHandlingWorkflow, p21BatchWorkflow, false);
    }

    private WorkflowInstance createInstance(CreateWorkflowInstanceCommand command, UUID qualityRunId,
                                           boolean p12ResultWorkflow, boolean p15ServiceRequestWorkflow,
                                           boolean p16ServiceRequestHandlingWorkflow, boolean p21BatchWorkflow,
                                           boolean p27BusinessWorkflow) {
        return createInstance(command, qualityRunId, p12ResultWorkflow, p15ServiceRequestWorkflow,
                p16ServiceRequestHandlingWorkflow, p21BatchWorkflow, p27BusinessWorkflow, false);
    }

    private WorkflowInstance createInstance(CreateWorkflowInstanceCommand command, UUID qualityRunId,
                                           boolean p12ResultWorkflow, boolean p15ServiceRequestWorkflow,
                                           boolean p16ServiceRequestHandlingWorkflow, boolean p21BatchWorkflow,
                                           boolean p27BusinessWorkflow, boolean p29ProjectBriefWorkflow) {
        if (command == null || command.actor() == null || command.workspaceId() == null || command.workflowId() == null
                || command.idempotencyKey() == null || command.idempotencyKey().isBlank()
                || command.idempotencyKey().length() > 200)
            throw EafException.invalid("Workflow 实例创建请求不完整。");
        var actor = command.actor();
        var access = workspaces.require(actor, command.workspaceId(), "workflow:start");
        if (P12_RESULT_WORKFLOW_ID.equals(command.workflowId()) != p12ResultWorkflow)
            throw EafException.forbidden("结果同步流程只能通过固定的业务入口启动。");
        if (P15_SERVICE_REQUEST_WORKFLOW_ID.equals(command.workflowId()) != p15ServiceRequestWorkflow)
            throw EafException.forbidden("服务请求登记流程只能通过固定的业务入口启动。");
        if (P16_SERVICE_REQUEST_WORKFLOW_ID.equals(command.workflowId()) != p16ServiceRequestHandlingWorkflow)
            throw EafException.forbidden("服务请求协作流程只能通过固定的来源核验入口启动。");
        if (P21_SERVICE_REQUEST_BATCH_WORKFLOW_ID.equals(command.workflowId()) != p21BatchWorkflow)
            throw EafException.forbidden("批次分析流程只能由批次 Owner 创建。");
        if (p27WorkflowId(command.workflowId()) != p27BusinessWorkflow)
            throw EafException.forbidden("P27 业务连接流程只能通过固定业务入口启动。");
        if (P29_PROJECT_BRIEF_WORKFLOW_ID.equals(command.workflowId()) != p29ProjectBriefWorkflow)
            throw EafException.forbidden("项目简报流程只能通过固定来源核验入口启动。");
        if (!"USER".equals(command.source()) && !"EVALUATION".equals(command.source()))
            throw EafException.invalid("Workflow source 无效。");
        if (command.deadlineAt() != null && !jdbc.queryForObject("select ?::timestamptz > now() and ?::timestamptz <= now() + interval '30 days'",
                Boolean.class, Timestamp.from(command.deadlineAt()), Timestamp.from(command.deadlineAt())))
            throw EafException.invalid("Workflow 截止时间必须晚于当前时间且不超过 30 天。");
        if (actor.delegated() && !"USER".equals(command.source()))
            throw EafException.forbidden("委托 Workflow 只能保留 USER 来源。");
        if ("EVALUATION".equals(command.source())) workspaces.require(actor, command.workspaceId(), "evaluation:run");
        if (qualityRunId != null) workspaces.require(actor, command.workspaceId(), "evaluation:run");
        var definition = load(access.tenantId(), command.workspaceId(), command.workflowId(), command.workflowVersion());
        if (!"PUBLISHED".equals(definition.status())) throw EafException.notFound();
        var resolved = validateDefinition(actor, command.workspaceId(), definition);
        var canonicalInput = canonical(parseJson(command.inputJson(), "inputJson"));
        var input = parseJson(canonicalInput, "inputJson");
        schemas.validateInput(definition.inputSchema(), input);
        var inputHash = Hashing.sha256(canonicalInput);
        var requestHash = Hashing.sha256(String.join("\u001f", definition.id().toString(), definition.version(),
                definition.contentHash(), inputHash, command.source(), actor.actorId().toString(),
                actor.principalIdOrActorId().toString(), String.valueOf(actor.delegationId()),
                String.valueOf(actor.authorizationHash()), String.valueOf(command.deadlineAt())));
        if (qualityRunId != null) requestHash = Hashing.sha256(requestHash + "\u001fquality-run:" + qualityRunId);
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(),
                command.workspaceId().toString(), actor.actorId().toString(), command.idempotencyKey()));
        // Task 域持久创建预算；稳定来源键使跨调用重放取得同一范围，不因模型或流程版本重置。
        var budget = tasks.createBudgetScope(new CreateBudgetScopeCommand(actor, command.workspaceId(),
                "workflow:" + keyHash, requestHash));
        var dependencySnapshot = canonical(json.valueToTree(resolved.values().stream()
                .sorted(Comparator.comparing(value -> value.id().toString() + "@" + value.version())).toList()));
        var id = UUID.randomUUID();
        try {
            var inserted = jdbc.update("insert into workflow.instance(id, tenant_id, workspace_id, actor_id, initiator_id, delegation_id, authorization_hash, source, quality_run_id, workflow_id, workflow_version, definition_hash, root_budget_scope_id, input_json, input_hash, definition_snapshot, dependency_snapshot, idempotency_key_hash, request_hash, deadline_at, status) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?::jsonb, ?::jsonb, ?, ?, coalesce(?::timestamptz, now() + interval '24 hours'), 'QUEUED') on conflict (tenant_id, workspace_id, actor_id, idempotency_key_hash) do nothing",
                    id, access.tenantId(), command.workspaceId(), actor.actorId(), actor.principalIdOrActorId(), actor.delegationId(),
                    actor.authorizationHash(), command.source(), qualityRunId, definition.id(), definition.version(), definition.contentHash(), budget.id(), canonicalInput,
                    inputHash, canonical(json.valueToTree(definition)), dependencySnapshot, keyHash, requestHash,
                    command.deadlineAt() == null ? null : Timestamp.from(command.deadlineAt()));
            if (inserted == 1) return loadInstance(access.tenantId(), command.workspaceId(), id);
        } catch (DuplicateKeyException e) {
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "Workflow 实例幂等键已占用。");
        }
        var existing = jdbc.query("select id, request_hash from workflow.instance where tenant_id = ? and workspace_id = ? and actor_id = ? and idempotency_key_hash = ?",
                rs -> rs.next() ? new ExistingInstance(rs.getObject("id", UUID.class), rs.getString("request_hash")) : null,
                access.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (existing == null || !requestHash.equals(existing.requestHash()))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一 Workflow 幂等键对应了不同请求。");
        return loadInstance(access.tenantId(), command.workspaceId(), existing.id());
    }

    @Override
    public WorkflowInstance getInstance(ActorContext actor, UUID workspaceId, UUID instanceId) {
        var access = workspaces.require(actor, workspaceId, "workflow:read");
        return loadInstance(access.tenantId(), workspaceId, instanceId);
    }

    @Override
    public WorkflowSourcePage listForSourceTask(ActorContext actor, UUID workspaceId, UUID sourceTaskId,
                                                Instant cursorCreatedAt, UUID cursorInstanceId, int pageSize) {
        var access = workspaces.require(actor, workspaceId, "workflow:read");
        if (sourceTaskId == null || pageSize < 1 || pageSize > 50
                || (cursorCreatedAt == null) != (cursorInstanceId == null))
            throw EafException.invalid("Workflow 来源列表分页参数无效。");
        var where = " where tenant_id = ? and workspace_id = ? and source = 'USER' and input_json ->> 'sourceTaskId' = ?";
        var base = new ArrayList<Object>(List.of(access.tenantId(), workspaceId, sourceTaskId.toString()));
        var total = jdbc.queryForObject("select count(*) from workflow.instance" + where, Long.class, base.toArray());
        var pageWhere = new StringBuilder(where);
        var args = new ArrayList<>(base);
        if (cursorCreatedAt != null) {
            pageWhere.append(" and (created_at, id) < (?, ?)");
            args.add(Timestamp.from(cursorCreatedAt));
            args.add(cursorInstanceId);
        }
        args.add(pageSize + 1);
        var ids = jdbc.query("select id, created_at from workflow.instance" + pageWhere
                        + " order by created_at desc, id desc limit ?",
                (rs, row) -> new SourceInstanceKey(rs.getObject("id", UUID.class), rs.getTimestamp("created_at").toInstant()),
                args.toArray());
        var hasNext = ids.size() > pageSize;
        var selected = hasNext ? List.copyOf(ids.subList(0, pageSize)) : List.copyOf(ids);
        var items = selected.stream().map(item -> loadInstance(access.tenantId(), workspaceId, item.id())).toList();
        var last = hasNext ? selected.get(selected.size() - 1) : null;
        return new WorkflowSourcePage(items, total == null ? 0 : total,
                last == null ? null : last.createdAt(), last == null ? null : last.id());
    }

    @Override
    public WorkflowOperationsPage listOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                                  Instant createdAfter, WorkflowOperationsCursor cursor,
                                                  int pageSize) {
        var access = workspaces.require(actor, workspaceId, "workflow:read");
        if (pageSize < 1 || pageSize > 100 || (cursor != null && (cursor.createdAt() == null || cursor.instanceId() == null))
                || statuses != null && !OPERATION_STATUSES.containsAll(statuses))
            throw EafException.invalid("Workflow 运维列表分页或状态过滤无效。");
        var where = new StringBuilder(" where tenant_id = ? and workspace_id = ?");
        var filters = new ArrayList<Object>();
        filters.add(access.tenantId());
        filters.add(workspaceId);
        if (statuses != null) {
            if (statuses.isEmpty()) where.append(" and 1 = 0");
            else {
                where.append(" and status in (").append(String.join(",", java.util.Collections.nCopies(statuses.size(), "?"))).append(')');
                statuses.forEach(filters::add);
            }
        }
        if (createdAfter != null) {
            where.append(" and created_at >= ?");
            filters.add(Timestamp.from(createdAfter));
        }
        var totalSize = jdbc.queryForObject("select count(*) from workflow.instance" + where, Long.class, filters.toArray());
        var pageWhere = new StringBuilder(where);
        var pageArgs = new ArrayList<>(filters);
        if (cursor != null) {
            pageWhere.append(" and (created_at, id) < (?, ?)");
            pageArgs.add(Timestamp.from(cursor.createdAt()));
            pageArgs.add(cursor.instanceId());
        }
        pageArgs.add(pageSize + 1);
        // 只选运行关联和处置状态，避免把客户输入、执行结果或固定定义快照带入运维响应。
        var selected = jdbc.query("select id, workflow_id, workflow_version, status, current_step_id, root_task_id, error_code, row_version, created_at, deadline_at, waiting_reason, business_effect_status, "
                        + "(select s.child_task_id from workflow.step s where s.instance_id = workflow.instance.id and s.step_id = workflow.instance.current_step_id) child_task_id "
                        + "from workflow.instance" + pageWhere + " order by created_at desc, id desc limit ?",
                (rs, row) -> new WorkflowOperationsItem(rs.getObject("id", UUID.class),
                        rs.getObject("workflow_id", UUID.class), rs.getString("workflow_version"),
                        rs.getString("status"), rs.getString("current_step_id"), rs.getObject("root_task_id", UUID.class),
                        rs.getObject("child_task_id", UUID.class), rs.getString("error_code"), rs.getString("waiting_reason"),
                        rs.getString("business_effect_status"), rs.getLong("row_version"),
                        rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("deadline_at").toInstant()), pageArgs.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        var next = last == null ? null : new WorkflowOperationsCursor(last.createdAt(), last.instanceId());
        return new WorkflowOperationsPage(items, totalSize == null ? 0 : totalSize, next);
    }

    @Override
    @Transactional
    public WorkflowInstance cancelInstance(ActorContext actor, UUID workspaceId, UUID instanceId, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "workflow:write");
        var current = loadInstance(access.tenantId(), workspaceId, instanceId);
        if (jdbc.queryForObject("select count(*) from workflow.batch_item where workflow_instance_id = ?",
                Integer.class, instanceId) > 0)
            throw EafException.conflict("BATCH_WORKFLOW_MANAGED", "Workflow 必须通过批次或批次项目取消。");
        if (current.rowVersion() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "Workflow 实例版本已变化。");
        if (Set.of("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT").contains(current.status())) return current;
        if ("CANCELLING".equals(current.status())) return current;
        var changed = jdbc.update("update workflow.instance set status = 'CANCELLING', cancel_target_status = 'CANCELLED', error_code = 'WORKFLOW_CANCELLED', waiting_reason = null, lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, next_poll_at = now(), row_version = row_version + 1, updated_at = now() where id = ? and tenant_id = ? and workspace_id = ? and row_version = ? and status in ('QUEUED','RUNNING','WAITING_CHILD','WAITING_HUMAN')",
                instanceId, access.tenantId(), workspaceId, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "Workflow 取消竞争失败。");
        closeOpenHumanWorkItem(instanceId, "CANCELLED");
        var updated = loadInstance(access.tenantId(), workspaceId, instanceId);
        var cancellation = cancelCurrentChild(updated);
        recordCancellationProgress(instanceId, cancellation);
        audit.append(new AuditFact("workflow-cancelled:" + instanceId + ":" + expectedVersion,
                access.tenantId(), workspaceId, actor.actorId(), instanceId, "WORKFLOW_CANCEL_REQUESTED",
                cancellation.externalEffectPending() ? "WAITING_VERIFICATION" : "CANCELLING", "{}", null));
        return loadInstance(access.tenantId(), workspaceId, instanceId);
    }

    @Override
    public WorkflowInstance requireRunnable(ActorContext actor, UUID workspaceId, UUID instanceId) {
        var access = workspaces.require(actor, workspaceId, "workflow:start");
        var instance = loadInstance(access.tenantId(), workspaceId, instanceId);
        if (!instance.deadlineAt().isAfter(Instant.now()))
            throw EafException.conflict("DEADLINE_EXCEEDED", "Workflow 截止时间已到。");
        if (P29_PROJECT_BRIEF_WORKFLOW_ID.equals(instance.workflowId())) requireProjectBriefCurrent(actor, instance);
        var definition = load(access.tenantId(), workspaceId, instance.workflowId(), instance.workflowVersion());
        if (!"PUBLISHED".equals(definition.status()))
            throw EafException.conflict("WORKFLOW_WITHDRAWN", "Workflow 版本已撤回，不能继续发起步骤。");
        var resolved = validateDefinition(actor, workspaceId, definition);
        var currentSnapshot = canonical(json.valueToTree(resolved.values().stream()
                .sorted(Comparator.comparing(value -> value.id().toString() + "@" + value.version())).toList()));
        if (!currentSnapshot.equals(canonical(parseJson(instance.dependencySnapshot(), "dependencySnapshot"))))
            throw EafException.conflict("WORKFLOW_DEPENDENCY_CHANGED", "Workflow 固定依赖已撤回或内容发生变化。");
        return instance;
    }

    @Transactional
    public void openHumanWorkItem(WorkflowLease lease, String stepId) {
        requireLease(lease);
        var instance = loadInstanceById(lease.instanceId());
        if (P29_PROJECT_BRIEF_WORKFLOW_ID.equals(instance.workflowId())) {
            if (!Set.of("review", "handoff").contains(stepId)
                    || !P29_PROJECT_BRIEF_WORKFLOW_VERSION.equals(instance.workflowVersion())
                    || !stepId.equals(instance.currentStepId()))
                throw EafException.forbidden("项目简报只允许固定 review/handoff 人工步骤创建工作项。");
            var creator = briefActor(instance.tenantId(), instance.workspaceId(), instance.actorId());
            requireProjectBriefCurrent(creator, instance);
            var p29Step = stepRuntime(instance.id(), stepId);
            if (p29Step == null || !"HUMAN_TASK".equals(p29Step.stepType()))
                throw EafException.conflict("WORKFLOW_STEP_MISSING", "固定项目简报人工步骤意图不存在。");
            var p29Input = parseJson(p29Step.inputJson(), "projectBrief.human.input");
            UUID p29Assignee;
            try { p29Assignee = UUID.fromString(p29Input.path("assigneeId").asText()); }
            catch (IllegalArgumentException invalid) { throw EafException.invalid("固定项目简报步骤缺少有效处理人。"); }
            requireEligibleAssignee(instance.tenantId(), instance.workspaceId(), p29Assignee);
            var p29WorkId = UUID.randomUUID();
            var insertedP29 = jdbc.update("insert into workflow.human_work_item(id, tenant_id, workspace_id, instance_id, step_id, assignee_id, status) "
                            + "values (?, ?, ?, ?, ?, ?, 'OPEN') on conflict (instance_id, step_id) do nothing",
                    p29WorkId, instance.tenantId(), instance.workspaceId(), instance.id(), stepId, p29Assignee);
            var existingP29 = jdbc.query("select id, assignee_id, status from workflow.human_work_item where instance_id = ? and step_id = ? for update",
                    rs -> rs.next() ? new ExistingHumanWorkItem(rs.getObject("id", UUID.class),
                            rs.getObject("assignee_id", UUID.class), rs.getString("status")) : null,
                    instance.id(), stepId);
            if (existingP29 == null || !p29Assignee.equals(existingP29.assigneeId()) || !"OPEN".equals(existingP29.status()))
                throw EafException.conflict("HUMAN_WORK_ITEM_CONFLICT", "固定项目简报人工工作项与冻结输入不匹配。");
            var p29StepChanged = jdbc.update("update workflow.step set status = 'WAITING_HUMAN', updated_at = now() "
                            + "where instance_id = ? and step_id = ? and step_type = 'HUMAN_TASK' and status in ('INTENT','WAITING_HUMAN')",
                    instance.id(), stepId);
            if (p29StepChanged != 1) throw EafException.conflict("WORKFLOW_STEP_CONFLICT", "项目简报人工步骤已不处于等待状态。");
            var p29Changed = jdbc.update("update workflow.instance set status = 'WAITING_HUMAN', current_step_id = ?, waiting_reason = 'HUMAN_TASK', "
                            + "lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, next_poll_at = deadline_at, "
                            + "row_version = row_version + 1, updated_at = now() where id = ? and lease_owner_id = ? and lease_fence = ? "
                            + "and lease_until > now() and status in ('RUNNING','WAITING_HUMAN') and deadline_at > now()",
                    stepId, instance.id(), lease.ownerId(), lease.fence());
            if (p29Changed != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "项目简报人工工作项等待状态无法提交。");
            if (insertedP29 == 1) audit.append(new AuditFact("p29-work-item-created:" + existingP29.id(),
                    instance.tenantId(), instance.workspaceId(), instance.actorId(), existingP29.id(),
                    "PROJECT_BRIEF_WORK_ITEM_CREATED", stepId.toUpperCase(java.util.Locale.ROOT), "{}", null));
            return;
        }
        if (!"handle".equals(stepId)) throw EafException.forbidden("只允许固定 handle 人工步骤创建工作项。");
        if (!P16_SERVICE_REQUEST_WORKFLOW_ID.equals(instance.workflowId())
                || !Set.of(P16_SERVICE_REQUEST_WORKFLOW_VERSION, P17_SERVICE_REQUEST_WORKFLOW_VERSION,
                        P18_SERVICE_REQUEST_WORKFLOW_VERSION).contains(instance.workflowVersion())
                || !"handle".equals(instance.currentStepId()))
            throw EafException.forbidden("人工工作项不属于固定流程。");
        var step = stepRuntime(instance.id(), stepId);
        if (step == null || !"HUMAN_TASK".equals(step.stepType()))
            throw EafException.conflict("WORKFLOW_STEP_MISSING", "固定人工步骤意图不存在。");
        var input = parseJson(step.inputJson(), "human.input");
        UUID assigneeId;
        try { assigneeId = UUID.fromString(input.path("assigneeId").asText()); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("固定人工步骤缺少有效处理人。"); }
        requireEligibleAssignee(instance.tenantId(), instance.workspaceId(), assigneeId);
        var sharedBrief = requireText(input.path("sharedBrief").asText(null), 2_000, "sharedBrief");
        var handlingAdvice = requireText(input.path("handlingAdvice").asText(null), 2_000, "handlingAdvice");
        var cautions = input.path("cautions").asText(null);
        if (cautions == null || cautions.length() > 1_000)
            throw EafException.invalid("固定人工步骤 cautions 长度无效。");
        var workItemId = UUID.randomUUID();
        var inserted = jdbc.update("insert into workflow.human_work_item(id, tenant_id, workspace_id, instance_id, step_id, assignee_id, status) "
                        + "values (?, ?, ?, ?, ?, ?, 'OPEN') on conflict (instance_id, step_id) do nothing",
                workItemId, instance.tenantId(), instance.workspaceId(), instance.id(), stepId, assigneeId);
        var existing = jdbc.query("select id, assignee_id, status from workflow.human_work_item where instance_id = ? and step_id = ? for update",
                rs -> rs.next() ? new ExistingHumanWorkItem(rs.getObject("id", UUID.class),
                        rs.getObject("assignee_id", UUID.class), rs.getString("status")) : null,
                instance.id(), stepId);
        if (existing == null || !assigneeId.equals(existing.assigneeId()) || !"OPEN".equals(existing.status()))
            throw EafException.conflict("HUMAN_WORK_ITEM_CONFLICT", "固定人工工作项与冻结输入不匹配。");
        var stepChanged = jdbc.update("update workflow.step set status = 'WAITING_HUMAN', updated_at = now() "
                        + "where instance_id = ? and step_id = ? and step_type = 'HUMAN_TASK' and status in ('INTENT','WAITING_HUMAN')",
                instance.id(), stepId);
        if (stepChanged != 1) throw EafException.conflict("WORKFLOW_STEP_CONFLICT", "人工步骤已不处于等待状态。");
        var changed = jdbc.update("update workflow.instance set status = 'WAITING_HUMAN', current_step_id = ?, waiting_reason = 'HUMAN_TASK', "
                        + "lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, next_poll_at = deadline_at, "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and lease_owner_id = ? and lease_fence = ? "
                        + "and lease_until > now() and status in ('RUNNING','WAITING_HUMAN') and deadline_at > now()",
                stepId, instance.id(), lease.ownerId(), lease.fence());
        if (changed != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "人工工作项等待状态无法提交。");
        if (inserted == 1)
            audit.append(new AuditFact("p16-work-item-created:" + existing.id(), instance.tenantId(), instance.workspaceId(),
                    instance.actorId(), existing.id(), "HUMAN_WORK_ITEM_CREATED", "OPEN", "{}", null));
        if (inserted == 1) recordP16WorkItemEvent(existing.id(), "CREATED", null, assigneeId);
    }

    private record ExistingHumanWorkItem(UUID id, UUID assigneeId, String status) { }

    @Override
    @Transactional(readOnly = true)
    public ServiceRequestHandlingSnapshot getServiceRequestHandling(ActorContext actor, UUID workspaceId, UUID instanceId) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        requireActiveHuman(actor);
        var instance = loadInstance(access.tenantId(), workspaceId, instanceId);
        if (!P16_SERVICE_REQUEST_WORKFLOW_ID.equals(instance.workflowId())) throw EafException.notFound();
        var item = handlingItem(access.tenantId(), workspaceId, instanceId, actor);
        if (!actor.actorId().equals(instance.actorId()) && (item == null || !actor.actorId().equals(item.assigneeId())))
            throw EafException.notFound();
        return handlingSnapshot(instance, item);
    }

    private HumanWorkItem handlingItem(UUID tenantId, UUID workspaceId, UUID instanceId, ActorContext actor) {
        var itemId = jdbc.query("select id from workflow.human_work_item where instance_id = ?",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null, instanceId);
        return itemId == null ? null : loadHumanWorkItem(tenantId, workspaceId, itemId, actor);
    }

    @Override
    @Transactional(readOnly = true)
    public HumanWorkItemPage listHumanWorkItems(ActorContext actor, UUID workspaceId, String relation, String status,
            Instant cursorCreatedAt, UUID cursorId, int pageSize) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        requireActiveHuman(actor);
        var view = relation == null ? "assigned" : relation;
        var filterStatus = status == null ? "OPEN" : status;
        if (!Set.of("assigned", "created").contains(view) || !HUMAN_ITEM_STATUSES.contains(filterStatus)
                || pageSize < 1 || pageSize > 50 || (cursorCreatedAt == null) != (cursorId == null))
            throw EafException.invalid("Human work item 过滤或游标参数无效。");
        var where = new StringBuilder(" where h.tenant_id = ? and h.workspace_id = ? and h.status = ? and i.workflow_id = ?");
        var args = new ArrayList<Object>(List.of(access.tenantId(), workspaceId, filterStatus, P16_SERVICE_REQUEST_WORKFLOW_ID));
        if ("assigned".equals(view)) { where.append(" and h.assignee_id = ?"); args.add(actor.actorId()); }
        else { where.append(" and i.actor_id = ?"); args.add(actor.actorId()); }
        if (cursorCreatedAt != null) {
            where.append(" and (h.created_at, h.id) < (?, ?)");
            args.add(Timestamp.from(cursorCreatedAt)); args.add(cursorId);
        }
        args.add(pageSize + 1);
        var keys = jdbc.query("select h.id, h.created_at from workflow.human_work_item h join workflow.instance i "
                        + "on i.id = h.instance_id and i.tenant_id = h.tenant_id and i.workspace_id = h.workspace_id" + where
                        + " order by h.created_at desc, h.id desc limit ?",
                (rs, row) -> new SourceInstanceKey(rs.getObject("id", UUID.class), rs.getTimestamp("created_at").toInstant()),
                args.toArray());
        var hasNext = keys.size() > pageSize;
        var selected = hasNext ? List.copyOf(keys.subList(0, pageSize)) : List.copyOf(keys);
        var items = selected.stream().map(key -> loadHumanWorkItem(access.tenantId(), workspaceId, key.id(), actor)).toList();
        var last = hasNext ? selected.get(selected.size() - 1) : null;
        return new HumanWorkItemPage(items, last == null ? null : last.createdAt(), last == null ? null : last.id());
    }

    @Override
    @Transactional(readOnly = true)
    public WorkflowService.P16DigestSourcePage readMyP16DigestSources(ActorContext actor, UUID workspaceId, int maxItems) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        requireActiveHuman(actor);
        if (maxItems < 1 || maxItems > 20) throw EafException.invalid("P16 摘要来源上限必须为 1—20。");
        var ids = jdbc.query("select h.id from workflow.human_work_item h join workflow.instance i on i.id = h.instance_id "
                        + "where h.tenant_id = ? and h.workspace_id = ? and h.assignee_id = ? and h.status = 'OPEN' "
                        + "and i.workflow_id = ? and i.status = 'WAITING_HUMAN' and i.current_step_id = 'handle' "
                        + "and i.deadline_at > now() order by i.deadline_at asc nulls last, h.created_at asc, h.id asc limit ?",
                (rs, row) -> rs.getObject("id", UUID.class), access.tenantId(), workspaceId, actor.actorId(),
                P16_SERVICE_REQUEST_WORKFLOW_ID, maxItems + 1);
        var hasMore = ids.size() > maxItems;
        var selected = hasMore ? ids.subList(0, maxItems) : ids;
        return new WorkflowService.P16DigestSourcePage(selected.stream()
                .map(id -> digestSource(loadHumanWorkItem(access.tenantId(), workspaceId, id, actor))).toList(), hasMore);
    }

    @Override
    @Transactional(readOnly = true)
    public P16DigestSource readP16DigestSource(ActorContext actor, UUID workspaceId, UUID workItemId) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        requireActiveHuman(actor);
        var item = getHumanWorkItem(actor, workspaceId, workItemId);
        if (!P16_SERVICE_REQUEST_WORKFLOW_VERSION.equals(workflowVersion(access.tenantId(), workspaceId, item.instanceId()))
                || !actor.actorId().equals(item.assigneeId())) throw EafException.notFound();
        return digestSource(item);
    }

    private String workflowVersion(UUID tenantId, UUID workspaceId, UUID instanceId) {
        return jdbc.query("select workflow_version from workflow.instance where id = ? and tenant_id = ? and workspace_id = ? "
                        + "and workflow_id = ?",
                rs -> rs.next() ? rs.getString(1) : null, instanceId, tenantId, workspaceId, P16_SERVICE_REQUEST_WORKFLOW_ID);
    }

    private P16DigestSource digestSource(HumanWorkItem item) {
        return new P16DigestSource(item.id(), item.instanceId(), item.requestId(), item.status(), item.rowVersion(),
                item.assigneeId(), item.deadlineAt(), item.createdAt(), item.sharedBrief(), item.handlingAdvice(), item.cautions());
    }

    @Override
    @Transactional(readOnly = true)
    public HumanWorkItem getHumanWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        requireActiveHuman(actor);
        return loadHumanWorkItem(access.tenantId(), workspaceId, workItemId, actor);
    }

    @Override
    @Transactional
    public ProjectBriefSnapshot createProjectBrief(CreateProjectBriefCommand command) {
        if (command == null) throw EafException.invalid("项目简报创建请求不能为空。");
        requireDirectHuman(command.actor());
        var actor = command.actor();
        var workspaceId = command.workspaceId();
        if (workspaceId == null || command.idempotencyKey() == null || command.idempotencyKey().isBlank()
                || command.idempotencyKey().length() > 128)
            throw EafException.invalid("项目简报创建字段不完整。");
        var access = workspaces.require(actor, workspaceId, "workflow:start");
        workspaces.require(actor, workspaceId, "work-item:read");
        workspaces.require(actor, workspaceId, "work-item:assign");
        requireActiveHuman(actor);
        var title = requireText(command.title(), 120, "title");
        var goal = requireText(command.goal(), 1_000, "goal");
        if (title.isBlank() || goal.isBlank()) throw EafException.invalid("项目主题和整理目标不能为空。");
        var knowledgeRefs = command.knowledgeRefs();
        var workRefs = command.workItemRefs();
        if (knowledgeRefs.isEmpty() || knowledgeRefs.size() > 10 || workRefs.size() > 5
                || (workRefs.isEmpty() && command.oaQueryRef() == null)
                || knowledgeRefs.stream().anyMatch(ref -> ref == null || ref.documentId() == null
                        || ref.documentVersion() < 1 || ref.chunkId() == null || ref.buildId() == null
                        || ref.contentHash() == null || !ref.contentHash().matches("[0-9a-f]{64}"))
                || knowledgeRefs.stream().map(ref -> ref.documentId() + ":" + ref.documentVersion() + ":"
                        + ref.chunkId() + ":" + ref.buildId() + ":" + ref.contentHash()).distinct().count() != knowledgeRefs.size()
                || workRefs.stream().anyMatch(ref -> ref == null || ref.workItemId() == null || ref.expectedRowVersion() < 1)
                || workRefs.stream().map(CreateProjectBriefCommand.WorkItemRef::workItemId).distinct().count() != workRefs.size()
                || command.oaQueryRef() != null && (command.oaQueryRef().queryId() == null
                        || command.oaQueryRef().expectedRowVersion() < 1))
            throw EafException.invalid("项目简报资料引用数量或字段无效。");
        var reviewerId = command.reviewerId() == null ? actor.actorId() : command.reviewerId();
        var recipientId = command.recipientId() == null ? actor.actorId() : command.recipientId();
        requireProjectBriefParticipant(access.tenantId(), workspaceId, reviewerId);
        requireProjectBriefParticipant(access.tenantId(), workspaceId, recipientId);
        if (command.oaQueryRef() != null
                && (!reviewerId.equals(actor.actorId()) || !recipientId.equals(actor.actorId())))
            throw EafException.forbidden("P27 OA 快照仅允许原查询 Owner 参与项目简报。");
        var deadline = command.deadlineAt() == null
                ? jdbc.queryForObject("select now() + interval '24 hours'", Timestamp.class).toInstant()
                : command.deadlineAt().truncatedTo(ChronoUnit.MICROS);
        if (!Boolean.TRUE.equals(jdbc.queryForObject("select ?::timestamptz > now() and ?::timestamptz <= now() + interval '7 days'",
                Boolean.class, Timestamp.from(deadline), Timestamp.from(deadline))))
            throw EafException.invalid("项目简报截止时间必须晚于当前时间且不超过 7 天。");

        var replay = lookupInstanceByIdempotencyKey(actor, workspaceId, P29_PROJECT_BRIEF_WORKFLOW_ID,
                P29_PROJECT_BRIEF_WORKFLOW_VERSION, command.idempotencyKey());
        if (replay.isPresent()) {
            var prior = replay.get();
            if (!P29_PROJECT_BRIEF_WORKFLOW_ID.equals(prior.workflowId()))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "项目简报幂等键已绑定其他流程。");
            requireProjectBriefRequestMatches(prior, command, title, goal, reviewerId, recipientId);
            return projectBriefSnapshot(actor, prior, true);
        }

        var bundle = captureProjectBriefBundle(actor, workspaceId, title, goal, reviewerId, recipientId,
                knowledgeRefs, workRefs, command.oaQueryRef(), access.tenantId());
        var estimatedCodePoints = title.codePointCount(0, title.length()) + goal.codePointCount(0, goal.length());
        var evidence = bundle.path("evidence");
        for (var item : evidence) {
            estimatedCodePoints += item.path("content").asText("").codePointCount(0, item.path("content").asText("").length());
            estimatedCodePoints += item.path("sharedBrief").asText("").codePointCount(0, item.path("sharedBrief").asText("").length());
            estimatedCodePoints += item.path("summary").asText("").codePointCount(0, item.path("summary").asText("").length());
            estimatedCodePoints += item.path("nextAction").asText("").codePointCount(0, item.path("nextAction").asText("").length());
            for (var todo : item.path("todos"))
                estimatedCodePoints += todo.path("title").asText("").codePointCount(0, todo.path("title").asText("").length());
        }
        if (estimatedCodePoints / 4 > 2_000)
            throw EafException.invalid("BRIEF_INPUT_TOO_LARGE: 请减少选择的资料或待办。");
        var bundleJson = canonical(bundle);
        var bundleHash = Hashing.sha256(bundleJson);
        var input = json.createObjectNode().put("title", title).put("goal", goal)
                .put("creatorId", actor.actorId().toString()).put("reviewerId", reviewerId.toString())
                .put("recipientId", recipientId.toString()).put("evidenceBundleJson", bundleJson)
                .put("evidenceBundleHash", bundleHash);
        var request = new CreateWorkflowInstanceCommand(actor, workspaceId, P29_PROJECT_BRIEF_WORKFLOW_ID,
                P29_PROJECT_BRIEF_WORKFLOW_VERSION, canonical(input), command.idempotencyKey(), "USER", deadline);
        WorkflowInstance instance;
        try {
            instance = createInstance(request, null, false, false, false, false, false, true);
        } catch (EafException conflict) {
            if (!"IDEMPOTENCY_CONFLICT".equals(conflict.code())) throw conflict;
            var raced = lookupInstanceByIdempotencyKey(actor, workspaceId, P29_PROJECT_BRIEF_WORKFLOW_ID,
                    P29_PROJECT_BRIEF_WORKFLOW_VERSION, command.idempotencyKey()).orElseThrow(() -> conflict);
            requireProjectBriefRequestMatches(raced, command, title, goal, reviewerId, recipientId);
            requireProjectBriefCurrent(actor, raced);
            return projectBriefSnapshot(actor, raced, false);
        }
        jdbc.update("insert into workflow.project_brief_state(instance_id, tenant_id, workspace_id, evidence_bundle_hash) "
                        + "values (?, ?, ?, ?) on conflict (instance_id) do nothing",
                instance.id(), access.tenantId(), workspaceId, bundleHash);
        audit.append(new AuditFact("p29-project-brief-created:" + instance.id(), access.tenantId(), workspaceId,
                actor.actorId(), instance.id(), "PROJECT_BRIEF_CREATED", "QUEUED", "{}", null));
        return projectBriefSnapshot(actor, instance, false);
    }

    private void requireProjectBriefParticipant(UUID tenantId, UUID workspaceId, UUID actorId) {
        if (actorId == null || !identityDirectory.isActiveHuman(tenantId, actorId)
                || !workspaces.isAuthorized(tenantId, actorId, workspaceId, "workflow:read")
                || !workspaces.isAuthorized(tenantId, actorId, workspaceId, "work-item:read")
                || !workspaces.isAuthorized(tenantId, actorId, workspaceId, "work-item:complete"))
            throw EafException.invalid("项目简报参与人必须是有效且具备读取、处理权限的 Workspace HUMAN。");
    }

    private ActorContext briefActor(UUID tenantId, UUID workspaceId, UUID actorId) {
        return new ActorContext(actorId, tenantId, io.eaf.shared.ActorType.HUMAN,
                workspaces.actions(tenantId, actorId, workspaceId));
    }

    private JsonNode captureProjectBriefBundle(ActorContext creator, UUID workspaceId, String title, String goal,
            UUID reviewerId, UUID recipientId, List<CreateProjectBriefCommand.KnowledgeRef> knowledgeRefs,
            List<CreateProjectBriefCommand.WorkItemRef> workRefs, CreateProjectBriefCommand.OaQueryRef oaRef,
            UUID tenantId) {
        var bundle = json.createObjectNode().put("schemaVersion", P29_EVIDENCE_SCHEMA).put("title", title)
                .put("goal", goal).put("creatorId", creator.actorId().toString())
                .put("reviewerId", reviewerId.toString()).put("recipientId", recipientId.toString());
        bundle.set("knowledgeRefs", json.valueToTree(knowledgeRefs));
        bundle.set("workItemRefs", json.valueToTree(workRefs));
        bundle.set("oaQueryRef", json.valueToTree(oaRef));
        var rows = bundle.putArray("evidence");
        var now = jdbc.queryForObject("select now()", Timestamp.class).toInstant();
        var kRefs = knowledgeRefs.stream().map(ref -> new PublishedKnowledgeChunk.Ref(ref.documentId(),
                ref.documentVersion(), ref.chunkId(), ref.buildId(), ref.contentHash())).toList();
        var sources = List.of(creator.actorId(), reviewerId, recipientId).stream().distinct().map(id -> briefActor(tenantId, workspaceId, id)).toList();
        var byKey = new HashMap<String, PublishedKnowledgeChunk>();
        for (var sourceActor : sources) for (var chunk : knowledge.readPublishedChunks(sourceActor, workspaceId, kRefs))
            byKey.put(briefKnowledgeKey(chunk.ref()), chunk);
        for (int i = 0; i < kRefs.size(); i++) {
            var ref = kRefs.get(i);
            var chunk = byKey.get(briefKnowledgeKey(ref));
            if (chunk == null) throw EafException.conflict("BRIEF_SOURCE_UNAVAILABLE", "所选知识资料当前不可读取。");
            var item = rows.addObject().put("evidenceId", "K" + (i + 1)).put("sourceType", "KNOWLEDGE")
                    .put("documentId", ref.documentId().toString()).put("documentVersion", ref.documentVersion())
                    .put("chunkId", ref.chunkId().toString()).put("buildId", ref.buildId().toString())
                    .put("contentHash", ref.contentHash()).put("documentTitle", chunk.documentTitle())
                    .put("content", chunk.content()).put("capturedAt", now.toString());
            item.set("headingPath", json.valueToTree(chunk.headingPath()));
            item.put("snapshotHash", briefKnowledgeHash(chunk));
        }
        for (int i = 0; i < workRefs.size(); i++) {
            var ref = workRefs.get(i);
            var item = readBriefWorkItem(sources, workspaceId, ref.workItemId());
            if (item.rowVersion() != ref.expectedRowVersion())
                throw EafException.conflict("BRIEF_SOURCE_CHANGED", "所选人工工作项版本已变化。");
            rows.add(briefWorkItemEvidence(i + 1, item, now));
        }
        if (oaRef != null) rows.add(briefOaEvidence(creator, workspaceId, oaRef, now));
        return bundle;
    }

    private String briefKnowledgeKey(PublishedKnowledgeChunk.Ref ref) {
        return ref.documentId() + ":" + ref.documentVersion() + ":" + ref.chunkId() + ":" + ref.buildId() + ":" + ref.contentHash();
    }

    private String briefKnowledgeHash(PublishedKnowledgeChunk chunk) {
        var stable = json.createObjectNode().put("documentId", chunk.ref().documentId().toString())
                .put("documentVersion", chunk.ref().documentVersion()).put("chunkId", chunk.ref().chunkId().toString())
                .put("buildId", chunk.ref().buildId().toString()).put("contentHash", chunk.ref().contentHash())
                .put("documentTitle", chunk.documentTitle()).put("content", chunk.content());
        stable.set("headingPath", json.valueToTree(chunk.headingPath()));
        return Hashing.sha256(canonical(stable));
    }

    private HumanWorkItem readBriefWorkItem(List<ActorContext> actors, UUID workspaceId, UUID workItemId) {
        HumanWorkItem selected = null;
        for (var sourceActor : actors) {
            var item = getHumanWorkItem(sourceActor, workspaceId, workItemId);
            if (selected == null) selected = item;
            else if (!briefHumanHash(selected).equals(briefHumanHash(item)))
                throw EafException.conflict("BRIEF_SOURCE_CHANGED", "所选人工工作项读取结果不一致。");
        }
        if (selected == null) throw EafException.notFound();
        return selected;
    }

    private String briefHumanHash(HumanWorkItem item) {
        var stable = json.createObjectNode().put("id", item.id().toString()).put("instanceId", item.instanceId().toString())
                .put("rowVersion", item.rowVersion()).put("status", item.status())
                .put("sharedBrief", item.sharedBrief() == null ? "" : item.sharedBrief())
                .put("handlingAdvice", item.handlingAdvice() == null ? "" : item.handlingAdvice())
                .put("cautions", item.cautions() == null ? "" : item.cautions())
                .put("outcome", item.outcome() == null ? "" : item.outcome())
                .put("summary", item.summary() == null ? "" : item.summary())
                .put("nextAction", item.nextAction() == null ? "" : item.nextAction())
                .put("assigneeId", item.assigneeId().toString());
        if (item.completedBy() != null) stable.put("completedBy", item.completedBy().toString());
        if (item.completedAt() != null) stable.put("completedAt", item.completedAt().toString());
        return Hashing.sha256(canonical(stable));
    }

    private com.fasterxml.jackson.databind.node.ObjectNode briefWorkItemEvidence(int ordinal, HumanWorkItem source, Instant capturedAt) {
        return json.createObjectNode().put("evidenceId", "W" + ordinal).put("sourceType", "P16_WORK_ITEM")
                .put("workItemId", source.id().toString()).put("instanceId", source.instanceId().toString())
                .put("rowVersion", source.rowVersion()).put("status", source.status())
                .put("assigneeId", source.assigneeId().toString()).put("sharedBrief", source.sharedBrief() == null ? "" : source.sharedBrief())
                .put("handlingAdvice", source.handlingAdvice() == null ? "" : source.handlingAdvice())
                .put("cautions", source.cautions() == null ? "" : source.cautions())
                .put("outcome", source.outcome() == null ? "" : source.outcome())
                .put("summary", source.summary() == null ? "" : source.summary())
                .put("nextAction", source.nextAction() == null ? "" : source.nextAction())
                .put("completedBy", source.completedBy() == null ? "" : source.completedBy().toString())
                .put("completedAt", source.completedAt() == null ? "" : source.completedAt().toString())
                .put("capturedAt", capturedAt.toString()).put("snapshotHash", briefHumanHash(source));
    }

    private com.fasterxml.jackson.databind.node.ObjectNode briefOaEvidence(ActorContext creator, UUID workspaceId,
            CreateProjectBriefCommand.OaQueryRef ref, Instant capturedAt) {
        var isOaTodoQuery = jdbc.queryForObject("select count(*) from workflow.instance where id = ? and tenant_id = ? "
                        + "and workspace_id = ? and workflow_id in (?, ?)", Integer.class,
                ref.queryId(), creator.tenantId(), workspaceId, P27_OA_LIST_WORKFLOW_ID, P27_OA_ITEM_WORKFLOW_ID);
        if (isOaTodoQuery == null || isOaTodoQuery != 1)
            throw EafException.notFound();
        var query = getQuery(creator, workspaceId, ref.queryId());
        if (!"SUCCEEDED".equals(query.status()) || query.rowVersion() != ref.expectedRowVersion()
                || query.result() == null || !query.result().path("page").path("items").isArray()
                || query.result().path("page").path("items").size() > 20)
            throw EafException.conflict("BRIEF_SOURCE_UNAVAILABLE", "OA 查询必须是本人当前可读的成功查询且最多包含 20 项。");
        var resultHash = Hashing.sha256(canonical(query.result()));
        return json.createObjectNode().put("evidenceId", "OA1").put("sourceType", "P27_OA_QUERY")
                .put("queryId", query.id().toString()).put("taskId", query.taskId() == null ? "" : query.taskId().toString())
                .put("rowVersion", query.rowVersion()).put("queriedAt", query.updatedAt().toString())
                .put("resultHash", resultHash).put("capturedAt", capturedAt.toString())
                .set("todos", query.result().path("page").path("items").deepCopy());
    }

    private void requireProjectBriefRequestMatches(WorkflowInstance instance, CreateProjectBriefCommand command,
            String title, String goal, UUID reviewerId, UUID recipientId) {
        var input = parseJson(instance.inputJson(), "projectBrief.input");
        var bundle = parseJson(input.path("evidenceBundleJson").asText(), "projectBrief.evidence");
        var sameRefs = canonical(bundle.path("knowledgeRefs")).equals(canonical(json.valueToTree(command.knowledgeRefs())))
                && canonical(bundle.path("workItemRefs")).equals(canonical(json.valueToTree(command.workItemRefs())))
                && canonical(bundle.path("oaQueryRef")).equals(canonical(json.valueToTree(command.oaQueryRef())));
        if (!title.equals(input.path("title").asText()) || !goal.equals(input.path("goal").asText())
                || !command.actor().actorId().toString().equals(input.path("creatorId").asText())
                || !reviewerId.toString().equals(input.path("reviewerId").asText())
                || !recipientId.toString().equals(input.path("recipientId").asText()) || !sameRefs
                || command.deadlineAt() != null && !instance.deadlineAt().equals(command.deadlineAt().truncatedTo(ChronoUnit.MICROS)))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一项目简报幂等键对应了不同输入。");
    }

    private void requireProjectBriefCurrent(ActorContext requester, WorkflowInstance instance) {
        requireProjectBriefCurrent(requester, instance, new UUID[0]);
    }

    private void requireProjectBriefCurrent(ActorContext requester, WorkflowInstance instance, UUID... additionalActors) {
        if (!P29_PROJECT_BRIEF_WORKFLOW_ID.equals(instance.workflowId())
                || !P29_PROJECT_BRIEF_WORKFLOW_VERSION.equals(instance.workflowVersion())
                || !"USER".equals(instance.source()) || instance.qualityRunId() != null)
            throw EafException.notFound();
        var input = parseJson(instance.inputJson(), "projectBrief.input");
        var bundle = parseJson(input.path("evidenceBundleJson").asText(), "projectBrief.evidence");
        var expectedHash = input.path("evidenceBundleHash").asText();
        if (!expectedHash.equals(Hashing.sha256(canonical(bundle))))
            throw EafException.conflict("BRIEF_SOURCE_CHANGED", "项目简报的固定证据包摘要不匹配。");
        var tenantId = instance.tenantId();
        var workspaceId = instance.workspaceId();
        var currentAssignees = jdbc.query("select assignee_id from workflow.human_work_item where instance_id = ?",
                (rs, row) -> rs.getObject(1, UUID.class), instance.id());
        var actors = java.util.stream.Stream.concat(java.util.stream.Stream.of(instance.actorId(),
                        UUID.fromString(input.path("reviewerId").asText()), UUID.fromString(input.path("recipientId").asText())),
                        java.util.stream.Stream.concat(currentAssignees.stream(), java.util.Arrays.stream(additionalActors)))
                .distinct()
                .map(id -> briefActor(tenantId, workspaceId, id)).toList();
        actors.forEach(actor -> {
            requireProjectBriefParticipant(tenantId, workspaceId, actor.actorId());
            workspaces.require(actor, workspaceId, "work-item:read");
        });
        var knowledgeSourceRefs = new ArrayList<PublishedKnowledgeChunk.Ref>();
        for (var source : bundle.path("evidence")) if ("KNOWLEDGE".equals(source.path("sourceType").asText()))
            knowledgeSourceRefs.add(new PublishedKnowledgeChunk.Ref(UUID.fromString(source.path("documentId").asText()),
                    source.path("documentVersion").asInt(), UUID.fromString(source.path("chunkId").asText()),
                    UUID.fromString(source.path("buildId").asText()), source.path("contentHash").asText()));
        for (var actor : actors) {
            var current = new HashMap<String, PublishedKnowledgeChunk>();
            for (var chunk : knowledge.readPublishedChunks(actor, workspaceId, knowledgeSourceRefs))
                current.put(briefKnowledgeKey(chunk.ref()), chunk);
            for (var source : bundle.path("evidence")) if ("KNOWLEDGE".equals(source.path("sourceType").asText())) {
                var ref = new PublishedKnowledgeChunk.Ref(UUID.fromString(source.path("documentId").asText()),
                        source.path("documentVersion").asInt(), UUID.fromString(source.path("chunkId").asText()),
                        UUID.fromString(source.path("buildId").asText()), source.path("contentHash").asText());
                var chunk = current.get(briefKnowledgeKey(ref));
                if (chunk == null || !source.path("snapshotHash").asText().equals(briefKnowledgeHash(chunk)))
                    throw EafException.conflict("BRIEF_SOURCE_UNAVAILABLE", "项目简报引用的知识资料已变化或不可读。");
            }
        }
        var workItemIds = new ArrayList<UUID>();
        for (var ref : bundle.path("workItemRefs")) workItemIds.add(UUID.fromString(ref.path("workItemId").asText()));
        for (var id : workItemIds) {
            var current = readBriefWorkItem(actors, workspaceId, id);
            var source = java.util.stream.StreamSupport.stream(bundle.path("evidence").spliterator(), false)
                    .filter(item -> "P16_WORK_ITEM".equals(item.path("sourceType").asText())
                            && id.toString().equals(item.path("workItemId").asText())).findFirst().orElse(null);
            if (source == null || current.rowVersion() != source.path("rowVersion").asLong()
                    || !source.path("snapshotHash").asText().equals(briefHumanHash(current)))
                throw EafException.conflict("BRIEF_SOURCE_UNAVAILABLE", "项目简报引用的 P16 工作项已变化或不可读。");
        }
        for (var source : bundle.path("evidence")) if ("P27_OA_QUERY".equals(source.path("sourceType").asText())) {
            if (!requester.actorId().equals(instance.actorId())) throw EafException.notFound();
            var query = getQuery(requester, workspaceId, UUID.fromString(source.path("queryId").asText()));
            if (!"SUCCEEDED".equals(query.status()) || query.rowVersion() != source.path("rowVersion").asLong()
                    || query.result() == null || !source.path("resultHash").asText().equals(Hashing.sha256(canonical(query.result()))))
                throw EafException.conflict("BRIEF_SOURCE_UNAVAILABLE", "项目简报引用的 OA 查询已变化或不可读。");
        }
    }

    @Override
    @Transactional(readOnly = true)
    public ProjectBriefSnapshot getProjectBrief(ActorContext actor, UUID workspaceId, UUID briefId) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "workflow:read");
        requireActiveHuman(actor);
        var instance = loadInstance(access.tenantId(), workspaceId, briefId);
        if (!P29_PROJECT_BRIEF_WORKFLOW_ID.equals(instance.workflowId())
                || !isProjectBriefReader(instance, actor.actorId())) throw EafException.notFound();
        return projectBriefSnapshot(actor, instance, true);
    }

    @Override
    @Transactional
    public ProjectBriefSnapshot cancelProjectBrief(ActorContext actor, UUID workspaceId, UUID briefId,
            long expectedVersion) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "workflow:write");
        requireActiveHuman(actor);
        var instance = loadInstance(access.tenantId(), workspaceId, briefId);
        if (!P29_PROJECT_BRIEF_WORKFLOW_ID.equals(instance.workflowId()) || !actor.actorId().equals(instance.actorId()))
            throw EafException.notFound();
        return projectBriefSnapshot(actor, cancelInstance(actor, workspaceId, briefId, expectedVersion), true);
    }

    private boolean isProjectBriefParticipant(WorkflowInstance instance, UUID actorId) {
        var input = parseJson(instance.inputJson(), "projectBrief.input");
        return actorId.equals(instance.actorId()) || actorId.toString().equals(input.path("reviewerId").asText())
                || actorId.toString().equals(input.path("recipientId").asText());
    }

    private boolean isProjectBriefReader(WorkflowInstance instance, UUID actorId) {
        return isProjectBriefParticipant(instance, actorId) || Boolean.TRUE.equals(jdbc.queryForObject(
                "select exists(select 1 from workflow.human_work_item where instance_id = ? and assignee_id = ?)",
                Boolean.class, instance.id(), actorId));
    }

    private ProjectBriefSnapshot projectBriefSnapshot(ActorContext actor, WorkflowInstance instance,
            boolean checkSources) {
        var input = parseJson(instance.inputJson(), "projectBrief.input");
        var bundle = parseJson(input.path("evidenceBundleJson").asText(), "projectBrief.evidence");
        var versions = jdbc.query("select artifact_version from workflow.project_brief_artifact where instance_id = ? order by artifact_version",
                (rs, row) -> rs.getInt(1), instance.id());
        var taskId = jdbc.query("select child_task_id from workflow.step where instance_id = ? and step_id = 'prepare'",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, instance.id());
        var allowed = new java.util.LinkedHashSet<String>();
        allowed.add("read");
        var blocked = false;
        if (checkSources) {
            try { requireProjectBriefCurrent(actor, instance); }
            catch (EafException unavailable) { blocked = true; }
        }
        if (!blocked) {
            if (!versions.isEmpty()) allowed.add("readArtifact");
            if (actor.actorId().equals(instance.actorId())
                    && workspaces.isAuthorized(instance.tenantId(), actor.actorId(), instance.workspaceId(), "workflow:write")
                    && !Set.of("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT").contains(instance.status()))
                allowed.add("cancel");
        } else if (actor.actorId().equals(instance.actorId())
                && workspaces.isAuthorized(instance.tenantId(), actor.actorId(), instance.workspaceId(), "workflow:write")
                && !Set.of("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT").contains(instance.status())) {
            allowed.add("cancel");
        }
        return new ProjectBriefSnapshot(instance.id(), instance.id(), blocked ? null : input.path("title").asText(),
                instance.actorId(), UUID.fromString(input.path("reviewerId").asText()),
                UUID.fromString(input.path("recipientId").asText()), instance.status(), instance.currentStepId(),
                instance.waitingReason(), instance.rowVersion(), instance.createdAt(), instance.deadlineAt(),
                taskId, versions, blocked, allowed);
    }

    @Override
    @Transactional(readOnly = true)
    public ProjectBriefArtifact getProjectBriefArtifact(ActorContext actor, UUID workspaceId, UUID briefId,
            int version) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "workflow:read");
        requireActiveHuman(actor);
        if (version < 1 || version > 2) throw EafException.notFound();
        var instance = loadInstance(access.tenantId(), workspaceId, briefId);
        if (!P29_PROJECT_BRIEF_WORKFLOW_ID.equals(instance.workflowId())
                || !isProjectBriefReader(instance, actor.actorId())) throw EafException.notFound();
        var row = jdbc.query("select artifact_kind, template_version, generation_task_id, generation_attempt, "
                        + "content_hash, created_at, markdown from workflow.project_brief_artifact "
                        + "where instance_id = ? and tenant_id = ? and workspace_id = ? and artifact_version = ?",
                rs -> rs.next() ? new BriefArtifactRow(rs.getString("artifact_kind"), rs.getString("template_version"),
                        rs.getObject("generation_task_id", UUID.class), rs.getInt("generation_attempt"),
                        rs.getString("content_hash"), rs.getTimestamp("created_at").toInstant(), rs.getString("markdown")) : null,
                briefId, access.tenantId(), workspaceId, version);
        if (row == null) throw EafException.notFound();
        try { requireProjectBriefCurrent(actor, instance); }
        catch (EafException unavailable) {
            return new ProjectBriefArtifact(briefId, version, row.kind(), row.templateVersion(), row.taskId(),
                    row.attempt(), null, row.createdAt(), true, null);
        }
        if (!Hashing.sha256(row.markdown()).equals(row.contentHash()))
            throw EafException.conflict("BRIEF_ARTIFACT_INVALID", "简报成果摘要校验失败。");
        return new ProjectBriefArtifact(briefId, version, row.kind(), row.templateVersion(), row.taskId(),
                row.attempt(), row.contentHash(), row.createdAt(), false, row.markdown());
    }

    private record BriefArtifactRow(String kind, String templateVersion, UUID taskId, int attempt,
            String contentHash, Instant createdAt, String markdown) { }

    @Override
    @Transactional(readOnly = true)
    public ProjectBriefTaskSource requireProjectBriefTaskSource(ActorContext actor, UUID workspaceId, UUID taskId) {
        requireDirectHuman(actor);
        workspaces.require(actor, workspaceId, "workflow:start");
        requireActiveHuman(actor);
        var row = jdbc.query("select i.id, i.tenant_id, i.workspace_id, i.actor_id, i.workflow_id, i.workflow_version, "
                        + "i.source, i.quality_run_id, i.status, i.current_step_id, s.status step_status, i.input_json::text input_json, "
                        + "b.generation_attempted, b.generation_task_id, b.generation_attempt, b.prepared_result_json::text prepared_result_json "
                        + "from workflow.step s join workflow.instance i on i.id = s.instance_id "
                        + "join workflow.project_brief_state b on b.instance_id = i.id "
                        + "where s.child_task_id = ? and s.step_id = 'prepare' and i.tenant_id = ? and i.workspace_id = ?",
                rs -> rs.next() ? new BriefTaskRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("actor_id", UUID.class),
                        rs.getObject("workflow_id", UUID.class), rs.getString("workflow_version"), rs.getString("source"),
                        rs.getObject("quality_run_id", UUID.class), rs.getString("status"), rs.getString("current_step_id"),
                        rs.getString("step_status"), rs.getString("input_json"), rs.getBoolean("generation_attempted"),
                        rs.getObject("generation_task_id", UUID.class), rs.getObject("generation_attempt", Integer.class),
                        rs.getString("prepared_result_json")) : null,
                taskId, actor.tenantId(), workspaceId);
        if (row == null || !P29_PROJECT_BRIEF_WORKFLOW_ID.equals(row.workflowId())
                || !P29_PROJECT_BRIEF_WORKFLOW_VERSION.equals(row.workflowVersion())
                || !actor.actorId().equals(row.actorId()) || !"USER".equals(row.source()) || row.qualityRunId() != null
                || !"WAITING_CHILD".equals(row.status()) || !"prepare".equals(row.currentStep())
                || !"WAITING_CHILD".equals(row.stepStatus()))
            throw EafException.forbidden("Task 只能通过本人当前项目简报 prepare 步骤读取证据。");
        var instance = loadInstance(row.tenantId(), row.workspaceId(), row.instanceId());
        requireProjectBriefCurrent(actor, instance);
        var input = parseJson(row.inputJson(), "projectBrief.input");
        var bundleJson = input.path("evidenceBundleJson").asText(null);
        var bundleHash = input.path("evidenceBundleHash").asText(null);
        if (bundleJson == null || bundleHash == null || !bundleHash.equals(Hashing.sha256(bundleJson)))
            throw EafException.conflict("BRIEF_SOURCE_CHANGED", "项目简报证据包校验失败。");
        return new ProjectBriefTaskSource(row.instanceId(), bundleJson, bundleHash,
                row.generationAttempted(), row.preparedResultJson());
    }

    private record BriefTaskRow(UUID instanceId, UUID tenantId, UUID workspaceId, UUID actorId, UUID workflowId,
            String workflowVersion, String source, UUID qualityRunId, String status, String currentStep,
            String stepStatus, String inputJson, boolean generationAttempted, UUID generationTaskId,
            Integer generationAttempt, String preparedResultJson) { }

    @Override
    @Transactional(readOnly = true)
    public void requireTaskCreation(CreateWorkflowTaskCommand command) {
        if (command == null || command.actor() == null || command.workflowProvenance() == null
                || !P29_PROJECT_BRIEF_WORKFLOW_ID.equals(command.workflowProvenance().workflowId())
                || !P29_PROJECT_BRIEF_WORKFLOW_VERSION.equals(command.workflowProvenance().workflowVersion())
                || !"prepare".equals(command.workflowProvenance().stepId()))
            throw EafException.forbidden("Task 只允许通过固定 P29 prepare 步骤创建。");
        requireDirectHuman(command.actor());
        var access = workspaces.require(command.actor(), command.workspaceId(), "workflow:start");
        requireActiveHuman(command.actor());
        var row = jdbc.query("select i.id, i.root_budget_scope_id, i.actor_id, i.status, i.current_step_id, "
                        + "i.input_json::text workflow_input, s.status step_status, s.input_json::text step_input, s.dispatch_key "
                        + "from workflow.instance i join workflow.step s on s.instance_id = i.id "
                        + "where i.id = ? and i.tenant_id = ? and i.workspace_id = ? and s.step_id = 'prepare'",
                rs -> rs.next() ? new Object[]{rs.getObject("id", UUID.class), rs.getObject("root_budget_scope_id", UUID.class),
                        rs.getObject("actor_id", UUID.class), rs.getString("status"), rs.getString("current_step_id"),
                        rs.getString("workflow_input"), rs.getString("step_status"), rs.getString("step_input"),
                        rs.getString("dispatch_key")} : null,
                command.workflowProvenance().workflowInstanceId(), access.tenantId(), command.workspaceId());
        if (row == null || !command.actor().actorId().equals(row[2]) || !"RUNNING".equals(row[3])
                || !"prepare".equals(row[4]) || !"INTENT".equals(row[6])
                || !command.budgetScopeId().equals(row[1])
                || !(command.workflowProvenance().workflowInstanceId() + ":prepare").equals(row[8])
                || !row[8].equals(command.dispatchKey())
                || !canonical(parseJson(command.input(), "projectBrief.taskInput"))
                        .equals(canonical(parseJson((String) row[7], "projectBrief.stepInput"))))
            throw EafException.forbidden("Task 输入、预算或步骤来源与固定项目简报流程不匹配。");
        var instance = loadInstance(access.tenantId(), command.workspaceId(), command.workflowProvenance().workflowInstanceId());
        requireProjectBriefCurrent(command.actor(), instance);
    }

    @Override
    @Transactional(readOnly = true)
    public void requireTaskResultCurrent(ActorContext actor, UUID workspaceId, UUID taskId, int attempt) {
        requireDirectHuman(actor);
        workspaces.require(actor, workspaceId, "workflow:read");
        requireActiveHuman(actor);
        var row = jdbc.query("select i.id, i.tenant_id, i.workspace_id, i.actor_id, i.workflow_id, i.workflow_version, "
                        + "i.source, i.quality_run_id from workflow.step s join workflow.instance i on i.id = s.instance_id "
                        + "where s.child_task_id = ? and s.step_id = 'prepare' and i.tenant_id = ? and i.workspace_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("actor_id", UUID.class),
                        rs.getObject("workflow_id", UUID.class), rs.getString("workflow_version"),
                        rs.getString("source"), rs.getObject("quality_run_id", UUID.class)} : null,
                taskId, actor.tenantId(), workspaceId);
        if (row == null || !P29_PROJECT_BRIEF_WORKFLOW_ID.equals(row[4])
                || !P29_PROJECT_BRIEF_WORKFLOW_VERSION.equals(row[5]) || !actor.actorId().equals(row[3])
                || !"USER".equals(row[6]) || row[7] != null) throw EafException.notFound();
        var instance = loadInstance(actor.tenantId(), workspaceId, (UUID) row[0]);
        requireProjectBriefCurrent(actor, instance);
    }

    @Override
    @Transactional
    public boolean beginProjectBriefGeneration(ActorContext actor, UUID workspaceId, UUID taskId, int attempt) {
        var source = requireProjectBriefTaskSource(actor, workspaceId, taskId);
        if (attempt != 1) throw EafException.forbidden("项目简报 prepare 只允许首次 Task attempt 发起模型调用。");
        var changed = jdbc.update("update workflow.project_brief_state set generation_attempted = true, "
                        + "generation_task_id = ?, generation_attempt = ? where instance_id = ? "
                        + "and generation_attempted = false and evidence_bundle_hash = ?",
                taskId, attempt, source.briefId(), source.evidenceBundleHash());
        return changed == 1;
    }

    @Override
    @Transactional
    public String recordProjectBriefGenerationResult(ActorContext actor, UUID workspaceId, UUID taskId, int attempt,
            String resultJson) {
        var source = requireProjectBriefTaskSource(actor, workspaceId, taskId);
        var normalized = validateProjectBriefResult(source, resultJson);
        var row = jdbc.query("select generation_attempted, generation_task_id, generation_attempt, "
                        + "prepared_result_json::text prepared_result_json from workflow.project_brief_state "
                        + "where instance_id = ? for update",
                rs -> rs.next() ? new BriefGenerationRow(rs.getBoolean("generation_attempted"),
                        rs.getObject("generation_task_id", UUID.class), rs.getObject("generation_attempt", Integer.class),
                        rs.getString("prepared_result_json")) : null, source.briefId());
        if (row == null || !row.attempted() || !taskId.equals(row.taskId()) || !Integer.valueOf(attempt).equals(row.attempt()))
            throw EafException.conflict("BRIEF_GENERATION_BINDING_INVALID", "生成结果与唯一模型尝试不匹配。");
        if (row.resultJson() != null) {
            if (!canonical(parseJson(row.resultJson(), "brief.preparedResult")).equals(normalized))
                throw EafException.conflict("BRIEF_GENERATION_RESULT_CONFLICT", "已有生成结果不能被替换。");
            return normalized;
        }
        var instance = loadInstance(actor.tenantId(), workspaceId, source.briefId());
        var bundle = parseJson(source.evidenceBundleJson(), "projectBrief.evidence");
        var markdown = renderProjectBriefV1(instance, bundle, parseJson(normalized, "brief.result"));
        var contentHash = Hashing.sha256(markdown);
        var inserted = jdbc.update("insert into workflow.project_brief_artifact(instance_id, tenant_id, workspace_id, "
                        + "artifact_version, artifact_kind, template_version, generation_task_id, generation_attempt, "
                        + "evidence_bundle_hash, agent_id, agent_version, capability_id, capability_version, author_id, markdown, content_hash) "
                        + "values (?, ?, ?, 1, 'GENERATED', 'PROJECT_BRIEF_MARKDOWN_V1', ?, ?, ?, ?, '1.0.0', ?, '1.0.0', ?, ?, ?) on conflict do nothing",
                source.briefId(), actor.tenantId(), workspaceId, taskId, attempt, source.evidenceBundleHash(),
                P29_PROJECT_BRIEF_AGENT_ID, P29_PROJECT_BRIEF_CAPABILITY_ID, actor.actorId(), markdown, contentHash);
        if (inserted == 0) {
            var existing = jdbc.queryForObject("select content_hash from workflow.project_brief_artifact where instance_id = ? and artifact_version = 1",
                    String.class, source.briefId());
            if (!contentHash.equals(existing)) throw EafException.conflict("BRIEF_ARTIFACT_CONFLICT", "简报 v1 已由不同结果生成。");
        }
        var changed = jdbc.update("update workflow.project_brief_state set prepared_result_json = ?::jsonb "
                        + "where instance_id = ? and generation_task_id = ? and generation_attempt = ? and prepared_result_json is null",
                normalized, source.briefId(), taskId, attempt);
        if (changed == 0 && row.resultJson() == null)
            throw EafException.conflict("BRIEF_GENERATION_RESULT_CONFLICT", "简报生成结果无法原子保存。");
        return normalized;
    }

    private record BriefGenerationRow(boolean attempted, UUID taskId, Integer attempt, String resultJson) { }

    private String validateProjectBriefResult(ProjectBriefTaskSource source, String resultJson) {
        var root = parseJson(resultJson, "brief.result");
        if (!root.isObject() || !Set.of("overview", "attentionItems", "citations").equals(fieldSet(root))
                || !validBriefText(root.path("overview"), 1, 1_500)
                || !root.path("attentionItems").isArray() || root.path("attentionItems").size() > 8
                || !root.path("citations").isArray() || root.path("citations").size() > 40)
            throw EafException.invalid("项目简报生成结果格式或长度无效。");
        var bundle = parseJson(source.evidenceBundleJson(), "projectBrief.evidence");
        var known = new HashSet<String>();
        bundle.path("evidence").forEach(item -> known.add(item.path("evidenceId").asText()));
        var cited = new HashSet<String>();
        for (var citation : root.path("citations")) {
            var id = citation.path("evidenceId").asText(null);
            if (!Set.of("evidenceId", "reason").equals(fieldSet(citation)) || id == null || !known.contains(id)
                    || !cited.add(id) || !validBriefText(citation.path("reason"), 0, 240))
                throw EafException.invalid("项目简报包含无效或未知来源引用。");
        }
        for (var item : root.path("attentionItems")) {
            var ids = item.path("evidenceIds");
            if (!Set.of("text", "evidenceIds").equals(fieldSet(item)) || !validBriefText(item.path("text"), 1, 500)
                    || !ids.isArray() || ids.isEmpty() || ids.size() > 10)
                throw EafException.invalid("项目简报关注事项无效。");
            var unique = new HashSet<String>();
            for (var id : ids) if (!id.isTextual() || !known.contains(id.asText()) || !unique.add(id.asText())
                    || !cited.contains(id.asText()))
                throw EafException.invalid("项目简报关注事项引用未验证的来源。");
        }
        return canonical(root);
    }

    private Set<String> fieldSet(JsonNode node) {
        var result = new HashSet<String>();
        if (node != null && node.isObject()) node.fieldNames().forEachRemaining(result::add);
        return result;
    }

    private boolean validBriefText(JsonNode node, int min, int max) {
        return node != null && node.isTextual() && node.asText().length() >= min && node.asText().length() <= max;
    }

    private String renderProjectBriefV1(WorkflowInstance instance, JsonNode bundle, JsonNode result) {
        var out = new StringBuilder("# ").append(markdownText(bundle.path("title").asText())).append("\n\n")
                .append("整理时间：").append(instance.createdAt()).append("\n\n")
                .append("## 整理目标\n\n").append(markdownText(bundle.path("goal").asText())).append("\n\n")
                .append("## 来源事实\n\n");
        for (var source : bundle.path("evidence")) {
            var id = source.path("evidenceId").asText();
            if ("KNOWLEDGE".equals(source.path("sourceType").asText())) {
                out.append("- [").append(id).append("] 知识资料：").append(markdownText(source.path("documentTitle").asText()))
                        .append("（版本 ").append(source.path("documentVersion").asInt()).append("，读取于 ")
                        .append(markdownText(source.path("capturedAt").asText())).append("）\n  ")
                        .append(markdownText(String.join(" / ", json.convertValue(source.path("headingPath"),
                                new TypeReference<List<String>>() { })))).append("\n  ")
                        .append(markdownText(source.path("content").asText())).append("\n");
            } else if ("P16_WORK_ITEM".equals(source.path("sourceType").asText())) {
                out.append("- [").append(id).append("] P16 人工工作项：状态 ")
                        .append(markdownText(source.path("status").asText())).append("，共享说明：")
                        .append(markdownText(source.path("sharedBrief").asText()));
                if (!source.path("summary").asText().isBlank()) out.append("；人工记录：").append(markdownText(source.path("summary").asText()));
                if (!source.path("nextAction").asText().isBlank()) out.append("；后续行动：").append(markdownText(source.path("nextAction").asText()));
                out.append("\n");
            } else if ("P27_OA_QUERY".equals(source.path("sourceType").asText())) {
                out.append("- [OA1] 本人 OA 查询，原查询时间：").append(markdownText(source.path("queriedAt").asText())).append("\n");
                for (var todo : source.path("todos")) out.append("  - ").append(markdownText(todo.path("title").asText()))
                        .append("（").append(markdownText(todo.path("status").asText())).append("）\n");
            }
        }
        out.append("\n## 分析建议\n\n").append(markdownText(result.path("overview").asText())).append("\n\n## 关注事项\n\n");
        if (result.path("attentionItems").isEmpty()) out.append("- 暂无结构化关注事项。\n");
        for (var item : result.path("attentionItems")) out.append("- ").append(markdownText(item.path("text").asText()))
                .append("（证据：").append(joinJsonStrings(item.path("evidenceIds"))).append("）\n");
        out.append("\n## 引用目录\n\n");
        for (var item : result.path("citations")) out.append("- [").append(markdownText(item.path("evidenceId").asText()))
                .append("] ").append(markdownText(item.path("reason").asText())).append("\n");
        out.append("\n## 人工确认\n\nreview 与 handoff 尚待处理；分析建议不是来源事实或业务完成证明。\n");
        return requireBriefMarkdown(out.toString());
    }

    private String joinJsonStrings(JsonNode values) {
        var result = new ArrayList<String>();
        values.forEach(value -> result.add(markdownText(value.asText())));
        return String.join(", ", result);
    }

    private String markdownText(String value) {
        if (value == null) return "";
        var plain = value.replaceAll("[\\r\\n\\t]+", " ").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
        return plain.replaceAll("([\\\\`*_{}\\[\\]()#+.!|>-])", "\\\\$1");
    }

    private String requireBriefMarkdown(String markdown) {
        if (markdown.getBytes(StandardCharsets.UTF_8).length > 65_536)
            throw EafException.invalid("BRIEF_ARTIFACT_TOO_LARGE: 简报超过 64 KiB。");
        return markdown;
    }

    @Override
    @Transactional(readOnly = true)
    public ProjectBriefWorkItem getProjectBriefWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        requireActiveHuman(actor);
        var item = briefWorkItemRow(access.tenantId(), workspaceId, workItemId);
        if (item == null || !P29_PROJECT_BRIEF_WORKFLOW_ID.equals(item.workflowId())
                || !isProjectBriefParticipant(item.instance(), actor.actorId())
                && !actor.actorId().equals(item.assigneeId())) throw EafException.notFound();
        var blocked = false;
        try { requireProjectBriefCurrent(actor, item.instance()); }
        catch (EafException unavailable) { blocked = true; }
        var allowed = new java.util.LinkedHashSet<String>();
        allowed.add("read");
        var actionable = !blocked && "OPEN".equals(item.status()) && "WAITING_HUMAN".equals(item.workflowStatus())
                && item.deadlineAt().isAfter(Instant.now());
        if (actionable && actor.actorId().equals(item.creatorId())
                && workspaces.isAuthorized(item.tenantId(), actor.actorId(), workspaceId, "work-item:assign")) allowed.add("assign");
        if (actionable && actor.actorId().equals(item.assigneeId())
                && workspaces.isAuthorized(item.tenantId(), actor.actorId(), workspaceId, "work-item:complete"))
            allowed.add("review".equals(item.stepId()) ? "review" : "receive");
        var result = item.itemResult() == null ? json.createObjectNode() : parseJson(item.itemResult(), "brief.workItem.result");
        return new ProjectBriefWorkItem(item.id(), item.instanceId(), "review".equals(item.stepId())
                ? "PROJECT_BRIEF_REVIEW_V1" : "PROJECT_BRIEF_HANDOFF_V1", item.stepId(), item.creatorId(), item.assigneeId(),
                item.status(), item.rowVersion(), item.deadlineAt(), item.workflowStatus(), item.currentStep(),
                blocked ? null : result.has("artifactVersion") ? result.path("artifactVersion").asInt(2) : null,
                blocked ? null : result.path("decision").asText(null), blocked ? null : result.path("disposition").asText(null),
                blocked ? null : result.path("notes").asText(null), blocked ? null : result.path("nextAction").asText(null),
                blocked ? null : result.path("note").asText(null), item.completedBy(), item.completedAt(), Set.copyOf(allowed), blocked);
    }

    private BriefWorkItemRow briefWorkItemRow(UUID tenantId, UUID workspaceId, UUID workItemId) {
        return jdbc.query("select h.id, h.instance_id, h.step_id, h.assignee_id, h.status, h.row_version, "
                        + "h.result_json::text item_result, h.completed_by, h.completed_at, h.created_at, "
                        + "i.tenant_id, i.workspace_id, i.actor_id creator_id, i.workflow_id, i.workflow_version, "
                        + "i.status workflow_status, i.current_step_id, i.deadline_at, i.input_json::text input_json "
                        + "from workflow.human_work_item h join workflow.instance i on i.id = h.instance_id "
                        + "where h.id = ? and h.tenant_id = ? and h.workspace_id = ?",
                rs -> rs.next() ? new BriefWorkItemRow(rs.getObject("id", UUID.class), rs.getObject("instance_id", UUID.class),
                        rs.getString("step_id"), rs.getObject("assignee_id", UUID.class), rs.getString("status"),
                        rs.getLong("row_version"), rs.getString("item_result"), rs.getObject("completed_by", UUID.class),
                        rs.getTimestamp("completed_at") == null ? null : rs.getTimestamp("completed_at").toInstant(),
                        rs.getTimestamp("created_at").toInstant(), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("creator_id", UUID.class),
                        rs.getObject("workflow_id", UUID.class), rs.getString("workflow_version"),
                        rs.getString("workflow_status"), rs.getString("current_step_id"),
                        rs.getTimestamp("deadline_at").toInstant(), rs.getString("input_json")) : null,
                workItemId, tenantId, workspaceId);
    }

    private record BriefWorkItemRow(UUID id, UUID instanceId, String stepId, UUID assigneeId, String status,
            long rowVersion, String itemResult, UUID completedBy, Instant completedAt, Instant createdAt,
            UUID tenantId, UUID workspaceId, UUID creatorId, UUID workflowId, String workflowVersion,
            String workflowStatus, String currentStep, Instant deadlineAt, String inputJson) {
        WorkflowInstance instance() { return new WorkflowInstance(idFromInput(), tenantId, workspaceId, creatorId, creatorId,
                null, "USER", workflowId, workflowVersion, null, null, inputJson, null, null, null,
                workflowStatus, null, currentStep, null, null, rowVersion, createdAt, deadlineAt, null,
                null, null, null, null); }
        private UUID idFromInput() { return instanceId; }
    }

    @Override
    @Transactional
    public ProjectBriefWorkItem reassignProjectBriefWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId,
            long expectedVersion, UUID assigneeId) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        workspaces.require(actor, workspaceId, "work-item:assign");
        requireActiveHuman(actor);
        if (assigneeId == null) throw EafException.invalid("assigneeId 必填。");
        var instanceId = lockHumanWorkItem(access.tenantId(), workspaceId, workItemId);
        var row = briefWorkItemRow(access.tenantId(), workspaceId, workItemId);
        if (row == null || !P29_PROJECT_BRIEF_WORKFLOW_ID.equals(row.workflowId())
                || !actor.actorId().equals(row.creatorId())) throw EafException.notFound();
        if (!"OPEN".equals(row.status()) || !"WAITING_HUMAN".equals(row.workflowStatus())
                || !row.stepId().equals(row.currentStep()) || !isBeforeDeadline(instanceId))
            throw EafException.conflict("HUMAN_WORK_ITEM_CLOSED", "只有等待中的项目简报 OPEN 工作项可以改派。");
        if (row.rowVersion() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "工作项版本已变化。");
        requireEligibleAssignee(access.tenantId(), workspaceId, assigneeId);
        requireProjectBriefParticipant(access.tenantId(), workspaceId, assigneeId);
        var instance = loadInstance(access.tenantId(), workspaceId, instanceId);
        var bundle = parseJson(parseJson(instance.inputJson(), "projectBrief.input").path("evidenceBundleJson").asText(), "projectBrief.evidence");
        if (java.util.stream.StreamSupport.stream(bundle.path("evidence").spliterator(), false)
                .anyMatch(source -> "P27_OA_QUERY".equals(source.path("sourceType").asText()))
                && !instance.actorId().equals(assigneeId))
            throw EafException.forbidden("含 OA 来源的项目简报只能由原查询 Owner 处理。");
        requireProjectBriefCurrent(actor, instance, assigneeId);
        var changed = jdbc.update("update workflow.human_work_item set assignee_id = ?, row_version = row_version + 1, updated_at = now() "
                        + "where id = ? and tenant_id = ? and workspace_id = ? and status = 'OPEN' and row_version = ?",
                assigneeId, workItemId, access.tenantId(), workspaceId, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "项目简报工作项改派竞争失败。");
        audit.append(new AuditFact("p29-work-item-reassigned:" + workItemId + ":" + expectedVersion,
                access.tenantId(), workspaceId, actor.actorId(), workItemId, "PROJECT_BRIEF_WORK_ITEM_REASSIGNED", "OPEN", "{}", null));
        return getProjectBriefWorkItem(actor, workspaceId, workItemId);
    }

    @Override
    @Transactional
    public ProjectBriefWorkItem reviewProjectBriefWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId,
            long expectedVersion, String idempotencyKey, String decision, String notes, String nextAction) {
        return completeProjectBriefHumanItem(actor, workspaceId, workItemId, expectedVersion, idempotencyKey,
                "review", decision, notes, nextAction, null);
    }

    @Override
    @Transactional
    public ProjectBriefWorkItem receiveProjectBriefWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId,
            long expectedVersion, String idempotencyKey, String disposition, String note) {
        return completeProjectBriefHumanItem(actor, workspaceId, workItemId, expectedVersion, idempotencyKey,
                "handoff", disposition, null, null, note);
    }

    private ProjectBriefWorkItem completeProjectBriefHumanItem(ActorContext actor, UUID workspaceId, UUID workItemId,
            long expectedVersion, String idempotencyKey, String stepId, String choice, String notes,
            String nextAction, String note) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        workspaces.require(actor, workspaceId, "work-item:complete");
        requireActiveHuman(actor);
        var normalizedChoice = choice == null ? "" : choice.trim();
        var normalizedNotes = notes == null ? "" : notes.trim();
        var normalizedNext = nextAction == null ? "" : nextAction.trim();
        var normalizedNote = note == null ? "" : note.trim();
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 128
                || "review".equals(stepId) && (!Set.of("CONFIRMED", "NEEDS_FOLLOWUP").contains(normalizedChoice)
                    || normalizedNotes.length() > 2_000 || normalizedNext.length() > 1_000
                    || "NEEDS_FOLLOWUP".equals(normalizedChoice) && (normalizedNotes.isBlank() || normalizedNext.isBlank()))
                || "handoff".equals(stepId) && (!Set.of("RECEIVED", "NEEDS_FOLLOWUP").contains(normalizedChoice)
                    || normalizedNote.length() > 1_000))
            throw EafException.invalid("项目简报人工提交字段无效。");
        var instanceId = lockHumanWorkItem(access.tenantId(), workspaceId, workItemId);
        var row = briefWorkItemRow(access.tenantId(), workspaceId, workItemId);
        if (row == null || !P29_PROJECT_BRIEF_WORKFLOW_ID.equals(row.workflowId())
                || !stepId.equals(row.stepId())) throw EafException.notFound();
        if (!actor.actorId().equals(row.assigneeId())) throw EafException.notFound();
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(), workspaceId.toString(),
                workItemId.toString(), actor.actorId().toString(), idempotencyKey));
        var requestHash = Hashing.sha256(String.join("\u001f", stepId, normalizedChoice, normalizedNotes,
                normalizedNext, normalizedNote, actor.actorId().toString()));
        var completed = "COMPLETED".equals(row.status());
        if (completed) {
            var completion = jdbc.query("select completion_key_hash, completion_request_hash from workflow.human_work_item where id = ?",
                    rs -> rs.next() ? new String[]{rs.getString(1), rs.getString(2)} : null, workItemId);
            requireProjectBriefCurrent(actor, loadInstance(access.tenantId(), workspaceId, instanceId));
            if (completion != null && keyHash.equals(completion[0]) && requestHash.equals(completion[1]))
                return getProjectBriefWorkItem(actor, workspaceId, workItemId);
            throw EafException.conflict("HUMAN_WORK_ITEM_COMPLETED", "项目简报人工项已经完成，不能更改提交内容。");
        }
        if (!"OPEN".equals(row.status()) || !"WAITING_HUMAN".equals(row.workflowStatus())
                || !stepId.equals(row.currentStep()) || row.rowVersion() != expectedVersion || !isBeforeDeadline(instanceId))
            throw EafException.conflict("VERSION_CONFLICT", "项目简报人工项状态、版本或截止时间已变化。");
        requireEligibleAssignee(access.tenantId(), workspaceId, actor.actorId());
        var instance = loadInstance(access.tenantId(), workspaceId, instanceId);
        if (!identityDirectory.isActiveHuman(access.tenantId(), instance.actorId()))
            throw EafException.conflict("WORKFLOW_ACTOR_UNAVAILABLE", "项目简报发起身份当前不可用。");
        var creator = briefActor(access.tenantId(), workspaceId, instance.actorId());
        requireRunnable(creator, workspaceId, instanceId);
        requireProjectBriefCurrent(actor, instance);
        var completedAt = jdbc.queryForObject("select now()", Timestamp.class).toInstant();
        var output = json.createObjectNode();
        if ("review".equals(stepId)) {
            output.put("decision", normalizedChoice).put("notes", normalizedNotes).put("nextAction", normalizedNext)
                    .put("completedBy", actor.actorId().toString()).put("completedAt", completedAt.toString())
                    .put("artifactVersion", "2");
            createProjectBriefRevision(instance, actor.actorId(), normalizedChoice, normalizedNotes, normalizedNext);
        } else {
            var exactArtifact = getProjectBriefArtifact(actor, workspaceId, instanceId, 2);
            if (exactArtifact.blocked() || exactArtifact.markdown() == null)
                throw EafException.conflict("BRIEF_SOURCE_UNAVAILABLE", "交接前必须能读取当前来源对应的 v2 简报。");
            output.put("disposition", normalizedChoice).put("note", normalizedNote)
                    .put("completedBy", actor.actorId().toString()).put("completedAt", completedAt.toString())
                    .put("artifactVersion", "2");
        }
        var outputSchema = "review".equals(stepId) ? P29_REVIEW_OUTPUT_SCHEMA : P29_HANDOFF_OUTPUT_SCHEMA;
        schemas.validateDeclaredOutput(outputSchema, output);
        var outputJson = canonical(output);
        var changed = jdbc.update("update workflow.human_work_item set status = 'COMPLETED', result_json = ?::jsonb, "
                        + "completed_by = ?, completed_at = ?, completion_key_hash = ?, completion_request_hash = ?, "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and status = 'OPEN' and row_version = ?",
                outputJson, actor.actorId(), Timestamp.from(completedAt), keyHash, requestHash, workItemId, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "项目简报人工项完成竞争失败。");
        var nextStep = "review".equals(stepId) ? "handoff" : "complete";
        changed = jdbc.update("update workflow.step set status = 'SUCCEEDED', output_json = ?::jsonb, selected_next_step_id = ?, updated_at = now() "
                        + "where instance_id = ? and step_id = ? and step_type = 'HUMAN_TASK' and status = 'WAITING_HUMAN'",
                outputJson, nextStep, instanceId, stepId);
        if (changed != 1) throw EafException.conflict("WORKFLOW_STEP_CONFLICT", "项目简报人工步骤状态已变化。");
        changed = jdbc.update("update workflow.instance set status = 'RUNNING', current_step_id = ?, waiting_reason = null, "
                        + "lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, next_poll_at = now(), "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and status = 'WAITING_HUMAN' "
                        + "and current_step_id = ? and deadline_at > now()",
                nextStep, instanceId, stepId);
        if (changed != 1) throw EafException.conflict("WORKFLOW_STATE_CONFLICT", "项目简报不再等待当前人工步骤。");
        audit.append(new AuditFact("p29-work-item-completed:" + workItemId, access.tenantId(), workspaceId,
                actor.actorId(), workItemId, "PROJECT_BRIEF_WORK_ITEM_COMPLETED", normalizedChoice, "{}", null));
        return getProjectBriefWorkItem(actor, workspaceId, workItemId);
    }

    private void createProjectBriefRevision(WorkflowInstance instance, UUID authorId, String decision,
            String notes, String nextAction) {
        var source = getProjectBriefArtifact(briefActor(instance.tenantId(), instance.workspaceId(), authorId),
                instance.workspaceId(), instance.id(), 1);
        if (source.blocked() || source.markdown() == null)
            throw EafException.conflict("BRIEF_SOURCE_UNAVAILABLE", "人工补充前无法读取当前来源对应的 v1 简报。");
        var markdown = requireBriefMarkdown(source.markdown() + "\n\n## 人工补充与复核\n\n"
                + "复核人：" + markdownText(authorId.toString()) + "\n\n"
                + "复核结论：" + markdownText(decision) + "\n\n"
                + "补充说明：" + markdownText(notes.isBlank() ? "无" : notes) + "\n\n"
                + "后续行动：" + markdownText(nextAction.isBlank() ? "无" : nextAction)
                + "\n\n交接状态：等待接收；handoff 将绑定本 v2 版本。\n");
        var generated = jdbc.query("select generation_task_id, generation_attempt, evidence_bundle_hash from workflow.project_brief_state where instance_id = ? for update",
                rs -> rs.next() ? new Object[]{rs.getObject(1, UUID.class), rs.getInt(2), rs.getString(3)} : null, instance.id());
        if (generated == null || generated[0] == null || generated[1] == null)
            throw EafException.conflict("BRIEF_GENERATION_MISSING", "项目简报尚无可供人工补充的生成版。");
        var hash = Hashing.sha256(markdown);
        var inserted = jdbc.update("insert into workflow.project_brief_artifact(instance_id, tenant_id, workspace_id, "
                        + "artifact_version, artifact_kind, template_version, generation_task_id, generation_attempt, "
                        + "evidence_bundle_hash, agent_id, agent_version, capability_id, capability_version, author_id, markdown, content_hash) "
                        + "values (?, ?, ?, 2, 'HUMAN_REVISION', 'PROJECT_BRIEF_MARKDOWN_V1', ?, ?, ?, ?, '1.0.0', ?, '1.0.0', ?, ?, ?) on conflict do nothing",
                instance.id(), instance.tenantId(), instance.workspaceId(), generated[0], generated[1], generated[2],
                P29_PROJECT_BRIEF_AGENT_ID, P29_PROJECT_BRIEF_CAPABILITY_ID, authorId, markdown, hash);
        if (inserted == 0) {
            var prior = jdbc.queryForObject("select content_hash from workflow.project_brief_artifact where instance_id = ? and artifact_version = 2",
                    String.class, instance.id());
            if (!hash.equals(prior)) throw EafException.conflict("BRIEF_ARTIFACT_CONFLICT", "简报 v2 已由其他复核结果生成。");
        }
    }

    @Override
    @Transactional(readOnly = true)
    public ProjectBriefInboxPage listProjectBriefInbox(ActorContext actor, UUID workspaceId, Instant cursorUpdatedAt,
            UUID cursorId, int pageSize) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "workflow:read");
        requireActiveHuman(actor);
        if (pageSize < 1 || pageSize > 50 || (cursorUpdatedAt == null) != (cursorId == null))
            throw EafException.invalid("项目简报收件箱分页参数无效。");
        var sql = new StringBuilder("select id, updated_at from workflow.instance where tenant_id = ? and workspace_id = ? "
                + "and workflow_id = ? and (actor_id = ? or input_json ->> 'reviewerId' = ? or input_json ->> 'recipientId' = ? "
                + "or exists(select 1 from workflow.human_work_item h where h.instance_id = workflow.instance.id and h.assignee_id = ?))");
        var args = new ArrayList<Object>(List.of(access.tenantId(), workspaceId, P29_PROJECT_BRIEF_WORKFLOW_ID,
                actor.actorId(), actor.actorId().toString(), actor.actorId().toString(), actor.actorId()));
        if (cursorUpdatedAt != null) {
            sql.append(" and (updated_at, id) < (?, ?)");
            args.add(Timestamp.from(cursorUpdatedAt)); args.add(cursorId);
        }
        args.add(pageSize + 1);
        var keys = jdbc.query(sql + " order by updated_at desc, id desc limit ?",
                (rs, row) -> new SourceInstanceKey(rs.getObject("id", UUID.class), rs.getTimestamp("updated_at").toInstant()),
                args.toArray());
        var hasNext = keys.size() > pageSize;
        var selected = hasNext ? List.copyOf(keys.subList(0, pageSize)) : List.copyOf(keys);
        var items = new ArrayList<ProjectBriefInboxPage.Item>();
        for (var key : selected) {
            var instance = loadInstance(access.tenantId(), workspaceId, key.id());
            var snapshot = projectBriefSnapshot(actor, instance, true);
            items.add(new ProjectBriefInboxPage.Item(instance.id(), snapshot.blocked() ? "项目简报（来源暂不可用）" : snapshot.title(),
                    instance.actorId(), instance.status(), instance.waitingReason(), key.createdAt(),
                    "/api/v1/workspaces/" + workspaceId + "/project-briefs/" + instance.id(),
                    snapshot.allowedActions(), snapshot.blocked()));
        }
        var last = hasNext ? selected.get(selected.size() - 1) : null;
        return new ProjectBriefInboxPage(items, last == null ? null : last.createdAt(), last == null ? null : last.id());
    }

    @Override
    @Transactional(readOnly = true)
    public WorkflowHumanInboxPage listMyHumanInbox(ActorContext actor, UUID workspaceId, Instant cursorUpdatedAt,
            UUID cursorId, int pageSize) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        requireActiveHuman(actor);
        if (pageSize < 1 || pageSize > 50 || (cursorUpdatedAt == null) != (cursorId == null))
            throw EafException.invalid("人工收件箱分页参数无效。");
        var args = new ArrayList<Object>(List.of(access.tenantId(), workspaceId, actor.actorId(),
                P16_SERVICE_REQUEST_WORKFLOW_ID, P29_PROJECT_BRIEF_WORKFLOW_ID));
        var sql = new StringBuilder("select h.id, h.instance_id, h.step_id, h.updated_at, i.workflow_id, i.actor_id "
                + "from workflow.human_work_item h join workflow.instance i on i.id = h.instance_id "
                + "where h.tenant_id = ? and h.workspace_id = ? and h.assignee_id = ? and h.status = 'OPEN' "
                + "and i.workflow_id in (?, ?) and i.status = 'WAITING_HUMAN' and i.deadline_at > now()");
        if (cursorUpdatedAt != null) {
            sql.append(" and (h.updated_at, h.id) < (?, ?)");
            args.add(Timestamp.from(cursorUpdatedAt)); args.add(cursorId);
        }
        args.add(pageSize + 1);
        var keys = jdbc.query(sql + " order by h.updated_at desc, h.id desc limit ?",
                (rs, row) -> new HumanInboxKey(rs.getObject("id", UUID.class), rs.getObject("instance_id", UUID.class),
                        rs.getString("step_id"), rs.getTimestamp("updated_at").toInstant(),
                        rs.getObject("workflow_id", UUID.class), rs.getObject("actor_id", UUID.class)), args.toArray());
        var hasNext = keys.size() > pageSize;
        var selected = hasNext ? List.copyOf(keys.subList(0, pageSize)) : List.copyOf(keys);
        var items = new ArrayList<WorkflowHumanInboxPage.Item>();
        for (var key : selected) {
            if (P29_PROJECT_BRIEF_WORKFLOW_ID.equals(key.workflowId())) {
                var item = getProjectBriefWorkItem(actor, workspaceId, key.id());
                items.add(new WorkflowHumanInboxPage.Item(key.id(), item.kind(), item.blocked()
                        ? "项目简报人工项（来源暂不可用）" : "项目简报" + ("review".equals(key.stepId()) ? "复核" : "交接"),
                        key.creatorId(), actor.actorId(), item.status(), "HUMAN_TASK", key.updatedAt(),
                        "/api/v1/workspaces/" + workspaceId + "/project-brief-work-items/" + key.id(),
                        item.allowedActions(), item.blocked()));
            } else {
                var item = getHumanWorkItem(actor, workspaceId, key.id());
                items.add(new WorkflowHumanInboxPage.Item(key.id(), "P16_SERVICE_REQUEST", "服务请求人工处理",
                        key.creatorId(), actor.actorId(), item.status(), "HUMAN_TASK", key.updatedAt(),
                        "/api/v1/workspaces/" + workspaceId + "/human-work-items/" + key.id(),
                        item.allowedActions(), false));
            }
        }
        var last = hasNext ? selected.get(selected.size() - 1) : null;
        return new WorkflowHumanInboxPage(items, last == null ? null : last.updatedAt(), last == null ? null : last.id());
    }

    private record HumanInboxKey(UUID id, UUID instanceId, String stepId, Instant updatedAt,
            UUID workflowId, UUID creatorId) { }

    @Override
    @Transactional(readOnly = true)
    public WorkflowService.TeamExperienceSource requireTeamExperienceSource(ActorContext actor, UUID workspaceId,
            UUID workItemId) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        requireActiveHuman(actor);
        var item = loadHumanWorkItem(access.tenantId(), workspaceId, workItemId, actor);
        var instance = loadInstance(access.tenantId(), workspaceId, item.instanceId());
        if (!P16_SERVICE_REQUEST_WORKFLOW_ID.equals(instance.workflowId())
                || !Set.of(P16_SERVICE_REQUEST_WORKFLOW_VERSION, P17_SERVICE_REQUEST_WORKFLOW_VERSION,
                        P18_SERVICE_REQUEST_WORKFLOW_VERSION)
                        .contains(instance.workflowVersion())
                || !"USER".equals(instance.source()) || instance.qualityRunId() != null
                || !"handle".equals(item.stepId()) || !"COMPLETED".equals(item.status())
                || item.outcome() == null || item.summary() == null || item.completedBy() == null || item.completedAt() == null)
            throw EafException.conflict("TEAM_EXPERIENCE_SOURCE_INVALID", "只接受同 Workspace 已完成的 HUMAN 工作结果。");
        var stable = json.createObjectNode().put("workItemId", item.id().toString())
                .put("instanceId", item.instanceId().toString()).put("workItemVersion", item.rowVersion())
                .put("outcome", item.outcome()).put("summary", item.summary()).put("nextAction", item.nextAction() == null ? "" : item.nextAction())
                .put("completedBy", item.completedBy().toString()).put("completedAt", item.completedAt().toString())
                .put("sourceType", "USER");
        return new WorkflowService.TeamExperienceSource(item.id(), item.instanceId(), item.rowVersion(),
                item.completedBy(), item.completedAt(), item.outcome(), Hashing.sha256(canonical(stable)), "USER");
    }

    @Override
    @Transactional(readOnly = true)
    public WorkflowService.TeamExperienceTaskSelection requireTeamExperienceSelectionForTask(
            ActorContext actor, UUID workspaceId, UUID taskId) {
        requireDirectHuman(actor);
        workspaces.require(actor, workspaceId, "memory:read");
        requireActiveHuman(actor);
        var row = jdbc.query("select i.tenant_id, i.actor_id, i.workflow_version, i.source, i.status, i.current_step_id, "
                        + "s.step_type, s.status step_status, i.input_json::text input_json "
                        + "from workflow.instance i join workflow.step s on s.instance_id = i.id "
                        + "where s.child_task_id = ? and s.step_id = 'prepare' and i.workflow_id = ? and i.workspace_id = ?",
                rs -> rs.next() ? new TeamExperienceTaskRow(rs.getObject("tenant_id", UUID.class),
                        rs.getObject("actor_id", UUID.class), rs.getString("workflow_version"), rs.getString("source"),
                        rs.getString("status"), rs.getString("current_step_id"), rs.getString("step_type"),
                        rs.getString("step_status"), rs.getString("input_json")) : null,
                taskId, P16_SERVICE_REQUEST_WORKFLOW_ID, workspaceId);
        if (row == null) return p21ExperienceSelection(actor, workspaceId, taskId);
        if (row == null || !Set.of(P17_SERVICE_REQUEST_WORKFLOW_VERSION, P18_SERVICE_REQUEST_WORKFLOW_VERSION)
                .contains(row.workflowVersion())
                || !"USER".equals(row.source()) || !actor.actorId().equals(row.actorId())
                // 子 Task 开始运行后，Workflow 已进入 WAITING_CHILD；此时正是 prepare 读取冻结选择的阶段。
                || !"WAITING_CHILD".equals(row.instanceStatus()) || !"prepare".equals(row.currentStep())
                || !"RUN_CAPABILITY".equals(row.stepType()) || !"WAITING_CHILD".equals(row.stepStatus()))
            throw EafException.forbidden("所选团队经验只允许由固定 prepare 步骤读取。");
        var input = parseJson(row.inputJson(), "service-request-handling.input");
        var scenario = input.path("scenarioKey").asText(null);
        final List<TeamExperienceRef> refs;
        try { refs = json.readValue(input.path("teamExperienceRefsJson").asText(), new TypeReference<>() { }); }
        catch (Exception invalid) { throw EafException.conflict("TEAM_EXPERIENCE_SELECTION_INVALID", "冻结的团队经验引用无法读取。"); }
        UUID assigneeId;
        try { assigneeId = UUID.fromString(input.path("assigneeId").asText()); }
        catch (IllegalArgumentException invalid) { throw EafException.conflict("TEAM_EXPERIENCE_ASSIGNEE_INVALID", "冻结的初始处理人无效。"); }
        var tenantId = row.tenantId();
        if (!identityDirectory.isActiveHuman(tenantId, assigneeId)
                || !workspaces.isAuthorized(tenantId, assigneeId, workspaceId, "work-item:read")
                || !workspaces.isAuthorized(tenantId, assigneeId, workspaceId, "work-item:complete")
                || !workspaces.isAuthorized(tenantId, assigneeId, workspaceId, "memory:read"))
            throw EafException.conflict("TEAM_EXPERIENCE_ASSIGNEE_ACCESS_REVOKED", "当前处理人已失去读取团队经验所需权限。");
        if (scenario == null || !scenario.matches("[a-z][a-z0-9-]{0,63}") || refs.isEmpty() || refs.size() > 3)
            throw EafException.conflict("TEAM_EXPERIENCE_SELECTION_INVALID", "冻结的团队经验场景或引用无效。");
        return new WorkflowService.TeamExperienceTaskSelection(scenario, refs, assigneeId);
    }

    private WorkflowService.TeamExperienceTaskSelection p21ExperienceSelection(ActorContext actor, UUID workspaceId, UUID taskId) {
        var row = jdbc.query("select i.tenant_id, i.actor_id, i.workflow_version, i.source, i.status, i.current_step_id, "
                        + "s.status step_status, p.status branch_status, p.input_json::text input_json, b.initiator_id "
                        + "from workflow.parallel_branch p join workflow.instance i on i.id = p.instance_id "
                        + "join workflow.step s on s.instance_id = i.id and s.step_id = p.group_step_id "
                        + "join workflow.batch_item bi on bi.workflow_instance_id = i.id "
                        + "join workflow.batch b on b.id = bi.batch_id "
                        + "where p.child_task_id = ? and p.role = 'experience' and i.workspace_id = ?",
                rs -> rs.next() ? new P21BranchRow(rs.getObject("tenant_id", UUID.class), rs.getObject("actor_id", UUID.class),
                        rs.getString("workflow_version"), rs.getString("source"), rs.getString("status"),
                        rs.getString("current_step_id"), rs.getString("step_status"), rs.getString("branch_status"),
                        rs.getString("input_json"), rs.getObject("initiator_id", UUID.class)) : null,
                taskId, workspaceId);
        if (row == null || !"1.0.0".equals(row.workflowVersion()) || !"USER".equals(row.source())
                || !actor.actorId().equals(row.actorId()) || !actor.actorId().equals(row.initiatorId())
                || !"WAITING_CHILD".equals(row.instanceStatus()) || !"gather".equals(row.currentStep())
                || !"WAITING_CHILD".equals(row.stepStatus()) || !"WAITING_CHILD".equals(row.branchStatus()))
            throw EafException.forbidden("团队经验只允许由当前流程或本人批次经验分支读取。");
        var input = parseJson(row.inputJson(), "p21.experience.input");
        var refs = parseRefs(input.path("experienceRefsJson").asText("[]"));
        var scenario = input.path("scenarioKey").asText(null);
        if (scenario == null || scenario.isBlank() || refs.size() > 3)
            throw EafException.conflict("TEAM_EXPERIENCE_SELECTION_INVALID", "冻结的经验引用或场景无效。");
        return new WorkflowService.TeamExperienceTaskSelection(scenario, refs, row.initiatorId());
    }

    @Override
    @Transactional(readOnly = true)
    public void requireP21KnowledgeBranch(ActorContext actor, UUID workspaceId, UUID taskId) {
        requireDirectHuman(actor);
        workspaces.require(actor, workspaceId, "workflow:start");
        requireActiveHuman(actor);
        var allowed = jdbc.queryForObject("select count(*) from workflow.parallel_branch p join workflow.instance i on i.id = p.instance_id "
                        + "join workflow.step s on s.instance_id = i.id and s.step_id = p.group_step_id "
                        + "join workflow.batch_item bi on bi.workflow_instance_id = i.id join workflow.batch b on b.id = bi.batch_id "
                        + "where p.child_task_id = ? and p.role = 'knowledge' and p.capability_id = ? and i.tenant_id = ? "
                        + "and i.workspace_id = ? and i.actor_id = ? and b.initiator_id = ? and i.source = 'USER' "
                        + "and i.status = 'WAITING_CHILD' and i.current_step_id = 'gather' and s.status = 'WAITING_CHILD' and p.status = 'WAITING_CHILD'",
                Integer.class, taskId, P21_KNOWLEDGE_CAPABILITY_ID, actor.tenantId(), workspaceId, actor.actorId(), actor.actorId());
        if (allowed == null || allowed != 1) throw EafException.forbidden("知识查询只允许由本人固定 knowledge 分支调用。");
    }

    @Override
    @Transactional
    public HumanWorkItem reassignHumanWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId,
            long expectedVersion, UUID assigneeId) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        workspaces.require(actor, workspaceId, "work-item:assign");
        requireActiveHuman(actor);
        if (assigneeId == null) throw EafException.invalid("assigneeId 必填。");
        var instanceId = lockHumanWorkItem(access.tenantId(), workspaceId, workItemId);
        var state = humanItemWriteState(access.tenantId(), workspaceId, workItemId);
        if (!P16_SERVICE_REQUEST_WORKFLOW_ID.equals(state.workflowId()) || !actor.actorId().equals(state.creatorId()))
            throw EafException.notFound();
        if (!"OPEN".equals(state.status()) || !"WAITING_HUMAN".equals(state.workflowStatus())
                || !"handle".equals(state.currentStep()) || !isBeforeDeadline(instanceId))
            throw EafException.conflict("HUMAN_WORK_ITEM_CLOSED", "只有等待中的 OPEN 工作项可以改派。");
        if (state.rowVersion() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "工作项版本已变化。");
        requireEligibleAssignee(access.tenantId(), workspaceId, assigneeId);
        var handling = loadInstance(access.tenantId(), workspaceId, instanceId);
        if (Set.of(P17_SERVICE_REQUEST_WORKFLOW_VERSION, P18_SERVICE_REQUEST_WORKFLOW_VERSION).contains(handling.workflowVersion())
                && parseJson(handling.inputJson(), "handling.input").has("teamExperienceRefsJson")
                && !workspaces.isAuthorized(access.tenantId(), assigneeId, workspaceId, "memory:read"))
            throw EafException.forbidden("新处理人没有读取当前 Workspace 团队经验的权限。");
        var changed = jdbc.update("update workflow.human_work_item set assignee_id = ?, row_version = row_version + 1, updated_at = now() "
                        + "where id = ? and tenant_id = ? and workspace_id = ? and status = 'OPEN' and row_version = ?",
                assigneeId, workItemId, access.tenantId(), workspaceId, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "工作项改派竞争失败。");
        audit.append(new AuditFact("p16-work-item-reassigned:" + workItemId + ":" + expectedVersion,
                access.tenantId(), workspaceId, actor.actorId(), workItemId, "HUMAN_WORK_ITEM_REASSIGNED", "OPEN", "{}", null));
        recordP16WorkItemEvent(workItemId, "REASSIGNED", state.assigneeId(), assigneeId);
        return loadHumanWorkItem(access.tenantId(), workspaceId, workItemId, actor);
    }

    @Override
    @Transactional
    public HumanWorkItem completeHumanWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId,
            long expectedVersion, String idempotencyKey, String outcome, String summary, String nextAction) {
        requireDirectHuman(actor);
        var access = workspaces.require(actor, workspaceId, "work-item:read");
        workspaces.require(actor, workspaceId, "work-item:complete");
        requireActiveHuman(actor);
        var normalizedOutcome = outcome == null ? "" : outcome.trim();
        var normalizedSummary = requireText(summary, 2_000, "summary");
        var normalizedNextAction = nextAction == null ? "" : nextAction.trim();
        if (!HUMAN_OUTCOMES.contains(normalizedOutcome) || normalizedSummary.isEmpty()
                || normalizedNextAction.length() > 1_000 || idempotencyKey == null
                || idempotencyKey.isBlank() || idempotencyKey.length() > 128
                || !"COMPLETED".equals(normalizedOutcome) && normalizedNextAction.isEmpty())
            throw EafException.invalid("人工处理结果字段无效。");
        var instanceId = lockHumanWorkItem(access.tenantId(), workspaceId, workItemId);
        var state = humanItemWriteState(access.tenantId(), workspaceId, workItemId);
        if (!P16_SERVICE_REQUEST_WORKFLOW_ID.equals(state.workflowId()) || !actor.actorId().equals(state.assigneeId()))
            throw EafException.notFound();
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(), workspaceId.toString(),
                workItemId.toString(), actor.actorId().toString(), idempotencyKey));
        var requestHash = Hashing.sha256(String.join("\u001f", normalizedOutcome, normalizedSummary,
                normalizedNextAction, actor.actorId().toString()));
        if ("COMPLETED".equals(state.status())) {
            if (keyHash.equals(state.completionKeyHash()) && requestHash.equals(state.completionRequestHash()))
                return loadHumanWorkItem(access.tenantId(), workspaceId, workItemId, actor);
            throw EafException.conflict("HUMAN_WORK_ITEM_COMPLETED", "工作项已经完成，不能用其他结果或幂等键重复提交。");
        }
        if (!"OPEN".equals(state.status()) || !"WAITING_HUMAN".equals(state.workflowStatus())
                || !"handle".equals(state.currentStep()) || !isBeforeDeadline(instanceId))
            throw EafException.conflict("HUMAN_WORK_ITEM_CLOSED", "工作项当前不能完成。");
        if (state.rowVersion() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "工作项版本已变化。");
        requireEligibleAssignee(access.tenantId(), workspaceId, actor.actorId());
        if (!identityDirectory.isActiveHuman(access.tenantId(), state.creatorId()))
            throw EafException.conflict("WORKFLOW_ACTOR_UNAVAILABLE", "Workflow 发起身份当前不可用。");
        var creator = new ActorContext(state.creatorId(), access.tenantId(), io.eaf.shared.ActorType.HUMAN,
                workspaces.actions(access.tenantId(), state.creatorId(), workspaceId));
        requireRunnable(creator, workspaceId, instanceId);
        var completedAt = jdbc.queryForObject("select now()", Timestamp.class).toInstant();
        var output = json.createObjectNode().put("outcome", normalizedOutcome).put("summary", normalizedSummary)
                .put("nextAction", normalizedNextAction).put("completedBy", actor.actorId().toString())
                .put("completedAt", completedAt.toString());
        schemas.validateDeclaredOutput(HUMAN_TASK_OUTPUT_SCHEMA, output);
        var outputJson = canonical(output);
        var changed = jdbc.update("update workflow.human_work_item set status = 'COMPLETED', result_json = ?::jsonb, "
                        + "completed_by = ?, completed_at = ?, completion_key_hash = ?, completion_request_hash = ?, "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and status = 'OPEN' and row_version = ?",
                outputJson, actor.actorId(), Timestamp.from(completedAt), keyHash, requestHash, workItemId, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "工作项完成竞争失败。");
        recordP16WorkItemEvent(workItemId, "COMPLETED", state.assigneeId(), null);
        changed = jdbc.update("update workflow.step set status = 'SUCCEEDED', output_json = ?::jsonb, selected_next_step_id = 'summarize', updated_at = now() "
                        + "where instance_id = ? and step_id = 'handle' and step_type = 'HUMAN_TASK' and status = 'WAITING_HUMAN'",
                outputJson, instanceId);
        if (changed != 1) throw EafException.conflict("WORKFLOW_STEP_CONFLICT", "Workflow 人工步骤状态已变化。");
        changed = jdbc.update("update workflow.instance set status = 'RUNNING', current_step_id = 'summarize', waiting_reason = null, "
                        + "lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, next_poll_at = now(), "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and status = 'WAITING_HUMAN' "
                        + "and current_step_id = 'handle' and deadline_at > now()",
                instanceId);
        if (changed != 1) throw EafException.conflict("WORKFLOW_STATE_CONFLICT", "Workflow 不再等待人工处理。");
        audit.append(new AuditFact("p16-work-item-completed:" + workItemId, access.tenantId(), workspaceId,
                actor.actorId(), workItemId, "HUMAN_WORK_ITEM_COMPLETED", normalizedOutcome, "{}", null));
        return loadHumanWorkItem(access.tenantId(), workspaceId, workItemId, actor);
    }

    private void requireActiveHuman(ActorContext actor) {
        if (!identityDirectory.isActiveHuman(actor.tenantId(), actor.actorId())) throw EafException.notFound();
    }

    private boolean isBeforeDeadline(UUID instanceId) {
        return Boolean.TRUE.equals(jdbc.queryForObject("select deadline_at > now() from workflow.instance where id = ?",
                Boolean.class, instanceId));
    }

    private UUID lockHumanWorkItem(UUID tenantId, UUID workspaceId, UUID workItemId) {
        var instanceId = jdbc.query("select instance_id from workflow.human_work_item where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getObject("instance_id", UUID.class) : null, workItemId, tenantId, workspaceId);
        if (instanceId == null) throw EafException.notFound();
        jdbc.query("select id from workflow.instance where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null, instanceId, tenantId, workspaceId);
        jdbc.query("select id from workflow.human_work_item where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null, workItemId, tenantId, workspaceId);
        return instanceId;
    }

    private HumanItemWriteState humanItemWriteState(UUID tenantId, UUID workspaceId, UUID workItemId) {
        return jdbc.query("select h.status, h.row_version, h.assignee_id, h.completion_key_hash, h.completion_request_hash, "
                        + "i.actor_id creator_id, i.workflow_id, i.status workflow_status, i.current_step_id "
                        + "from workflow.human_work_item h join workflow.instance i on i.id = h.instance_id "
                        + "where h.id = ? and h.tenant_id = ? and h.workspace_id = ?",
                rs -> rs.next() ? new HumanItemWriteState(rs.getString("status"), rs.getLong("row_version"),
                        rs.getObject("assignee_id", UUID.class), rs.getString("completion_key_hash"),
                        rs.getString("completion_request_hash"), rs.getObject("creator_id", UUID.class),
                        rs.getObject("workflow_id", UUID.class), rs.getString("workflow_status"), rs.getString("current_step_id")) : null,
                workItemId, tenantId, workspaceId);
    }

    private record HumanItemWriteState(String status, long rowVersion, UUID assigneeId, String completionKeyHash,
            String completionRequestHash, UUID creatorId, UUID workflowId, String workflowStatus, String currentStep) { }

    private HumanWorkItem loadHumanWorkItem(UUID tenantId, UUID workspaceId, UUID workItemId, ActorContext actor) {
        var item = jdbc.query("select h.id, h.instance_id, h.step_id, h.assignee_id, h.status, h.row_version, "
                        + "h.result_json::text item_result, h.completed_by, h.completed_at, h.created_at, "
                        + "i.actor_id creator_id, i.input_json::text input_json, i.result_json::text workflow_result, "
                        + "i.status workflow_status, i.row_version workflow_row_version, i.current_step_id, i.deadline_at, i.error_code, "
                        + "p.output_json::text prepare_output from workflow.human_work_item h "
                        + "join workflow.instance i on i.id = h.instance_id and i.tenant_id = h.tenant_id and i.workspace_id = h.workspace_id "
                        + "left join workflow.step p on p.instance_id = i.id and p.step_id = 'prepare' "
                        + "where h.id = ? and h.tenant_id = ? and h.workspace_id = ? and i.workflow_id = ?",
                rs -> rs.next() ? new HumanWorkItemData(rs.getObject("id", UUID.class), rs.getObject("instance_id", UUID.class),
                        rs.getString("step_id"), rs.getObject("assignee_id", UUID.class), rs.getString("status"),
                        rs.getLong("row_version"), rs.getString("item_result"), rs.getObject("completed_by", UUID.class),
                        rs.getTimestamp("completed_at") == null ? null : rs.getTimestamp("completed_at").toInstant(),
                        rs.getTimestamp("created_at").toInstant(), rs.getObject("creator_id", UUID.class),
                        rs.getString("input_json"), rs.getString("workflow_result"), rs.getString("workflow_status"),
                        rs.getLong("workflow_row_version"), rs.getString("current_step_id"),
                        rs.getTimestamp("deadline_at").toInstant(), rs.getString("error_code"), rs.getString("prepare_output")) : null,
                workItemId, tenantId, workspaceId, P16_SERVICE_REQUEST_WORKFLOW_ID);
        if (item == null) throw EafException.notFound();
        if (!actor.actorId().equals(item.creatorId()) && !actor.actorId().equals(item.assigneeId())) throw EafException.notFound();
        if (!identityDirectory.isActiveHuman(tenantId, actor.actorId())) throw EafException.notFound();
        var input = parseJson(item.inputJson(), "handling.input");
        var manual = item.itemResult() == null ? json.createObjectNode() : parseJson(item.itemResult(), "workItem.result");
        var finalResult = item.workflowResult() == null ? json.createObjectNode() : parseJson(item.workflowResult(), "handling.result");
        var prepare = item.prepareOutput() == null ? json.createObjectNode() : parseJson(item.prepareOutput(), "prepare.output");
        var allowed = new java.util.LinkedHashSet<String>();
        allowed.add("read");
        var openAndWaiting = "OPEN".equals(item.status()) && "WAITING_HUMAN".equals(item.workflowStatus())
                && item.deadlineAt().isAfter(Instant.now());
        if (openAndWaiting && actor.actorId().equals(item.creatorId())
                && workspaces.isAuthorized(tenantId, actor.actorId(), workspaceId, "work-item:assign")) allowed.add("assign");
        if (openAndWaiting && actor.actorId().equals(item.assigneeId())
                && workspaces.isAuthorized(tenantId, actor.actorId(), workspaceId, "work-item:complete")) allowed.add("complete");
        return new HumanWorkItem(item.id(), item.instanceId(), input.path("requestId").asText(null), item.stepId(),
                item.creatorId(), item.assigneeId(), item.status(), item.rowVersion(), item.deadlineAt(),
                input.path("sharedBrief").asText(null), prepare.path("handlingAdvice").asText(null),
                prepare.path("cautions").asText(null), manual.path("outcome").asText(null),
                manual.path("summary").asText(null), manual.path("nextAction").asText(null), item.completedBy(),
                item.completedAt(), finalResult.path("resultSummary").asText(null), finalResult.path("remainingWork").asText(null),
                item.workflowStatus(), item.workflowRowVersion(), item.currentStep(), item.errorCode(), Set.copyOf(allowed),
                item.createdAt(), prepare.has("teamExperienceUsage") ? prepare.get("teamExperienceUsage") : null);
    }

    private record HumanWorkItemData(UUID id, UUID instanceId, String stepId, UUID assigneeId, String status,
            long rowVersion, String itemResult, UUID completedBy, Instant completedAt, Instant createdAt,
            UUID creatorId, String inputJson, String workflowResult, String workflowStatus, long workflowRowVersion,
            String currentStep, Instant deadlineAt, String errorCode, String prepareOutput) { }

    private record TeamExperienceTaskRow(UUID tenantId, UUID actorId, String workflowVersion, String source,
            String instanceStatus, String currentStep, String stepType, String stepStatus, String inputJson) { }

    private record InstanceScope(UUID tenantId, UUID workspaceId) { }

    private WorkflowInstance loadInstanceById(UUID instanceId) {
        var scope = jdbc.query("select tenant_id, workspace_id from workflow.instance where id = ?",
                rs -> rs.next() ? new InstanceScope(rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class)) : null, instanceId);
        if (scope == null) throw EafException.notFound();
        return loadInstance(scope.tenantId(), scope.workspaceId(), instanceId);
    }

    @Transactional
    public java.util.Optional<WorkflowLease> claimOne() {
        var id = jdbc.query("select id from workflow.instance where status in ('QUEUED', 'RUNNING', 'WAITING_CHILD', 'WAITING_HUMAN', 'CANCELLING') "
                        + "and (lease_until is null or lease_until <= now()) and (next_poll_at is null or next_poll_at <= now()) "
                        + "order by created_at for update skip locked limit 1",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null);
        if (id == null) return java.util.Optional.empty();
        var owner = UUID.randomUUID();
        var fence = jdbc.queryForObject("update workflow.instance set status = case when deadline_at <= now() and status <> 'CANCELLING' then 'CANCELLING' when status = 'QUEUED' then 'RUNNING' else status end, "
                        + "cancel_target_status = case when deadline_at <= now() and status <> 'CANCELLING' then 'TIMED_OUT' else cancel_target_status end, "
                        + "error_code = case when deadline_at <= now() and status <> 'CANCELLING' then 'DEADLINE_EXCEEDED' else error_code end, "
                        + "lease_owner_id = ?, lease_fence = lease_fence + 1, lease_until = now() + interval '30 seconds', "
                        + "next_poll_at = null, row_version = row_version + 1, updated_at = now() where id = ? "
                        + "and (lease_until is null or lease_until <= now()) returning lease_fence",
                Long.class, owner, id);
        return java.util.Optional.of(new WorkflowLease(id, owner, fence));
    }

    public WorkflowRuntimeState runtimeState(WorkflowLease lease) {
        // 租约内状态映射与普通读取一致，必须保留服务端质量来源供步骤 Task 继承。
        return jdbc.query("select id, tenant_id, workspace_id, actor_id, initiator_id, delegation_id, authorization_hash, source, quality_run_id, workflow_id, workflow_version, definition_hash, root_budget_scope_id, input_json::text input_json, input_hash, definition_snapshot::text definition_snapshot, dependency_snapshot::text dependency_snapshot, status, result_json::text result_json, current_step_id, root_task_id, error_code, row_version, created_at, deadline_at, waiting_reason, business_effect_status, cancel_target_status, (select s.child_task_id from workflow.step s where s.instance_id = workflow.instance.id and s.step_id = workflow.instance.current_step_id) child_task_id "
                        + "from workflow.instance where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                rs -> rs.next() ? new WorkflowRuntimeState(mapInstance(rs), rs.getString("authorization_hash"), lease) : null,
                lease.instanceId(), lease.ownerId(), lease.fence());
    }

    @Transactional
    public WorkflowStepRuntime prepareStep(WorkflowLease lease, WorkflowStepSpec spec, String inputJson,
                                           String inputHash, String dispatchKey) {
        requireLease(lease);
        jdbc.update("insert into workflow.step(instance_id, tenant_id, workspace_id, step_id, step_type, status, input_json, input_hash, dispatch_key) "
                        + "select id, tenant_id, workspace_id, ?, ?, 'INTENT', ?::jsonb, ?, ? from workflow.instance where id = ? "
                        + "on conflict (instance_id, step_id) do nothing",
                spec.id(), spec.type().name(), inputJson, inputHash, dispatchKey, lease.instanceId());
        var step = loadStep(lease.instanceId(), spec.id());
        if (step == null || !step.stepType().equals(spec.type().name()) || !step.inputHash().equals(inputHash)
                || !step.dispatchKey().equals(dispatchKey))
            throw EafException.conflict("WORKFLOW_STEP_CONFLICT", "已保存步骤意图与恢复输入不匹配。");
        var changed = jdbc.update("update workflow.instance set current_step_id = ?, row_version = row_version + 1, updated_at = now() "
                        + "where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                spec.id(), lease.instanceId(), lease.ownerId(), lease.fence());
        if (changed != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "Workflow Worker 租约已失效。");
        return step;
    }

    @Transactional
    public List<WorkflowService.ParallelBranch> prepareParallelBranches(WorkflowLease lease, WorkflowStepSpec step,
            List<ParallelBranchIntent> intents) {
        requireLease(lease);
        if (step.type() != WorkflowStepType.PARALLEL_READ || intents == null || intents.size() != 2)
            throw EafException.forbidden("只允许固定双分支建立并行意图。");
        var instance = loadInstanceById(lease.instanceId());
        if (!P21_SERVICE_REQUEST_BATCH_WORKFLOW_ID.equals(instance.workflowId())
                || !"1.0.0".equals(instance.workflowVersion()) || !"gather".equals(step.id()))
            throw EafException.forbidden("并行分支不属于固定 Workflow。");
        for (var intent : intents) {
            jdbc.update("insert into workflow.parallel_branch(id, tenant_id, workspace_id, instance_id, group_step_id, role, effective_step_id, dispatch_key, input_json, input_hash, capability_id, capability_version, status) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, 'INTENT') on conflict (instance_id, group_step_id, role) do nothing",
                    UUID.randomUUID(), instance.tenantId(), instance.workspaceId(), instance.id(), step.id(), intent.role(),
                    intent.stepId(), intent.dispatchKey(), intent.inputJson(), intent.inputHash(), intent.capabilityId(),
                    intent.capabilityVersion());
        }
        var rows = parallelBranches(instance.id(), step.id());
        if (rows.size() != 2 || intents.stream().anyMatch(intent -> rows.stream().noneMatch(row ->
                row.role().equals(intent.role()) && row.dispatchKey().equals(intent.dispatchKey())
                        && row.inputHash().equals(intent.inputHash()) && row.capabilityId().equals(intent.capabilityId())
                        && row.capabilityVersion().equals(intent.capabilityVersion()))))
            throw EafException.conflict("PARALLEL_BRANCH_CONFLICT", "并行分支恢复输入与已保存意图不一致。");
        return rows;
    }

    @Transactional
    public io.eaf.task.api.TaskSnapshot createParallelBranchTask(WorkflowLease lease,
            WorkflowService.ParallelBranch branch, io.eaf.task.api.CreateWorkflowTaskCommand command) {
        requireLease(lease);
        var current = jdbc.query("select role, effective_step_id, dispatch_key, input_hash, child_task_id, status from workflow.parallel_branch where instance_id = ? and role = ? for update",
                rs -> rs.next() ? new Object[]{rs.getString("role"), rs.getString("effective_step_id"), rs.getString("dispatch_key"),
                        rs.getString("input_hash"), rs.getObject("child_task_id", UUID.class), rs.getString("status")} : null,
                lease.instanceId(), branch.role());
        if (current == null || !branch.stepId().equals(current[1]) || !branch.dispatchKey().equals(current[2])
                || !branch.inputHash().equals(current[3]) || "CANCELLED".equals(current[5]))
            throw EafException.conflict("PARALLEL_BRANCH_CONFLICT", "并行分支已取消或来源绑定发生变化。");
        var task = tasks.createWorkflowTask(command);
        var linked = (UUID) current[4];
        if (linked != null && !linked.equals(task.id()))
            throw EafException.conflict("PARALLEL_BRANCH_CONFLICT", "并行分支已绑定不同的 Task。");
        jdbc.update("update workflow.parallel_branch set child_task_id = ?, status = 'WAITING_CHILD', updated_at = now() where instance_id = ? and role = ? and (child_task_id is null or child_task_id = ?)",
                task.id(), lease.instanceId(), branch.role(), task.id());
        var instance = loadInstanceById(lease.instanceId());
        //  来源校验要求固定 PARALLEL_READ 步骤与分支同时处于等待态。
        jdbc.update("update workflow.step set status = 'WAITING_CHILD', updated_at = now() where instance_id = ? and step_id = ? and step_type = 'PARALLEL_READ' and status in ('INTENT', 'WAITING_CHILD')",
                lease.instanceId(), instance.currentStepId());
        if (instance.rootTaskId() != null && !instance.rootTaskId().equals(task.rootTaskId()))
            throw EafException.conflict("WORKFLOW_ROOT_CONFLICT", "分支未使用同一根预算 Task。");
        var changed = jdbc.update("update workflow.instance set root_task_id = coalesce(root_task_id, ?), status = 'WAITING_CHILD', waiting_reason = null, lease_owner_id = null, lease_until = null, next_poll_at = now() + interval '1 second', row_version = row_version + 1, updated_at = now() where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                task.rootTaskId(), lease.instanceId(), lease.ownerId(), lease.fence());
        if (changed != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "Workflow Worker 租约已失效。");
        return task;
    }

    public List<WorkflowService.ParallelBranch> parallelBranches(UUID instanceId, String groupStepId) {
        return jdbc.query("select role, effective_step_id, dispatch_key, input_json::text input_json, input_hash, capability_id, capability_version, child_task_id, status, error_code from workflow.parallel_branch where instance_id = ? and group_step_id = ? order by role",
                (rs, row) -> new WorkflowService.ParallelBranch(rs.getString("role"), rs.getString("effective_step_id"),
                        rs.getString("dispatch_key"), rs.getString("input_json"), rs.getString("input_hash"),
                        rs.getObject("capability_id", UUID.class), rs.getString("capability_version"),
                        rs.getObject("child_task_id", UUID.class), rs.getString("status"), rs.getString("error_code")),
                instanceId, groupStepId);
    }

    @Transactional
    public void completeParallelRead(WorkflowLease lease, WorkflowStepSpec step, String outputJson) {
        requireLease(lease);
        var branches = parallelBranches(lease.instanceId(), step.id());
        if (branches.size() != 2 || branches.stream().anyMatch(branch -> !"SUCCEEDED".equals(branch.status())))
            throw EafException.conflict("PARALLEL_BRANCH_INCOMPLETE", "汇合只能在两个分支均成功后提交。");
        completeStep(lease, step.id(), outputJson, step.nextStepId(), "RUNNING", null, "NONE");
    }

    @Transactional
    public void updateParallelBranch(WorkflowLease lease, WorkflowService.ParallelBranch branch,
                                     String status, String errorCode) {
        requireLease(lease);
        if (!Set.of("WAITING_CHILD", "SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT").contains(status))
            throw EafException.invalid("分支状态无效。");
        jdbc.update("update workflow.parallel_branch set status = ?, error_code = ?, updated_at = now() where instance_id = ? and group_step_id = 'gather' and role = ?",
                status, errorCode, lease.instanceId(), branch.role());
    }

    @Transactional
    public boolean cancelParallelBranches(WorkflowInstance instance) {
        var branches = parallelBranches(instance.id(), instance.currentStepId());
        boolean stopped = true;
        for (var branch : branches) {
            var cancellation = tasks.cancelWorkflowTask(instance.tenantId(), instance.workspaceId(), instance.actorId(), branch.dispatchKey());
            if (cancellation.taskId() != null && cancellation.status() != null
                    && !Set.of(io.eaf.task.api.TaskStatus.SUCCEEDED, io.eaf.task.api.TaskStatus.FAILED,
                    io.eaf.task.api.TaskStatus.CANCELLED, io.eaf.task.api.TaskStatus.TIMED_OUT).contains(cancellation.status()))
                stopped = false;
            jdbc.update("update workflow.parallel_branch set status = case when ? then 'CANCELLED' else status end, updated_at = now() where instance_id = ? and role = ? and child_task_id is null",
                    cancellation.taskId() == null, instance.id(), branch.role());
        }
        return stopped;
    }

    @Transactional
    public void linkChildTask(WorkflowLease lease, String stepId, UUID childTaskId, UUID rootTaskId) {
        requireLease(lease);
        var changed = jdbc.update("update workflow.step set child_task_id = ?, status = 'WAITING_CHILD', updated_at = now() "
                        + "where instance_id = ? and step_id = ? and status in ('INTENT', 'WAITING_CHILD') "
                        + "and (child_task_id is null or child_task_id = ?)",
                childTaskId, lease.instanceId(), stepId, childTaskId);
        if (changed != 1) throw EafException.conflict("WORKFLOW_CHILD_CONFLICT", "Workflow 步骤已关联不同的子 Task。");
        var row = jdbc.query("select root_task_id from workflow.instance where id = ? for update",
                rs -> rs.next() ? rs.getObject("root_task_id", UUID.class) : null, lease.instanceId());
        if (row != null && !row.equals(rootTaskId))
            throw EafException.conflict("WORKFLOW_ROOT_CONFLICT", "Workflow 已绑定不同的根 Task。");
        var instanceChanged = jdbc.update("update workflow.instance set root_task_id = coalesce(root_task_id, ?), status = 'WAITING_CHILD', "
                        + "lease_owner_id = null, lease_until = null, next_poll_at = now() + interval '1 second', "
                        + "row_version = row_version + 1, updated_at = now() "
                        + "where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                rootTaskId, lease.instanceId(), lease.ownerId(), lease.fence());
        if (instanceChanged != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "Workflow Worker 租约已失效。");
    }

    @Transactional
    public io.eaf.task.api.TaskSnapshot createProjectBriefTaskAndLink(WorkflowLease lease, String stepId,
            CreateWorkflowTaskCommand command) {
        requireLease(lease);
        var instance = loadInstanceById(lease.instanceId());
        if (!P29_PROJECT_BRIEF_WORKFLOW_ID.equals(instance.workflowId()) || !"prepare".equals(stepId)
                || !stepId.equals(instance.currentStepId()) || command == null || command.workflowProvenance() == null
                || !instance.id().equals(command.workflowProvenance().workflowInstanceId()))
            throw EafException.forbidden("只允许固定 P29 prepare 步骤创建并绑定 Task。");
        var runtime = loadStep(instance.id(), stepId);
        if (runtime == null || runtime.childTaskId() != null || !"INTENT".equals(runtime.status()))
            throw EafException.conflict("WORKFLOW_STEP_CONFLICT", "P29 prepare 步骤已创建或绑定 Task。");
        var existing = tasks.findByIdempotencyKey(command.actor(), instance.workspaceId(), runtime.dispatchKey());
        var child = tasks.createWorkflowTask(command);
        if (existing.isPresent() && !existing.get().id().equals(child.id()))
            throw EafException.conflict("WORKFLOW_CHILD_CONFLICT", "P29 Task 来源键查询与幂等创建返回了不同 Task。");
        linkChildTask(lease, stepId, child.id(), child.rootTaskId());
        return child;
    }

    @Transactional
    public void completeStep(WorkflowLease lease, String stepId, String outputJson,
                             String selectedNextStepId, String instanceStatus, String errorCode,
                             String businessEffectStatus) {
        requireLease(lease);
        jdbc.update("update workflow.step set status = 'SUCCEEDED', output_json = ?::jsonb, selected_next_step_id = ?, updated_at = now() "
                        + "where instance_id = ? and step_id = ? and status in ('INTENT', 'WAITING_CHILD')",
                outputJson, selectedNextStepId, lease.instanceId(), stepId);
        var changed = jdbc.update("update workflow.instance set status = ?, current_step_id = ?, result_json = case when ? = 'SUCCEEDED' then ?::jsonb else result_json end, "
                        + "business_effect_status = case when ? = 'NONE' then business_effect_status else ? end, waiting_reason = null, "
                        + "error_code = ?, lease_owner_id = null, lease_until = null, next_poll_at = null, "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                instanceStatus, selectedNextStepId, instanceStatus, outputJson, businessEffectStatus, businessEffectStatus, errorCode,
                lease.instanceId(), lease.ownerId(), lease.fence());
        if (changed != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "Workflow Worker 租约已失效。");
    }

    @Transactional
    public void releaseWaitingChild(WorkflowLease lease, String stepId, String reason, String businessEffectStatus) {
        requireLease(lease);
        jdbc.update("update workflow.step set status = 'WAITING_CHILD', updated_at = now() where instance_id = ? and step_id = ? and status in ('INTENT','WAITING_CHILD')",
                lease.instanceId(), stepId);
        var changed = jdbc.update("update workflow.instance set status = 'WAITING_CHILD', waiting_reason = ?, "
                        + "business_effect_status = case when ? = 'NONE' then business_effect_status else ? end, "
                        + "lease_owner_id = null, lease_until = null, next_poll_at = now() + interval '1 second', "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                reason, businessEffectStatus, businessEffectStatus, lease.instanceId(), lease.ownerId(), lease.fence());
        if (changed != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "Workflow Worker 租约已失效。");
    }

    @Transactional
    public void release(WorkflowLease lease, String status, String currentStepId) {
        jdbc.update("update workflow.instance set status = ?, current_step_id = coalesce(?, current_step_id), "
                        + "lease_owner_id = null, lease_until = null, next_poll_at = case when ? = 'WAITING_CHILD' then now() + interval '1 second' else null end, "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                status, currentStepId, status, lease.instanceId(), lease.ownerId(), lease.fence());
    }

    @Transactional
    public void fail(WorkflowLease lease, String status, String errorCode) {
        fail(lease, status, errorCode, "NONE");
    }

    @Transactional
    public void fail(WorkflowLease lease, String status, String errorCode, String businessEffectStatus) {
        var changed = jdbc.update("update workflow.instance set status = ?, error_code = ?, waiting_reason = null, cancel_target_status = null, "
                        + "business_effect_status = case when ? = 'NONE' then business_effect_status else ? end, lease_owner_id = null, lease_until = null, next_poll_at = null, "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                status, errorCode, businessEffectStatus, businessEffectStatus, lease.instanceId(), lease.ownerId(), lease.fence());
        if (changed == 1) closeOpenHumanWorkItem(lease.instanceId(), "TIMED_OUT".equals(status) ? "TIMED_OUT" : "FAILED");
    }

    @Transactional
    public void beginCancellation(WorkflowLease lease, String targetStatus, String errorCode) {
        if (!Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(targetStatus))
            throw EafException.invalid("Workflow 取消终态无效。");
        requireLease(lease);
        var state = runtimeState(lease);
        if (state == null) return;
        var cancellation = cancelCurrentChild(state.instance());
        var changed = jdbc.update("update workflow.instance set status = 'CANCELLING', cancel_target_status = ?, error_code = ?, "
                        + "waiting_reason = case when ? then 'EXTERNAL_EFFECT_VERIFYING' else null end, "
                        + "business_effect_status = case when ? = 'NONE' then business_effect_status else ? end, "
                        + "lease_owner_id = null, lease_until = null, lease_fence = lease_fence + 1, next_poll_at = now(), "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                targetStatus, errorCode, cancellation.externalEffectPending(), cancellation.externalEffectStatus(),
                cancellation.externalEffectStatus(), lease.instanceId(), lease.ownerId(), lease.fence());
        if (changed != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "Workflow 取消竞争失败。");
        closeOpenHumanWorkItem(lease.instanceId(), targetStatus);
    }

    @Transactional
    public void releaseCancellation(WorkflowLease lease, String reason, String businessEffectStatus) {
        var changed = jdbc.update("update workflow.instance set status = 'CANCELLING', waiting_reason = ?, "
                        + "business_effect_status = case when ? = 'NONE' then business_effect_status else ? end, "
                        + "lease_owner_id = null, lease_until = null, next_poll_at = now() + interval '1 second', "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                reason, businessEffectStatus, businessEffectStatus, lease.instanceId(), lease.ownerId(), lease.fence());
        if (changed != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "Workflow 取消等待竞争失败。");
    }

    @Transactional
    public void finishCancellation(WorkflowLease lease, String targetStatus, String errorCode, String businessEffectStatus) {
        var changed = jdbc.update("update workflow.instance set status = ?, error_code = ?, waiting_reason = null, cancel_target_status = null, "
                        + "business_effect_status = case when ? = 'NONE' then business_effect_status else ? end, "
                        + "lease_owner_id = null, lease_until = null, next_poll_at = null, row_version = row_version + 1, updated_at = now() "
                        + "where id = ? and status = 'CANCELLING' and cancel_target_status = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now()",
                targetStatus, errorCode, businessEffectStatus, businessEffectStatus, lease.instanceId(), targetStatus,
                lease.ownerId(), lease.fence());
        if (changed != 1) throw EafException.conflict("WORKFLOW_LEASE_LOST", "Workflow 终止竞争失败。");
        closeOpenHumanWorkItem(lease.instanceId(), targetStatus);
    }

    private void closeOpenHumanWorkItem(UUID instanceId, String status) {
        if (!Set.of("CANCELLED", "TIMED_OUT", "FAILED").contains(status)) return;
        var items = jdbc.query("select id, assignee_id from workflow.human_work_item where instance_id = ? and status = 'OPEN'",
                (rs, row) -> new Object[]{rs.getObject("id", UUID.class), rs.getObject("assignee_id", UUID.class)}, instanceId);
        var changed = jdbc.update("update workflow.human_work_item set status = ?, row_version = row_version + 1, updated_at = now() "
                        + "where instance_id = ? and status = 'OPEN'", status, instanceId);
        if (changed > 0) {
            var instance = loadInstanceById(instanceId);
            audit.append(new AuditFact("p16-work-item-closed:" + instanceId + ":" + status,
                    instance.tenantId(), instance.workspaceId(), instance.actorId(), instanceId,
                    "HUMAN_WORK_ITEM_CLOSED", status, "{}", null));
            for (var item : items) recordP16WorkItemEvent((UUID) item[0], status, (UUID) item[1], null);
        }
    }

    private void recordP16WorkItemEvent(UUID workItemId, String change, UUID oldAssigneeId, UUID newAssigneeId) {
        var row = jdbc.query("select h.tenant_id, h.workspace_id, h.row_version, h.status, i.workflow_id, i.workflow_version "
                        + "from workflow.human_work_item h join workflow.instance i on i.id = h.instance_id "
                        + "where h.id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                        rs.getLong("row_version"), rs.getString("status"), rs.getObject("workflow_id", UUID.class),
                        rs.getString("workflow_version")} : null,
                workItemId);
        if (row == null || !P16_SERVICE_REQUEST_WORKFLOW_ID.equals(row[4])
                || !P16_SERVICE_REQUEST_WORKFLOW_VERSION.equals(row[5])) return;
        var version = (long) row[2];
        jdbc.update("insert into workflow.p16_work_item_event(id, tenant_id, workspace_id, source, event_id, event_type, "
                        + "work_item_id, source_row_version, status, old_assignee_id, new_assignee_id, occurred_at) "
                        + "values (?, ?, ?, 'urn:eaf:workflow:p16', ?, 'io.eaf.workflow.p16-work-item.changed.v1', ?, ?, ?, ?, ?, now()) "
                        + "on conflict (source, event_id) do nothing",
                UUID.randomUUID(), row[0], row[1], workItemId + ":" + version, workItemId, version, row[3], oldAssigneeId, newAssigneeId);
    }

    WorkflowTaskCancellation cancelCurrentChild(WorkflowInstance instance) {
        if (instance.currentStepId() == null) return new WorkflowTaskCancellation(null, null, "NONE", false);
        var step = stepRuntime(instance.id(), instance.currentStepId());
        if (step == null || !Set.of("RUN_CAPABILITY", "RUN_TOOL").contains(step.stepType()))
            return new WorkflowTaskCancellation(null, null, "NONE", false);
        var cancellation = tasks.cancelWorkflowTask(instance.tenantId(), instance.workspaceId(), instance.actorId(), step.dispatchKey());
        if (!cancellation.externalEffectPending() && cancellation.taskId() != null)
            executions.cancelAwaitingApproval(instance.tenantId(), instance.workspaceId(), cancellation.taskId());
        return cancellation;
    }

    private void recordCancellationProgress(UUID instanceId, WorkflowTaskCancellation result) {
        jdbc.update("update workflow.instance set waiting_reason = case when ? then 'EXTERNAL_EFFECT_VERIFYING' else null end, "
                        + "business_effect_status = case when ? = 'NONE' then business_effect_status else ? end, updated_at = now() where id = ? and status = 'CANCELLING'",
                result.externalEffectPending(), result.externalEffectStatus(), result.externalEffectStatus(), instanceId);
    }

    public WorkflowStepRuntime stepRuntime(UUID instanceId, String stepId) {
        return loadStep(instanceId, stepId);
    }

    public Map<String, JsonNode> completedOutputs(UUID instanceId) {
        var outputs = new HashMap<String, JsonNode>();
        jdbc.query("select step_id, output_json::text output_json from workflow.step where instance_id = ? and status = 'SUCCEEDED' and output_json is not null",
                rs -> { outputs.put(rs.getString("step_id"), parseJson(rs.getString("output_json"), "step.output")); }, instanceId);
        return Map.copyOf(outputs);
    }

    public void validateWorkflowOutput(WorkflowDefinition definition, JsonNode output) {
        schemas.validateInput(definition.outputSchema(), output);
    }

    public void validateStepOutput(ActorContext actor, UUID workspaceId, WorkflowStepSpec step, JsonNode output) {
        String schemaText;
        if (step.type() == WorkflowStepType.RUN_CAPABILITY) {
            var capability = capabilities.requirePublished(actor, workspaceId, step.capabilityId(), step.capabilityVersion());
            schemaText = skills.requirePublished(actor, workspaceId, capability.skillId(), capability.skillVersion()).outputSchema();
        } else if (step.type() == WorkflowStepType.RUN_TOOL) {
            schemaText = tools.requirePublished(actor.tenantId(), workspaceId, step.toolName(), step.toolVersion()).outputSchema();
        } else {
            throw EafException.invalid("只有执行步骤可以校验外部 Task 输出。");
        }
        schemas.validateDeclaredOutput(schemaText, output);
    }

    public String canonicalWorkflowJson(JsonNode node) {
        return canonical(node);
    }

    private void insertVersion(ActorContext actor, UUID workspaceId, UUID workflowId,
                               String name, String description, CreateWorkflowVersionCommand command) {
        if (command == null) throw EafException.invalid("Workflow 版本不能为空。");
        var version = requireText(command.version(), 40, "version");
        if (!VERSION.matcher(version).matches()) throw EafException.invalid("Workflow version 必须使用固定语义版本格式。");
        var input = schemas.parseWorkflowSchema(command.inputSchema(), "inputSchema");
        var output = schemas.parseWorkflowSchema(command.outputSchema(), "outputSchema");
        var entryStepId = requireText(command.entryStepId(), 64, "entryStepId");
        var steps = command.steps() == null ? List.<WorkflowStepSpec>of() : List.copyOf(command.steps());
        var encodedSteps = canonical(json.valueToTree(steps));
        try {
            jdbc.update("insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema, entry_step_id, steps_json, status) values (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?::jsonb, 'DRAFT')",
                    workflowId, actor.tenantId(), workspaceId, version, canonical(input), canonical(output), entryStepId, encodedSteps);
        } catch (DuplicateKeyException e) {
            throw EafException.conflict("VERSION_CONFLICT", "Workflow 版本已存在。");
        }
        var refs = capabilityRefs(steps);
        for (var reference : refs)
            jdbc.update("insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version, capability_id, capability_version) values (?, ?, ?, ?, ?, ?)",
                    actor.tenantId(), workspaceId, workflowId, version, reference.id(), reference.version());
        // 结构摘要先覆盖定义与显式版本引用；发布时再绑定已验证的内容摘要。
        var draft = new WorkflowDefinition(workflowId, actor.tenantId(), workspaceId, actor.actorId(), name, description,
                version, "DRAFT", 1, canonical(input), canonical(output), entryStepId, steps,
                refs.stream().map(ref -> new WorkflowDependency(ref.id(), ref.version(), null)).toList(), null);
        var hash = Hashing.sha256(canonical(json.valueToTree(new VersionContent(draft.name(), draft.description(),
                draft.version(), input, output, entryStepId, steps, draft.dependencies()))));
        jdbc.update("update workflow.version set content_hash = ? where workflow_id = ? and workspace_id = ? and asset_version = ?",
                hash, workflowId, workspaceId, version);
    }

    // 发布前静态证明图可达、无环且所有映射/依赖均符合固定版本契约。
    private Map<CapabilityKey, CapabilityDefinition> validateDefinition(ActorContext actor, UUID workspaceId,
                                                                         WorkflowDefinition definition) {
        var inputSchema = schemas.parseWorkflowSchema(definition.inputSchema(), "inputSchema");
        var outputSchema = schemas.parseWorkflowSchema(definition.outputSchema(), "outputSchema");
        var steps = definition.steps();
        if (steps == null || steps.size() < 2 || steps.size() > MAX_STEPS)
            throw EafException.invalid("Workflow 必须包含 2 到 " + MAX_STEPS + " 个步骤。");
        var byId = new LinkedHashMap<String, WorkflowStepSpec>();
        for (var step : steps) {
            if (step == null || step.id() == null || !step.id().matches("[a-z][a-z0-9-]{0,63}") || step.type() == null
                    || byId.putIfAbsent(step.id(), step) != null)
                throw EafException.invalid("Workflow 步骤 ID 缺失、格式无效或重复。");
        }
        if (steps.stream().anyMatch(step -> step.type() == WorkflowStepType.HUMAN_TASK)
                && !((P16_SERVICE_REQUEST_WORKFLOW_ID.equals(definition.id())
                && Set.of(P16_SERVICE_REQUEST_WORKFLOW_VERSION, P17_SERVICE_REQUEST_WORKFLOW_VERSION,
                        P18_SERVICE_REQUEST_WORKFLOW_VERSION).contains(definition.version()))
                || (P29_PROJECT_BRIEF_WORKFLOW_ID.equals(definition.id())
                && P29_PROJECT_BRIEF_WORKFLOW_VERSION.equals(definition.version()))))
            throw EafException.forbidden("HUMAN_TASK 只允许出现在固定协作流程中。");
        if (P29_PROJECT_BRIEF_WORKFLOW_ID.equals(definition.id())
                && (!P29_PROJECT_BRIEF_WORKFLOW_VERSION.equals(definition.version())
                || !"prepare".equals(definition.entryStepId())
                || !steps.stream().map(WorkflowStepSpec::id).toList().equals(List.of("prepare", "review", "handoff", "complete"))
                || steps.get(0).type() != WorkflowStepType.RUN_CAPABILITY
                || !P29_PROJECT_BRIEF_CAPABILITY_ID.equals(steps.get(0).capabilityId())
                || !"1.0.0".equals(steps.get(0).capabilityVersion())
                || steps.get(1).type() != WorkflowStepType.HUMAN_TASK
                || steps.get(2).type() != WorkflowStepType.HUMAN_TASK
                || steps.get(3).type() != WorkflowStepType.COMPLETE
                || !"review".equals(steps.get(0).nextStepId())
                || !"handoff".equals(steps.get(1).nextStepId())
                || !"complete".equals(steps.get(2).nextStepId())))
            throw EafException.forbidden("项目简报只能使用 prepare/review/handoff/complete 固定步骤定义。");
        if (steps.stream().anyMatch(step -> step.type() == WorkflowStepType.PARALLEL_READ)
                && (!P21_SERVICE_REQUEST_BATCH_WORKFLOW_ID.equals(definition.id())
                || !"1.0.0".equals(definition.version())))
            throw EafException.forbidden("PARALLEL_READ 只允许出现在固定批次分析流程中。");
        if (!byId.containsKey(definition.entryStepId())) throw EafException.invalid("Workflow 入口步骤不存在。");
        var capabilitiesByKey = new HashMap<CapabilityKey, CapabilityDefinition>();
        var outputSchemas = new HashMap<String, JsonNode>();
        var inputSchemas = new HashMap<String, JsonNode>();
        for (var step : steps) {
            if (step.type() == WorkflowStepType.RUN_CAPABILITY || step.type() == WorkflowStepType.RUN_TOOL) {
                if (step.capabilityId() == null || step.capabilityVersion() == null)
                    throw EafException.invalid("执行步骤必须绑定已发布 Capability 版本。");
                var key = new CapabilityKey(step.capabilityId(), step.capabilityVersion());
                var capability = capabilitiesByKey.computeIfAbsent(key,
                        ignored -> capabilities.requirePublished(actor, workspaceId, key.id(), key.version()));
                var skill = skills.requirePublished(actor, workspaceId, capability.skillId(), capability.skillVersion());
                inputSchemas.put(step.id(), schemas.parseObjectSchema(skill.inputSchema(), "Capability input"));
                outputSchemas.put(step.id(), schemas.parseObjectSchema(skill.outputSchema(), "Capability output"));
                if (step.type() == WorkflowStepType.RUN_TOOL) {
                    workspaces.require(actor, workspaceId, "tool:read");
                    var declared = capability.toolDependencies().stream().anyMatch(tool ->
                            tool.name().equals(step.toolName()) && tool.version().equals(step.toolVersion()));
                    if (!declared) throw EafException.forbidden("RUN_TOOL 只能选择固定 Capability 已声明的 Tool 版本。");
                    var tool = tools.requirePublished(actor.tenantId(), workspaceId, step.toolName(), step.toolVersion());
                    inputSchemas.put(step.id(), schemas.parseObjectSchema(tool.inputSchema(), "Tool input"));
                    outputSchemas.put(step.id(), schemas.parseObjectSchema(tool.outputSchema(), "Tool output"));
                }
            }
        }
        var successors = new HashMap<String, List<String>>();
        var predecessors = new HashMap<String, Set<String>>();
        for (var id : byId.keySet()) { successors.put(id, new ArrayList<>()); predecessors.put(id, new HashSet<>()); }
        for (var step : steps) {
            switch (step.type()) {
                case RUN_CAPABILITY, RUN_TOOL -> {
                    requireExecutionStepFields(step);
                    addEdge(step.id(), step.nextStepId(), byId, successors, predecessors);
                }
                case PARALLEL_READ -> {
                    requireParallelReadFields(definition, step);
                    addEdge(step.id(), step.nextStepId(), byId, successors, predecessors);
                    for (var branch : step.parallelBranches()) {
                        var key = new CapabilityKey(branch.capabilityId(), branch.capabilityVersion());
                        var capability = capabilitiesByKey.computeIfAbsent(key,
                                ignored -> capabilities.requirePublished(actor, workspaceId, key.id(), key.version()));
                        if (!capability.toolDependencies().isEmpty())
                            throw EafException.forbidden("并行分支只允许无业务工具的固定只读 Capability。");
                        var skill = skills.requirePublished(actor, workspaceId, capability.skillId(), capability.skillVersion());
                        var target = schemas.parseObjectSchema(skill.inputSchema(), "branch input");
                        var ancestors = ancestors(step.id(), predecessors);
                        validateMapping(branch.inputMapping(), target, inputSchema, byId, outputSchemas, ancestors);
                    }
                    outputSchemas.put(step.id(), schemas.parseObjectSchema(
                            "{\"type\":\"object\",\"required\":[\"knowledgeTaskId\",\"experienceTaskId\"],\"additionalProperties\":false,\"properties\":{\"knowledgeTaskId\":{\"type\":\"string\"},\"experienceTaskId\":{\"type\":\"string\"}}}",
                            "parallel output"));
                }
                case HUMAN_TASK -> {
                    requireHumanTaskFields(definition, step);
                    inputSchemas.put(step.id(), schemas.parseObjectSchema(P29_PROJECT_BRIEF_WORKFLOW_ID.equals(definition.id())
                            ? P29_HUMAN_TASK_INPUT_SCHEMA : HUMAN_TASK_INPUT_SCHEMA, "HUMAN_TASK input"));
                    outputSchemas.put(step.id(), schemas.parseObjectSchema(P29_PROJECT_BRIEF_WORKFLOW_ID.equals(definition.id())
                            ? "review".equals(step.id()) ? P29_REVIEW_OUTPUT_SCHEMA : P29_HANDOFF_OUTPUT_SCHEMA
                            : HUMAN_TASK_OUTPUT_SCHEMA, "HUMAN_TASK output"));
                    addEdge(step.id(), step.nextStepId(), byId, successors, predecessors);
                }
                case BRANCH -> {
                    requireBranchFields(step);
                    addEdge(step.id(), step.whenTrueStepId(), byId, successors, predecessors);
                    addEdge(step.id(), step.whenFalseStepId(), byId, successors, predecessors);
                    if (step.whenTrueStepId().equals(step.whenFalseStepId()))
                        throw EafException.invalid("BRANCH 两个出口必须指向不同步骤。");
                }
                case COMPLETE -> requireCompleteFields(step);
            }
        }
        if (!predecessors.get(definition.entryStepId()).isEmpty())
            throw EafException.invalid("Workflow 入口步骤不能有前驱。");
        var visited = new HashSet<String>();
        var active = new HashSet<String>();
        visit(definition.entryStepId(), successors, visited, active);
        if (visited.size() != steps.size()) throw EafException.invalid("Workflow 存在不可达步骤。");
        for (var step : steps) {
            var ancestors = ancestors(step.id(), predecessors);
            if (step.type() == WorkflowStepType.RUN_CAPABILITY) {
                validateMapping(step.inputMapping(), inputSchemas.get(step.id()),
                        inputSchema, byId, outputSchemas, ancestors);
            } else if (step.type() == WorkflowStepType.RUN_TOOL) {
                validateMapping(step.inputMapping(), inputSchemas.get(step.id()),
                        inputSchema, byId, outputSchemas, ancestors);
            } else if (step.type() == WorkflowStepType.HUMAN_TASK) {
                validateMapping(step.inputMapping(), inputSchemas.get(step.id()),
                        inputSchema, byId, outputSchemas, ancestors);
            } else if (step.type() == WorkflowStepType.BRANCH) {
                var source = sourceType(step.conditionPath(), inputSchema,
                        byId, outputSchemas, ancestors);
                if (!"string".equals(source.type()) || step.conditionValue() == null
                        || source.enumValues() != null && !source.enumValues().contains(step.conditionValue()))
                    throw EafException.invalid("BRANCH 只允许匹配当前来源 Schema 中声明的字符串值。");
            } else if (step.type() == WorkflowStepType.COMPLETE) {
                validateMapping(step.outputMapping(), outputSchema,
                        inputSchema, byId, outputSchemas, ancestors);
            }
        }
        return Map.copyOf(capabilitiesByKey);
    }

    private void validateMapping(Map<String, String> mappings, JsonNode targetSchema, JsonNode rootInput,
                                 Map<String, WorkflowStepSpec> byId, Map<String, JsonNode> outputSchemas,
                                 Set<String> ancestors) {
        if (mappings == null || mappings.isEmpty()) throw EafException.invalid("步骤字段映射不能为空。");
        var targetProperties = targetSchema.path("properties");
        for (var target : mappings.keySet())
            if (!targetProperties.has(target)) throw EafException.invalid("字段映射目标不在依赖 Schema 中：" + target);
        var required = targetSchema.path("required");
        if (required.isArray()) for (var field : required)
            if (!mappings.containsKey(field.asText())) throw EafException.invalid("字段映射缺少必填目标：" + field.asText());
        for (var mapping : mappings.entrySet()) {
            var source = sourceType(mapping.getValue(), rootInput, byId, outputSchemas, ancestors);
            var targetType = targetProperties.path(mapping.getKey()).path("type").asText();
            if (!source.type().equals(targetType))
                throw EafException.invalid("字段映射源与目标 Schema 类型不一致：" + mapping.getKey());
        }
    }

    private SourceType sourceType(String path, JsonNode rootInput, Map<String, WorkflowStepSpec> byId,
                                  Map<String, JsonNode> outputSchemas,
                                  Set<String> ancestors) {
        if ("literal:true".equals(path) || "literal:false".equals(path)) return new SourceType("boolean", null);
        if (path == null) throw EafException.invalid("字段映射来源不能为空。");
        Matcher input = INPUT_PATH.matcher(path);
        if (input.matches()) return propertyType(rootInput, input.group(1));
        Matcher step = STEP_PATH.matcher(path);
        if (!step.matches()) throw EafException.invalid("字段映射路径格式无效。");
        var sourceStep = step.group(1);
        if (!ancestors.contains(sourceStep)) throw EafException.invalid("字段映射只能读取祖先步骤的输出。");
        if (!byId.containsKey(sourceStep) || !outputSchemas.containsKey(sourceStep))
            throw EafException.invalid("字段映射来源步骤没有可用输出。");
        return propertyType(outputSchemas.get(sourceStep), step.group(2));
    }

    private SourceType propertyType(JsonNode schema, String field) {
        var property = schema.path("properties").path(field);
        if (property.isMissingNode()) throw EafException.invalid("字段映射来源不存在：" + field);
        var enumValues = property.path("enum").isArray()
                ? toStringSet(property.path("enum")) : null;
        return new SourceType(property.path("type").asText(), enumValues);
    }

    private void requireExecutionStepFields(WorkflowStepSpec step) {
        if (step.nextStepId() == null || step.inputMapping() == null || step.inputMapping().isEmpty()
                || step.conditionPath() != null || step.conditionValue() != null
                || step.whenTrueStepId() != null || step.whenFalseStepId() != null
                || step.outputMapping() == null || !step.outputMapping().isEmpty())
            throw EafException.invalid("执行步骤字段与步骤类型不匹配。");
        if (step.parallelBranches() != null) throw EafException.invalid("普通执行步骤不能包含并行分支。");
        if (step.type() == WorkflowStepType.RUN_CAPABILITY && (step.toolName() != null || step.toolVersion() != null))
            throw EafException.invalid("RUN_CAPABILITY 不能携带 Tool 引用。");
        if (step.type() == WorkflowStepType.RUN_TOOL && (step.toolName() == null || step.toolVersion() == null))
            throw EafException.invalid("RUN_TOOL 必须显式绑定 Tool 名称和版本。");
    }

    private void requireHumanTaskFields(WorkflowDefinition definition, WorkflowStepSpec step) {
        var isP16 = P16_SERVICE_REQUEST_WORKFLOW_ID.equals(definition.id());
        var mappings = step.inputMapping();
        var p16Mappings = Map.of("assigneeId", "$.input.assigneeId", "sharedBrief", "$.input.sharedBrief",
                "handlingAdvice", "$.steps.prepare.output.handlingAdvice", "cautions", "$.steps.prepare.output.cautions");
        var p17Mappings = Map.of("assigneeId", "$.input.assigneeId", "sharedBrief", "$.input.sharedBrief",
                "handlingAdvice", "$.steps.prepare.output.handlingAdvice", "cautions", "$.steps.prepare.output.cautions",
                "teamExperienceUsage", "$.steps.prepare.output.teamExperienceUsage");
        var p29AssigneeKey = "review".equals(step.id()) ? "reviewerId" : "recipientId";
        var validP16 = isP16 && "handle".equals(step.id())
                && (P16_SERVICE_REQUEST_WORKFLOW_VERSION.equals(definition.version()) && p16Mappings.equals(mappings)
                || Set.of(P17_SERVICE_REQUEST_WORKFLOW_VERSION, P18_SERVICE_REQUEST_WORKFLOW_VERSION)
                        .contains(definition.version()) && p17Mappings.equals(mappings));
        var validP29 = P29_PROJECT_BRIEF_WORKFLOW_ID.equals(definition.id())
                && P29_PROJECT_BRIEF_WORKFLOW_VERSION.equals(definition.version())
                && Set.of("review", "handoff").contains(step.id()) && mappings != null && mappings.size() == 1
                && ("$.input." + p29AssigneeKey).equals(mappings.get("assigneeId"));
        if ((!validP16 && !validP29) || step.nextStepId() == null
                || step.capabilityId() != null || step.capabilityVersion() != null
                || step.toolName() != null || step.toolVersion() != null
                || step.conditionPath() != null || step.conditionValue() != null
                || step.whenTrueStepId() != null || step.whenFalseStepId() != null
                || step.outputMapping() == null || !step.outputMapping().isEmpty())
            throw EafException.invalid("HUMAN_TASK 只能使用原 P16 或固定 P29 人工步骤映射和后继步骤。");
        if (step.parallelBranches() != null) throw EafException.invalid("人工步骤不能包含并行分支。");
    }

    private void requireBranchFields(WorkflowStepSpec step) {
        if (step.nextStepId() != null || step.capabilityId() != null || step.capabilityVersion() != null
                || step.toolName() != null || step.toolVersion() != null || step.inputMapping() == null
                || !step.inputMapping().isEmpty() || step.outputMapping() == null || !step.outputMapping().isEmpty()
                || step.conditionPath() == null || step.conditionValue() == null
                || step.whenTrueStepId() == null || step.whenFalseStepId() == null)
            throw EafException.invalid("BRANCH 只允许条件和两个出口字段。");
        if (step.parallelBranches() != null) throw EafException.invalid("条件步骤不能包含并行分支。");
    }

    private void requireParallelReadFields(WorkflowDefinition definition, WorkflowStepSpec step) {
        if (!P21_SERVICE_REQUEST_BATCH_WORKFLOW_ID.equals(definition.id()) || !"1.0.0".equals(definition.version())
                || !"gather".equals(step.id()) || !"gather".equals(definition.entryStepId())
                || step.parallelBranches() == null || step.parallelBranches().size() != 2
                || step.nextStepId() == null || step.capabilityId() != null || step.capabilityVersion() != null
                || step.toolName() != null || step.toolVersion() != null || step.inputMapping() == null
                || !step.inputMapping().isEmpty() || step.conditionPath() != null || step.conditionValue() != null
                || step.whenTrueStepId() != null || step.whenFalseStepId() != null
                || step.outputMapping() == null || !step.outputMapping().isEmpty())
            throw EafException.forbidden("并行步骤必须符合固定 gather 双分支定义。");
        var branches = step.parallelBranches();
        if (branches.stream().anyMatch(java.util.Objects::isNull))
            throw EafException.forbidden("并行分支定义包含空分支。");
        var knowledge = branches.stream().filter(branch -> "knowledge".equals(branch.role())).findFirst().orElse(null);
        var experience = branches.stream().filter(branch -> "experience".equals(branch.role())).findFirst().orElse(null);
        if (knowledge == null || experience == null || branches.stream().anyMatch(branch -> branch == null
                || branch.capabilityVersion() == null || !"1.0.0".equals(branch.capabilityVersion())
                || branch.inputMapping() == null)
                || !P21_KNOWLEDGE_CAPABILITY_ID.equals(knowledge.capabilityId())
                || !P21_EXPERIENCE_CAPABILITY_ID.equals(experience.capabilityId())
                || !Set.of("done").contains(step.nextStepId()))
            throw EafException.forbidden("只允许 knowledge/experience 两个固定只读角色。");
    }

    private void requireCompleteFields(WorkflowStepSpec step) {
        if (step.nextStepId() != null || step.capabilityId() != null || step.capabilityVersion() != null
                || step.toolName() != null || step.toolVersion() != null || step.inputMapping() == null
                || !step.inputMapping().isEmpty() || step.conditionPath() != null || step.conditionValue() != null
                || step.whenTrueStepId() != null || step.whenFalseStepId() != null
                || step.outputMapping() == null || step.outputMapping().isEmpty())
            throw EafException.invalid("COMPLETE 只允许输出字段映射。");
        if (step.parallelBranches() != null) throw EafException.invalid("完成步骤不能包含并行分支。");
    }

    private void addEdge(String from, String to, Map<String, WorkflowStepSpec> byId,
                         Map<String, List<String>> successors, Map<String, Set<String>> predecessors) {
        if (to == null || !byId.containsKey(to)) throw EafException.invalid("Workflow 步骤出口目标不存在。");
        successors.get(from).add(to);
        predecessors.get(to).add(from);
    }

    private void visit(String id, Map<String, List<String>> successors, Set<String> visited, Set<String> active) {
        if (active.contains(id)) throw EafException.invalid("Workflow 图不能包含循环。");
        if (!visited.add(id)) return;
        active.add(id);
        for (var next : successors.get(id)) visit(next, successors, visited, active);
        active.remove(id);
    }

    private Set<String> ancestors(String id, Map<String, Set<String>> predecessors) {
        var result = new HashSet<String>();
        var queue = new ArrayDeque<>(predecessors.get(id));
        while (!queue.isEmpty()) {
            var current = queue.removeFirst();
            if (result.add(current)) queue.addAll(predecessors.get(current));
        }
        return result;
    }

    private List<CapabilityKey> capabilityRefs(List<WorkflowStepSpec> steps) {
        if (steps == null) return List.of();
        var refs = new ArrayList<CapabilityKey>();
        for (var step : steps) {
            if (step == null) continue;
            if ((step.type() == WorkflowStepType.RUN_CAPABILITY || step.type() == WorkflowStepType.RUN_TOOL)
                    && step.capabilityId() != null && step.capabilityVersion() != null)
                refs.add(new CapabilityKey(step.capabilityId(), step.capabilityVersion()));
            if (step.type() == WorkflowStepType.PARALLEL_READ && step.parallelBranches() != null)
                for (var branch : step.parallelBranches()) if (branch != null && branch.capabilityId() != null
                        && branch.capabilityVersion() != null)
                    refs.add(new CapabilityKey(branch.capabilityId(), branch.capabilityVersion()));
        }
        return refs.stream().distinct()
                .sorted(Comparator.comparing(value -> value.id().toString() + "@" + value.version())).toList();
    }

    private WorkflowDependency dependency(CapabilityDefinition capability) {
        return new WorkflowDependency(capability.id(), capability.version(), capability.contentHash());
    }

    private String contentHash(WorkflowDefinition definition, List<WorkflowDependency> dependencies) {
        var content = new VersionContent(definition.name(), definition.description(), definition.version(),
                parseJson(definition.inputSchema(), "inputSchema"), parseJson(definition.outputSchema(), "outputSchema"),
                definition.entryStepId(), definition.steps(), dependencies);
        return Hashing.sha256(canonical(json.valueToTree(content)));
    }

    private JsonNode parseJson(String source, String field) {
        try {
            var result = source == null ? null : json.readTree(source);
            if (result == null) throw EafException.invalid(field + " 必须是有效 JSON。");
            return result;
        } catch (JsonProcessingException e) {
            throw EafException.invalid(field + " 必须是有效 JSON。");
        }
    }

    private String canonical(JsonNode node) {
        try { return json.writeValueAsString(sort(node)); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Workflow JSON 无法规范化。", e); }
    }

    private JsonNode sort(JsonNode node) {
        if (node.isObject()) {
            var result = json.createObjectNode();
            var names = new ArrayList<String>();
            node.fieldNames().forEachRemaining(names::add);
            names.sort(String::compareTo);
            for (var name : names) result.set(name, sort(node.get(name)));
            return result;
        }
        if (node.isArray()) {
            var result = json.createArrayNode();
            node.forEach(value -> result.add(sort(value)));
            return result;
        }
        return node.deepCopy();
    }

    private WorkflowDefinition load(UUID tenantId, UUID workspaceId, UUID workflowId, String version) {
        if (version == null || !VERSION.matcher(version).matches()) throw EafException.invalid("Workflow version 格式无效。");
        var row = jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.name, d.description, v.asset_version, v.status, v.row_version, v.input_schema::text input_schema, v.output_schema::text output_schema, v.entry_step_id, v.steps_json::text steps_json, v.content_hash from workflow.definition d join workflow.version v on v.workflow_id = d.id and v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and v.asset_version = ?",
                rs -> rs.next() ? new WorkflowRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("name"),
                        rs.getString("description"), rs.getString("asset_version"), rs.getString("status"), rs.getLong("row_version"),
                        rs.getString("input_schema"), rs.getString("output_schema"), rs.getString("entry_step_id"),
                        rs.getString("steps_json"), rs.getString("content_hash")) : null,
                tenantId, workspaceId, workflowId, version);
        if (row == null) throw EafException.notFound();
        List<WorkflowStepSpec> steps;
        try { steps = json.readValue(row.stepsJson(), new TypeReference<>() { }); }
        catch (JsonProcessingException e) { throw new IllegalStateException("持久 Workflow 步骤无法解析。", e); }
        var dependencies = jdbc.query("select capability_id, capability_version, content_hash from workflow.capability_dependency where tenant_id = ? and workspace_id = ? and workflow_id = ? and workflow_version = ? order by capability_id, capability_version",
                (rs, index) -> new WorkflowDependency(rs.getObject("capability_id", UUID.class), rs.getString("capability_version"), rs.getString("content_hash")),
                tenantId, workspaceId, workflowId, version);
        return new WorkflowDefinition(row.id(), row.tenantId(), row.workspaceId(), row.ownerId(), row.name(), row.description(),
                row.version(), row.status(), row.rowVersion(), row.inputSchema(), row.outputSchema(), row.entryStepId(),
                steps, dependencies, row.contentHash());
    }

    private WorkflowInstance loadInstance(UUID tenantId, UUID workspaceId, UUID instanceId) {
        var result = jdbc.query("select i.id, i.tenant_id, i.workspace_id, i.actor_id, i.initiator_id, i.delegation_id, i.source, i.quality_run_id, i.workflow_id, i.workflow_version, i.definition_hash, i.root_budget_scope_id, i.input_json::text input_json, i.input_hash, i.definition_snapshot::text definition_snapshot, i.dependency_snapshot::text dependency_snapshot, i.status, i.result_json::text result_json, i.current_step_id, i.root_task_id, i.error_code, i.row_version, i.created_at, i.deadline_at, i.waiting_reason, i.business_effect_status, i.cancel_target_status, s.child_task_id "
                        + "from workflow.instance i left join workflow.step s on s.instance_id = i.id and s.step_id = i.current_step_id where i.id = ? and i.tenant_id = ? and i.workspace_id = ?",
                rs -> rs.next() ? mapInstance(rs) : null,
                instanceId, tenantId, workspaceId);
        if (result == null) throw EafException.notFound();
        return result;
    }

    private WorkflowInstance mapInstance(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new WorkflowInstance(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getObject("actor_id", UUID.class), rs.getObject("initiator_id", UUID.class),
                rs.getObject("delegation_id", UUID.class), rs.getString("source"), rs.getObject("workflow_id", UUID.class),
                rs.getString("workflow_version"), rs.getString("definition_hash"), rs.getObject("root_budget_scope_id", UUID.class),
                rs.getString("input_json"), rs.getString("input_hash"), rs.getString("definition_snapshot"),
                rs.getString("dependency_snapshot"), rs.getString("status"), rs.getString("result_json"),
                rs.getString("current_step_id"), rs.getObject("root_task_id", UUID.class), rs.getString("error_code"),
                rs.getLong("row_version"), rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("deadline_at").toInstant(),
                rs.getObject("child_task_id", UUID.class), rs.getString("waiting_reason"), rs.getString("business_effect_status"),
                rs.getString("cancel_target_status"), rs.getObject("quality_run_id", UUID.class));
    }

    private void requireLease(WorkflowLease lease) {
        if (lease == null || jdbc.query("select id from workflow.instance where id = ? and lease_owner_id = ? and lease_fence = ? and lease_until > now() for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null, lease.instanceId(), lease.ownerId(), lease.fence()) == null)
            throw EafException.conflict("WORKFLOW_LEASE_LOST", "Workflow Worker 租约已失效。");
    }

    private WorkflowStepRuntime loadStep(UUID instanceId, String stepId) {
        return jdbc.query("select step_id, step_type, status, input_json::text input_json, input_hash, dispatch_key, child_task_id, output_json::text output_json, selected_next_step_id from workflow.step where instance_id = ? and step_id = ?",
                rs -> rs.next() ? new WorkflowStepRuntime(rs.getString("step_id"), rs.getString("step_type"),
                        rs.getString("status"), rs.getString("input_json"), rs.getString("input_hash"),
                        rs.getString("dispatch_key"), rs.getObject("child_task_id", UUID.class),
                        rs.getString("output_json"), rs.getString("selected_next_step_id")) : null,
                instanceId, stepId);
    }

    private DefinitionRow loadDefinitionRow(UUID tenantId, UUID workspaceId, UUID workflowId) {
        var definition = jdbc.query("select id, tenant_id, workspace_id, owner_id, name, description from workflow.definition where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new DefinitionRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("name"), rs.getString("description")) : null,
                workflowId, tenantId, workspaceId);
        if (definition == null) throw EafException.notFound();
        return definition;
    }

    private void requireOwner(ActorContext actor, UUID ownerId) {
        if (!ownerId.equals(actor.actorId())) throw EafException.forbidden("只有 Workflow Owner 可以执行版本管理操作。");
    }

    private void lockDraftVersion(UUID tenantId, UUID workspaceId, UUID workflowId, String version,
                                  long expectedVersion) {
        var state = jdbc.query("select status, row_version from workflow.version where tenant_id = ? and workspace_id = ? and workflow_id = ? and asset_version = ? for update",
                rs -> rs.next() ? new VersionState(rs.getString("status"), rs.getLong("row_version")) : null,
                tenantId, workspaceId, workflowId, version);
        if (state == null) throw EafException.notFound();
        if (!"DRAFT".equals(state.status()) || state.rowVersion() != expectedVersion)
            throw EafException.conflict("VERSION_CONFLICT", "Workflow 状态或版本已变化。");
    }

    private String requireText(String value, int max, String field) {
        if (value == null || value.isBlank() || value.length() > max || !value.equals(value.trim()))
            throw EafException.invalid(field + " 不能为空、不能含首尾空格且长度不能超过 " + max + "。");
        return value;
    }

    private UUID uuidOrNull(String value) {
        try { return value == null ? null : UUID.fromString(value); }
        catch (IllegalArgumentException invalid) { return null; }
    }

    private Instant instantOrNull(String value) {
        try { return value == null ? null : Instant.parse(value); }
        catch (RuntimeException invalid) { return null; }
    }

    private Set<String> toStringSet(JsonNode values) {
        var result = new HashSet<String>();
        values.forEach(value -> { if (value.isTextual()) result.add(value.asText()); });
        return Set.copyOf(result);
    }

    private record CapabilityKey(UUID id, String version) { }
    private record VersionKey(UUID id, String version) { }
    private record ExistingInstance(UUID id, String requestHash) { }
    private record SourceInstanceKey(UUID id, Instant createdAt) { }
    private record VersionState(String status, long rowVersion) { }
    private record DefinitionRow(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String name, String description) { }
    private record WorkflowRow(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String name, String description,
                               String version, String status, long rowVersion, String inputSchema, String outputSchema,
                               String entryStepId, String stepsJson, String contentHash) { }
    private record SourceType(String type, Set<String> enumValues) { }
    private record ExistingBatch(UUID id, String payloadHash) { }
    private record P27ToolTask(UUID id, UUID tenantId, UUID actorId, String source, UUID qualityRunId,
            String instanceStatus, String currentStepId, UUID workflowId, String workflowVersion,
            String stepId, String stepStatus, UUID childTaskId, String stepInput, String taskStatus) { }
    private record P27HandlingSource(UUID workItemId, long workItemVersion, String requestId,
            String registrationOperationId, String sourceResultHash, UUID completedBy, Instant completedAt,
            String outcome, String summary, String nextAction) { }
    private record P27ResultSyncRow(UUID syncId, UUID actorId, UUID workItemId, long workItemVersion,
            String sourceResultHash, String requestId, String registrationOperationId, UUID stateQueryId,
            String expectedExternalVersion, String bindingVersion, String externalSubjectId, UUID completedBy,
            Instant completedAt, String outcome, String summary, String nextAction, UUID workflowInstanceId,
            String status, String idempotencyKeyHash, String requestHash) { }
    private record P27SyncCursor(UUID id, Instant createdAt) { }
    private record P27QueryRow(long rowVersion, Instant updatedAt, UUID taskId) { }
    private record BatchDispatchRow(UUID id, UUID tenantId, UUID workspaceId, UUID initiatorId,
                                    String status, String terminationTarget, boolean cancelRequested,
                                    int maxActiveItems, int itemCount, Instant deadlineAt) { }
    private record ActiveBatchItem(String itemKey, UUID workflowInstanceId, String workflowStatus, String errorCode) { }
    private record PendingBatchItem(String itemKey, String requestText, String scenarioKey,
                                    String refsJson, String stableCreateKey) { }
    private record BatchRow(UUID id, UUID initiatorId, String status, long version, Instant createdAt,
                            Instant deadlineAt, int maxActiveItems, int itemCount) { }
    private record BatchItemRow(ServiceRequestBatchSnapshot.Item item, String requestText, String scenarioKey,
                                String refsJson) { }
    private record P21BranchRow(UUID tenantId, UUID actorId, String workflowVersion, String source,
            String instanceStatus, String currentStep, String stepStatus, String branchStatus,
            String inputJson, UUID initiatorId) { }
    private record VersionContent(String name, String description, String version, JsonNode inputSchema,
                                  JsonNode outputSchema, String entryStepId, List<WorkflowStepSpec> steps,
                                  List<WorkflowDependency> dependencies) { }
}
// 本文件负责实现的 Workflow 版本治理、静态校验和实例快照创建。
