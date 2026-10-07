package io.eaf.learning.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.execution.api.ExecutionService;
import io.eaf.learning.api.Feedback;
import io.eaf.learning.api.FeedbackOutboxItem;
import io.eaf.learning.api.FeedbackOutboxPage;
import io.eaf.learning.api.FeedbackService;
import io.eaf.learning.api.FeedbackSubmission;
import io.eaf.learning.api.SubmitFeedbackCommand;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcFeedbackService implements FeedbackService {
    private static final Set<String> OUTBOX_STATUSES = Set.of("PENDING", "DELIVERED", "FAILED");
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final TaskService tasks;
    private final AgentCatalog agents;
    private final ExecutionService executions;
    private final RuntimeQuery runtime;
    private final ObjectMapper json;
    private final Clock clock;

    public JdbcFeedbackService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces, TaskService tasks,
                               AgentCatalog agents,
                               ExecutionService executions, RuntimeQuery runtime, ObjectMapper json, Clock clock) {
        this.jdbc = jdbc; this.workspaces = workspaces; this.tasks = tasks; this.agents = agents; this.executions = executions;
        this.runtime = runtime; this.json = json; this.clock = clock;
    }

    @Override
    @Transactional
    public FeedbackSubmission submit(SubmitFeedbackCommand command) {
        validate(command);
        var actor = command.actor();
        if (actor.type() != ActorType.HUMAN) throw EafException.forbidden("只有已认证员工可以提交学习反馈。");
        var access = workspaces.require(actor, command.workspaceId(), "feedback:create");
        var task = tasks.get(actor, command.workspaceId(), command.taskId());
        if (!"USER".equals(task.source())) throw EafException.forbidden("评测或系统任务不能生成学习反馈。");
        var agent = agents.requirePublished(actor.tenantId(), command.workspaceId(), task.agentId(), task.agentVersion());
        if ("EXPERIENCE_DRAFT_V1".equals(agent.responseProfile()))
            throw EafException.forbidden("个人整理 Task 不能再生成反馈或经验来源。");
        // Task 的公开 Evidence 是质量运行标记来源；带验收用途的 USER Task 也不能进入学习。
        if (tasks.evidence(actor.tenantId(), command.workspaceId(), task.id()).qualityRunId() != null)
            throw EafException.forbidden("带质量运行标记的验收 Task 不能生成学习反馈。");

        var correction = command.correction().trim();
        var evidence = command.evidence().trim();
        var requestHash = Hashing.sha256(writeJson(new RequestBody(correction, evidence, command.executionId())));
        var existing = findByKey(actor.tenantId(), access.workspaceId(), task.id(), actor.actorId(), command.idempotencyKey());
        if (existing.isPresent()) return sameCommand(existing.get(), requestHash);
        if (!terminal(task.status())) throw EafException.conflict("FEEDBACK_TASK_NOT_TERMINAL", "Task 尚未完成，暂不能提交反馈。");

        var executionEvidence = verifyExecution(actor, command.workspaceId(), task, command.executionId());
        var source = new Feedback.Source(task.agentId(), task.agentVersion(), task.promptVersion(), task.assetBinding(),
                runtime.contextSources(actor, command.workspaceId(), task.id()), executionEvidence);
        var id = UUID.randomUUID();
        var createdAt = Instant.now(clock);
        var feedback = new Feedback(id, task.tenantId(), task.workspaceId(), task.id(), task.attempt(),
                task.status().name(), actor.actorId(), task.source(), correction, evidence, source, createdAt);
        var inserted = jdbc.update("insert into learning.feedback(id, tenant_id, workspace_id, task_id, task_attempt, task_status, actor_id, source_type, correction, evidence, source_snapshot, idempotency_key, request_hash, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?) on conflict (tenant_id, workspace_id, task_id, actor_id, idempotency_key) do nothing",
                feedback.id(), feedback.tenantId(), feedback.workspaceId(), feedback.taskId(), feedback.taskAttempt(),
                feedback.taskStatus(), feedback.actorId(), feedback.sourceType(), feedback.correction(), feedback.evidence(),
                writeJson(feedback.source()), command.idempotencyKey(), requestHash, Timestamp.from(createdAt));
        if (inserted == 0) {
            var concurrent = findByKey(actor.tenantId(), access.workspaceId(), task.id(), actor.actorId(), command.idempotencyKey())
                    .orElseThrow(() -> EafException.conflict("IDEMPOTENCY_CONFLICT", "反馈幂等键已被占用。"));
            return sameCommand(concurrent, requestHash);
        }
        // Outbox 与反馈同事务提交，事件仅携带来源 ID；后续候选流程再经审核读取反馈正文。
        var payload = new FeedbackEvent(id, task.id(), task.source(), task.attempt());
        jdbc.update("insert into learning.outbox(event_id, event_type, payload) values (?, 'eaf.feedback.recorded.v1', ?::jsonb)",
                id, writeJson(payload));
        return new FeedbackSubmission(feedback, true);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Feedback> list(io.eaf.shared.ActorContext actor, UUID workspaceId, UUID taskId) {
        tasks.get(actor, workspaceId, taskId);
        return jdbc.query("select id, tenant_id, workspace_id, task_id, task_attempt, task_status, actor_id, source_type, correction, evidence, source_snapshot::text, created_at from learning.feedback where tenant_id = ? and workspace_id = ? and task_id = ? order by created_at, id",
                (rs, row) -> map(rs), actor.tenantId(), workspaceId, taskId);
    }

    @Override
    @Transactional(readOnly = true)
    public Feedback requireExperienceSource(io.eaf.shared.ActorContext actor, UUID workspaceId, UUID feedbackId) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated() || feedbackId == null)
            throw EafException.forbidden("个人经验来源只允许本人 HUMAN 操作者读取。");
        var access = workspaces.require(actor, workspaceId, "feedback:create");
        var feedback = jdbc.query("select id, tenant_id, workspace_id, task_id, task_attempt, task_status, actor_id, source_type, correction, evidence, source_snapshot::text, created_at "
                        + "from learning.feedback where id = ? and tenant_id = ? and workspace_id = ? and actor_id = ?",
                rs -> rs.next() ? map(rs) : null, feedbackId, access.tenantId(), workspaceId, actor.actorId());
        if (feedback == null) throw EafException.notFound();
        var task = tasks.get(actor, workspaceId, feedback.taskId());
        var evidence = tasks.evidence(actor.tenantId(), workspaceId, task.id());
        var agent = agents.requirePublished(actor.tenantId(), workspaceId, feedback.source().agentId(),
                feedback.source().agentVersion());
        if (!"USER".equals(feedback.sourceType()) || !"USER".equals(task.source())
                || !"AGENT".equals(task.runKind()) || !terminal(task.status()) || evidence.qualityRunId() != null)
            throw EafException.forbidden("评测、非用户或整理任务反馈不能作为个人经验来源。");
        if (!Set.of("KNOWLEDGE_QA_V1", "CONVERSATIONAL_KNOWLEDGE_QA_V1", "CONVERSATIONAL_KNOWLEDGE_QA_V2",
                "CONVERSATIONAL_CUSTOMER_FOLLOWUP_V1", "CONVERSATIONAL_CUSTOMER_FOLLOWUP_V2",
                "CUSTOMER_RISK_V1", "CUSTOMER_FOLLOWUP_V1").contains(agent.responseProfile()))
            throw EafException.forbidden("只有已发布的问答或客户分析反馈可以整理为个人经验。");
        return feedback;
    }

    @Override
    public FeedbackOutboxPage listOutboxOperations(io.eaf.shared.ActorContext actor, UUID workspaceId,
                                                   Set<String> statuses, Instant createdAfter,
                                                   Instant cursorCreatedAt, UUID cursorEventId, int pageSize) {
        var access = workspaces.require(actor, workspaceId, "learning:read");
        // 学习事件只向具备 Task 当前读取权的本人 HUMAN 管理员开放，避免撤权后经反馈来源绕读 Task。
        if (actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("只有本人操作的 HUMAN 管理员可以读取学习 outbox。 ");
        workspaces.require(actor, workspaceId, "task:read");
        if (pageSize < 1 || pageSize > 100 || (cursorCreatedAt == null) != (cursorEventId == null)
                || statuses != null && !OUTBOX_STATUSES.containsAll(statuses))
            throw EafException.invalid("Learning Outbox 运维分页或状态过滤无效。");
        var where = new StringBuilder(" where f.tenant_id = ? and f.workspace_id = ?");
        var filters = new java.util.ArrayList<Object>();
        filters.add(access.tenantId());
        filters.add(workspaceId);
        if (statuses != null) {
            if (statuses.isEmpty()) where.append(" and 1 = 0");
            else {
                where.append(" and o.status in (").append(String.join(",", java.util.Collections.nCopies(statuses.size(), "?"))).append(')');
                statuses.forEach(filters::add);
            }
        }
        if (createdAfter != null) {
            where.append(" and o.created_at >= ?");
            filters.add(Timestamp.from(createdAfter));
        }
        var from = " from learning.outbox o join learning.feedback f on f.id = o.event_id";
        var totalSize = jdbc.queryForObject("select count(*)" + from + where, Long.class, filters.toArray());
        var pageWhere = new StringBuilder(where);
        var pageArgs = new java.util.ArrayList<>(filters);
        if (cursorCreatedAt != null) {
            pageWhere.append(" and (o.created_at, o.event_id) < (?, ?)");
            pageArgs.add(Timestamp.from(cursorCreatedAt));
            pageArgs.add(cursorEventId);
        }
        pageArgs.add(pageSize + 1);
        // 反馈正文保留在 Learning 审核流程；值班页只显示来源 Task、事件状态与创建时间。
        var selected = jdbc.query("select o.event_id, f.task_id, f.source_type, o.event_type, o.status, o.created_at"
                        + from + pageWhere + " order by o.created_at desc, o.event_id desc limit ?",
                (rs, row) -> new FeedbackOutboxItem(rs.getObject("event_id", UUID.class),
                        rs.getObject("task_id", UUID.class), rs.getString("source_type"), rs.getString("event_type"),
                        rs.getString("status"), rs.getTimestamp("created_at").toInstant()), pageArgs.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        // 当前没有消费者实现或启用配置，PENDING 表示待接入消费，不是已启用投递器故障。
        return new FeedbackOutboxPage(items, totalSize == null ? 0 : totalSize,
                last == null ? null : last.createdAt(), last == null ? null : last.eventId(),
                "NOT_CONFIGURED", false);
    }

    private Feedback.ExecutionEvidence verifyExecution(io.eaf.shared.ActorContext actor, UUID workspaceId,
                                                        TaskSnapshot task, UUID executionId) {
        if (executionId == null) return null;
        var execution = executions.get(actor, workspaceId, executionId);
        if (!task.id().equals(execution.taskId()) || !workspaceId.equals(execution.workspaceId())
                || !actor.tenantId().equals(execution.tenantId())) throw EafException.notFound();
        var verified = "SUCCEEDED".equals(execution.status()) && execution.verificationJson() != null
                && !execution.verificationJson().isBlank();
        return new Feedback.ExecutionEvidence(execution.id(), execution.attempt(), execution.status(), verified);
    }

    private FeedbackSubmission sameCommand(Feedback existing, String requestHash) {
        var storedHash = jdbc.queryForObject("select request_hash from learning.feedback where id = ?", String.class, existing.id());
        if (!requestHash.equals(storedHash)) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一反馈幂等键对应了不同内容。");
        return new FeedbackSubmission(existing, false);
    }

    private java.util.Optional<Feedback> findByKey(UUID tenantId, UUID workspaceId, UUID taskId,
                                                    UUID actorId, String idempotencyKey) {
        return jdbc.query("select id, tenant_id, workspace_id, task_id, task_attempt, task_status, actor_id, source_type, correction, evidence, source_snapshot::text, created_at from learning.feedback where tenant_id = ? and workspace_id = ? and task_id = ? and actor_id = ? and idempotency_key = ?",
                rs -> rs.next() ? java.util.Optional.of(map(rs)) : java.util.Optional.empty(),
                tenantId, workspaceId, taskId, actorId, idempotencyKey);
    }

    private Feedback map(java.sql.ResultSet rs) throws java.sql.SQLException {
        try {
            return new Feedback(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                    rs.getObject("workspace_id", UUID.class), rs.getObject("task_id", UUID.class),
                    rs.getInt("task_attempt"), rs.getString("task_status"), rs.getObject("actor_id", UUID.class),
                    rs.getString("source_type"), rs.getString("correction"), rs.getString("evidence"),
                    json.readValue(rs.getString("source_snapshot"), Feedback.Source.class),
                    rs.getTimestamp("created_at").toInstant());
        } catch (JsonProcessingException invalidSnapshot) {
            throw new IllegalStateException("学习反馈来源快照无法读取。", invalidSnapshot);
        }
    }

    private String writeJson(Object value) {
        try { return json.writeValueAsString(value); }
        catch (JsonProcessingException invalidValue) { throw new IllegalStateException("学习反馈无法序列化。", invalidValue); }
    }

    private void validate(SubmitFeedbackCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null || command.taskId() == null
                || blank(command.correction()) || command.correction().length() > 4_000
                || blank(command.evidence()) || command.evidence().length() > 4_000
                || blank(command.idempotencyKey()) || command.idempotencyKey().length() > 200
                || hasControl(command.idempotencyKey()))
            throw EafException.invalid("反馈需要 Task、纠正内容、证据和有效幂等键。");
    }

    private boolean terminal(TaskStatus status) {
        return status == TaskStatus.SUCCEEDED || status == TaskStatus.FAILED
                || status == TaskStatus.TIMED_OUT || status == TaskStatus.CANCELLED;
    }

    private boolean blank(String value) { return value == null || value.isBlank(); }
    private boolean hasControl(String value) { return value.chars().anyMatch(ch -> ch < 0x20 || ch == 0x7f); }

    private record RequestBody(String correction, String evidence, UUID executionId) { }
    private record FeedbackEvent(UUID feedbackId, UUID taskId, String sourceType, int taskAttempt) { }
}
// 反馈是人工陈述而非事实；来源从受权 Task、Runtime 和 Execution API 派生，只有后续审核才能将其转为候选。
