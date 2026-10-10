package io.eaf.workflow.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.agent.api.AgentDefinition;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.identity.api.IdentityDirectory;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.task.api.CreateAutomationReadTaskCommand;
import io.eaf.task.api.P30AutomationTaskSourceVerifier;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.workflow.api.AutomationDigestItem;
import io.eaf.workflow.api.AutomationRun;
import io.eaf.workflow.api.AutomationRunPage;
import io.eaf.workflow.api.AutomationSubscription;
import io.eaf.workflow.api.AutomationSubscriptionPage;
import io.eaf.workflow.api.AutomationTaskSource;
import io.eaf.workflow.api.CreateAutomationSubscriptionCommand;
import io.eaf.workflow.api.P16DigestSource;
import io.eaf.workflow.api.WorkflowAutomationService;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.sql.Timestamp;
import java.sql.Time;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcWorkflowAutomationService implements WorkflowAutomationService, P30AutomationTaskSourceVerifier {
    private static final Logger LOG = Logger.getLogger(JdbcWorkflowAutomationService.class.getName());
    private static final UUID AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000024");
    private static final UUID CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000024");
    private static final UUID SKILL_ID = UUID.fromString("53000000-0000-4000-8000-000000000024");
    private static final String VERSION = "1.0.0";
    private static final String PROFILE = "MY_P16_WORK_DIGEST_RESPONSE_V1";
    private static final String EVENT_SOURCE = "urn:eaf:workflow:p16";
    private static final String EVENT_TYPE = "io.eaf.workflow.p16-work-item.changed.v1";
    private static final Duration GRACE = Duration.ofMinutes(15);
    private static final Set<String> TERMINAL_TASKS = Set.of("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT");
    private static final Set<String> REQUIRED_ACTIONS = Set.of("workflow:automation:manage", "task:create", "task:read",
            "task:cancel", "work-item:read", "capability:read", "skill:read", "prompt:read");
    private static final String SUB_COLUMNS = "id, tenant_id, workspace_id, owner_id, name, trigger_kind, day_of_week, "
            + "local_time, time_zone, work_item_id, max_items, expires_at, max_runs, admitted_runs, next_fire_at, status, "
            + "reason, authorization_epoch, epoch_started_at, active_run_id, last_observed_source_row_version, request_hash, "
            + "agent_id, agent_version, capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash, "
            + "profile_hash, row_version, created_at, updated_at";
    private static final String RUN_COLUMNS = "id, tenant_id, workspace_id, owner_id, subscription_id, authorization_epoch, "
            + "trigger_kind, trigger_key, planned_at, occurred_at, event_source, event_id, source_row_version, status, reason, "
            + "input_snapshot::text input_snapshot, input_hash, task_id, task_status, attempts, next_attempt_at, "
            + "generation_attempted, generation_task_id, generation_attempt, result_json::text result_json, "
            + "model_result_json::text model_result_json, result_markdown, admitted_at, finished_at, created_at";

    private final JdbcTemplate jdbc;
    private final WorkflowService workflows;
    private final WorkspaceAuthorization workspaces;
    private final IdentityDirectory identities;
    private final CapabilityService capabilities;
    private final AgentCatalog agents;
    private final TaskService tasks;
    private final ObjectMapper json;
    private final Clock clock;
    private final boolean deterministicMode;
    private final TransactionTemplate transactions;

    public JdbcWorkflowAutomationService(JdbcTemplate jdbc, WorkflowService workflows, WorkspaceAuthorization workspaces,
            IdentityDirectory identities, CapabilityService capabilities, AgentCatalog agents, TaskService tasks,
            ObjectMapper json, Clock clock, PlatformTransactionManager transactionManager,
            @Value("${eaf.model.mode:deterministic}") String modelMode) {
        this.jdbc = jdbc;
        this.workflows = workflows;
        this.workspaces = workspaces;
        this.identities = identities;
        this.capabilities = capabilities;
        this.agents = agents;
        this.tasks = tasks;
        this.json = json;
        this.clock = clock;
        this.deterministicMode = "deterministic".equalsIgnoreCase(modelMode);
        this.transactions = new TransactionTemplate(transactionManager);
        this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Override
    @Transactional
    public AutomationSubscription create(CreateAutomationSubscriptionCommand command) {
        if (command == null) throw EafException.invalid("自动化订阅请求不能为空。");
        var actor = command.actor();
        requireDirectHuman(actor);
        workspaces.requireActionForUpdate(actor, command.workspaceId(), "workflow:automation:manage");
        requireCurrentActions(actor, command.workspaceId());
        var now = Instant.now(clock);
        var name = command.name() == null ? "" : command.name().trim();
        if (name.isEmpty() || name.length() > 120 || command.expiresAt() == null
                || !command.expiresAt().isAfter(now) || command.expiresAt().isAfter(now.plus(Duration.ofDays(90)))
                || command.maxRuns() < 1 || command.maxRuns() > 50)
            throw EafException.invalid("订阅名称、有效期或累计运行上限无效。");
        if (!deterministicMode)
            throw EafException.conflict("AUTOMATION_MODE_UNSUPPORTED", "当前服务未运行 deterministic 模式，不能创建自动摘要订阅。");
        var weekly = "WEEKLY".equals(command.triggerKind());
        var event = "P16_WORK_ITEM_CHANGED".equals(command.triggerKind());
        if (!weekly && !event) throw EafException.invalid("仅支持 WEEKLY 或 P16_WORK_ITEM_CHANGED 触发方式。");
        DayOfWeek day = command.dayOfWeek();
        LocalTime time = command.localTime();
        ZoneId zone = null;
        Instant nextFire = null;
        long sourceVersion = 0;
        UUID workItemId = command.workItemId();
        int maxItems = command.maxItems();
        if (weekly) {
            if (day == null || time == null || command.timeZone() == null || workItemId != null
                    || maxItems < 1 || maxItems > 20) throw EafException.invalid("周订阅的日期、时间、时区或来源范围无效。");
            try { zone = ZoneId.of(command.timeZone()); }
            catch (RuntimeException invalid) { throw EafException.invalid("timeZone 必须是有效 IANA ZoneId。"); }
            nextFire = nextSlotAfter(now, day, time, zone).instant();
            if (!nextFire.isBefore(command.expiresAt())) throw EafException.invalid("订阅有效期内没有未来周计划槽。");
        } else {
            if (workItemId == null || day != null || time != null || command.timeZone() != null
                    || maxItems != 0 && maxItems != 1) throw EafException.invalid("事件订阅必须且只能绑定一个本人 P16 工作项。");
            maxItems = 1;
            var source = workflows.readP16DigestSource(actor, command.workspaceId(), workItemId);
            if (!"OPEN".equals(source.status()) || !actor.actorId().equals(source.assigneeId()))
                throw EafException.forbidden("事件订阅只允许选择本人当前受派的 OPEN P16 工作项。");
            sourceVersion = source.rowVersion();
        }
        var fixed = fixedAssets(actor, command.workspaceId());
        var requestHash = Hashing.sha256(String.join("\u001f", actor.actorId().toString(), command.workspaceId().toString(),
                name, command.triggerKind(), String.valueOf(day), String.valueOf(time), zone == null ? "" : zone.getId(),
                String.valueOf(workItemId), String.valueOf(maxItems), command.expiresAt().toString(),
                Integer.toString(command.maxRuns()), fixed.capability().contentHash(), fixed.capability().skillContentHash()));
        var keyHash = idempotencyHash(actor, command.workspaceId(), command.idempotencyKey());
        var replay = replayAction(actor, command.workspaceId(), keyHash, requestHash);
        if (replay != null) return replay;
        var activeCount = jdbc.queryForObject("select count(*) from workflow.automation_subscription where tenant_id = ? and workspace_id = ? "
                        + "and owner_id = ? and status <> 'DELETED'", Integer.class,
                actor.tenantId(), command.workspaceId(), actor.actorId());
        if (activeCount != null && activeCount >= 5)
            throw EafException.conflict("AUTOMATION_SUBSCRIPTION_LIMIT", "本人最多保留 5 个未删除自动化订阅。");
        var id = UUID.randomUUID();
        try {
            jdbc.update("insert into workflow.automation_subscription(id, tenant_id, workspace_id, owner_id, name, trigger_kind, "
                            + "day_of_week, local_time, time_zone, work_item_id, max_items, expires_at, max_runs, next_fire_at, status, "
                            + "epoch_started_at, last_observed_source_row_version, request_hash, agent_id, agent_version, capability_id, "
                            + "capability_version, capability_hash, skill_id, skill_version, skill_hash, profile_hash) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, '1.0.0', ?, '1.0.0', ?, ?, '1.0.0', ?, ?)",
                    id, actor.tenantId(), command.workspaceId(), actor.actorId(), name, command.triggerKind(),
                    weekly ? day.name() : null, weekly ? time : null, zone == null ? null : zone.getId(), workItemId,
                    maxItems, Timestamp.from(command.expiresAt()), command.maxRuns(), nextFire == null ? null : Timestamp.from(nextFire),
                    Timestamp.from(now), event ? sourceVersion : null, requestHash, AGENT_ID, CAPABILITY_ID,
                    fixed.capability().contentHash(), SKILL_ID, fixed.capability().skillContentHash(), profileHash(fixed));
        } catch (DuplicateKeyException duplicate) {
            throw EafException.conflict("AUTOMATION_SUBSCRIPTION_LIMIT", "自动化订阅已并发创建或达到本人上限。");
        }
        var result = subscriptionSnapshot(requireSubscription(actor.tenantId(), command.workspaceId(), actor.actorId(), id, false));
        saveAction(actor, command.workspaceId(), keyHash, requestHash, id, result);
        return result;
    }

    @Override
    @Transactional(readOnly = true)
    public AutomationSubscriptionPage list(ActorContext actor, UUID workspaceId, Instant cursorCreatedAt,
            UUID cursorId, int pageSize) {
        requireDirectHuman(actor);
        requireManager(actor, workspaceId);
        if (pageSize < 1 || pageSize > 50 || (cursorCreatedAt == null) != (cursorId == null))
            throw EafException.invalid("自动化订阅分页参数无效。");
        var ids = jdbc.query("select id, created_at from workflow.automation_subscription where tenant_id = ? and workspace_id = ? "
                        + "and owner_id = ? and status <> 'DELETED' and (?::timestamptz is null or (created_at, id) < (?, ?)) "
                        + "order by created_at desc, id desc limit ?",
                (rs, row) -> new Object[]{rs.getObject("id", UUID.class), instant(rs, "created_at")}, actor.tenantId(),
                workspaceId, actor.actorId(), cursorCreatedAt == null ? null : Timestamp.from(cursorCreatedAt),
                cursorCreatedAt == null ? Timestamp.from(Instant.EPOCH) : Timestamp.from(cursorCreatedAt),
                cursorId == null ? new UUID(0, 0) : cursorId, pageSize + 1);
        var more = ids.size() > pageSize;
        var selected = more ? ids.subList(0, pageSize) : ids;
        var items = selected.stream().map(item -> subscriptionSnapshot(requireSubscription(actor.tenantId(), workspaceId,
                actor.actorId(), (UUID) item[0], false))).toList();
        var last = more ? selected.get(selected.size() - 1) : null;
        return new AutomationSubscriptionPage(items, last == null ? null : (Instant) last[1], last == null ? null : (UUID) last[0]);
    }

    @Override
    @Transactional(readOnly = true)
    public AutomationSubscription get(ActorContext actor, UUID workspaceId, UUID subscriptionId) {
        requireDirectHuman(actor);
        requireManager(actor, workspaceId);
        return subscriptionSnapshot(requireSubscription(actor.tenantId(), workspaceId, actor.actorId(), subscriptionId, false));
    }

    @Override
    @Transactional
    public AutomationSubscription pause(ActorContext actor, UUID workspaceId, UUID subscriptionId,
            long expectedVersion, String idempotencyKey) {
        return changeState(actor, workspaceId, subscriptionId, expectedVersion, idempotencyKey, "PAUSE");
    }

    @Override
    @Transactional
    public AutomationSubscription resume(ActorContext actor, UUID workspaceId, UUID subscriptionId,
            long expectedVersion, String idempotencyKey) {
        return changeState(actor, workspaceId, subscriptionId, expectedVersion, idempotencyKey, "RESUME");
    }

    @Override
    @Transactional
    public AutomationSubscription delete(ActorContext actor, UUID workspaceId, UUID subscriptionId,
            long expectedVersion, String idempotencyKey) {
        return changeState(actor, workspaceId, subscriptionId, expectedVersion, idempotencyKey, "DELETE");
    }

    private AutomationSubscription changeState(ActorContext actor, UUID workspaceId, UUID subscriptionId,
            long expectedVersion, String idempotencyKey, String action) {
        requireDirectHuman(actor);
        requireManager(actor, workspaceId);
        var keyHash = idempotencyHash(actor, workspaceId, idempotencyKey);
        var requestHash = Hashing.sha256(String.join("\u001f", action, subscriptionId.toString(), Long.toString(expectedVersion)));
        var replay = replayAction(actor, workspaceId, keyHash, requestHash);
        if (replay != null) return replay;
        var sub = requireSubscription(actor.tenantId(), workspaceId, actor.actorId(), subscriptionId, true);
        if (sub.rowVersion() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "自动化订阅版本已变化。");
        var now = Instant.now(clock);
        UUID cancelTask = null;
        if ("PAUSE".equals(action)) {
            if (!"ACTIVE".equals(sub.status())) throw EafException.conflict("AUTOMATION_STATE_CONFLICT", "只有 ACTIVE 订阅可以暂停。");
            cancelTask = taskId(sub.activeRunId());
            jdbc.update("update workflow.automation_subscription set status = 'PAUSED', next_fire_at = null, reason = null, "
                            + "authorization_epoch = authorization_epoch + 1, row_version = row_version + 1, updated_at = ? "
                            + "where id = ? and row_version = ?", Timestamp.from(now), sub.id(), expectedVersion);
            skipReadyRuns(sub.id(), "PAUSED");
        } else if ("RESUME".equals(action)) {
            if (!Set.of("PAUSED", "BLOCKED").contains(sub.status()))
                throw EafException.conflict("AUTOMATION_STATE_CONFLICT", "只有 PAUSED 或 BLOCKED 订阅可以显式恢复。");
            if (!sub.expiresAt().isAfter(now)) throw EafException.conflict("AUTOMATION_EXPIRED", "订阅已到期，需创建新订阅。");
            if (sub.admittedRuns() >= sub.maxRuns()) throw EafException.conflict("AUTOMATION_EXHAUSTED", "累计运行次数已耗尽，需创建新订阅。");
            requireCurrentActions(actor, workspaceId);
            var next = "WEEKLY".equals(sub.triggerKind())
                    ? nextSlotAfter(now, DayOfWeek.valueOf(sub.dayOfWeek()), LocalTime.parse(sub.localTime()), ZoneId.of(sub.timeZone())).instant()
                    : null;
            if (next != null && !next.isBefore(sub.expiresAt())) throw EafException.conflict("AUTOMATION_NO_FUTURE_SLOT", "有效期内没有未来周计划槽。");
            if ("P16_WORK_ITEM_CHANGED".equals(sub.triggerKind())) {
                var source = workflows.readP16DigestSource(actor, workspaceId, sub.workItemId());
                if (!actor.actorId().equals(source.assigneeId()) || !"OPEN".equals(source.status()))
                    throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "原工作项已变化或不再受派，不能恢复订阅。");
            }
            fixedAssets(actor, workspaceId);
            jdbc.update("update workflow.automation_subscription set status = 'ACTIVE', reason = null, next_fire_at = ?, "
                            + "epoch_started_at = ?, last_observed_source_row_version = case when trigger_kind = 'P16_WORK_ITEM_CHANGED' "
                            + "then (select row_version from workflow.human_work_item where id = work_item_id) else last_observed_source_row_version end, "
                            + "authorization_epoch = authorization_epoch + 1, row_version = row_version + 1, updated_at = ? "
                            + "where id = ? and row_version = ?",
                    next == null ? null : Timestamp.from(next), Timestamp.from(now), Timestamp.from(now), sub.id(), expectedVersion);
        } else {
            if ("DELETED".equals(sub.status())) throw EafException.conflict("AUTOMATION_STATE_CONFLICT", "订阅已删除。");
            cancelTask = taskId(sub.activeRunId());
            jdbc.update("update workflow.automation_subscription set status = 'DELETED', next_fire_at = null, reason = null, "
                            + "authorization_epoch = authorization_epoch + 1, row_version = row_version + 1, updated_at = ? "
                            + "where id = ? and row_version = ?", Timestamp.from(now), sub.id(), expectedVersion);
            skipReadyRuns(sub.id(), "DELETED");
        }
        var result = subscriptionSnapshot(requireSubscription(actor.tenantId(), workspaceId, actor.actorId(), sub.id(), false));
        saveAction(actor, workspaceId, keyHash, requestHash, sub.id(), result);
        if (cancelTask != null) cancelAfterCommit(actor, workspaceId, cancelTask);
        return result;
    }

    @Override
    @Transactional(readOnly = true)
    public AutomationRunPage listRuns(ActorContext actor, UUID workspaceId, UUID subscriptionId,
            Instant cursorCreatedAt, UUID cursorId, int pageSize) {
        requireDirectHuman(actor);
        requireManager(actor, workspaceId);
        workspaces.require(actor, workspaceId, "task:read");
        requireSubscription(actor.tenantId(), workspaceId, actor.actorId(), subscriptionId, false);
        if (pageSize < 1 || pageSize > 50 || (cursorCreatedAt == null) != (cursorId == null))
            throw EafException.invalid("自动化运行分页参数无效。");
        var selected = jdbc.query("select id, created_at from workflow.automation_run where tenant_id = ? and workspace_id = ? "
                        + "and owner_id = ? and subscription_id = ? and (?::timestamptz is null or (created_at, id) < (?, ?)) "
                        + "order by created_at desc, id desc limit ?",
                (rs, row) -> new Object[]{rs.getObject("id", UUID.class), instant(rs, "created_at")}, actor.tenantId(),
                workspaceId, actor.actorId(), subscriptionId, cursorCreatedAt == null ? null : Timestamp.from(cursorCreatedAt),
                cursorCreatedAt == null ? Timestamp.from(Instant.EPOCH) : Timestamp.from(cursorCreatedAt),
                cursorId == null ? new UUID(0, 0) : cursorId, pageSize + 1);
        var more = selected.size() > pageSize;
        var page = more ? selected.subList(0, pageSize) : selected;
        var items = page.stream().map(row -> visibleRun(actor, workspaceId, subscriptionId, (UUID) row[0])).toList();
        var last = more ? page.get(page.size() - 1) : null;
        return new AutomationRunPage(items, last == null ? null : (Instant) last[1], last == null ? null : (UUID) last[0]);
    }

    @Override
    @Transactional(readOnly = true)
    public AutomationRun getRun(ActorContext actor, UUID workspaceId, UUID subscriptionId, UUID runId) {
        requireDirectHuman(actor);
        requireManager(actor, workspaceId);
        workspaces.require(actor, workspaceId, "task:read");
        requireSubscription(actor.tenantId(), workspaceId, actor.actorId(), subscriptionId, false);
        return visibleRun(actor, workspaceId, subscriptionId, runId);
    }

    private AutomationRun visibleRun(ActorContext actor, UUID workspaceId, UUID subscriptionId, UUID runId) {
        var row = readRun(actor.tenantId(), workspaceId, actor.actorId(), subscriptionId, runId, false);
        if (row == null) throw EafException.notFound();
        UUID visibleTaskId = row.taskId();
        String taskStatus = row.taskStatus();
        if (visibleTaskId != null) {
            try { taskStatus = tasks.get(actor, workspaceId, visibleTaskId).status().name(); }
            catch (EafException denied) { visibleTaskId = null; }
        }
        return new AutomationRun(row.id(), row.subscriptionId(), row.triggerKind(), row.triggerKey(), row.status(),
                row.reason(), row.plannedAt(), row.occurredAt(), row.sourceRowVersion(), visibleTaskId, taskStatus,
                row.attempts(), row.nextAttemptAt(), row.admittedAt(), row.finishedAt(), row.createdAt());
    }

    @Override
    public boolean dispatchOne() {
        if (reconcileOne()) return true;
        var attemptedRun = new java.util.concurrent.atomic.AtomicReference<UUID>();
        try {
            var admitted = transactions.execute(status -> admitOneReady(attemptedRun));
            if (Boolean.TRUE.equals(admitted)) return true;
        } catch (EafException failure) {
            var runId = attemptedRun.get();
            if (runId != null) recordAdmissionFailure(runId, failure);
            return runId != null;
        }
        return Boolean.TRUE.equals(transactions.execute(status -> prepareOneWeeklySlot()));
    }

    @Override
    public boolean consumeOneEvent() {
        return Boolean.TRUE.equals(transactions.execute(status -> processOneEvent()));
    }

    private boolean admitOneReady(java.util.concurrent.atomic.AtomicReference<UUID> attemptedRun) {
        var runId = jdbc.query("select r.id from workflow.automation_run r join workflow.automation_subscription s on s.id = r.subscription_id "
                        + "where r.status = 'READY' and (r.next_attempt_at is null or r.next_attempt_at <= ?) "
                        + "order by coalesce(r.next_attempt_at, r.created_at), r.created_at, r.id "
                        + "for update of s skip locked limit 1",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, Timestamp.from(Instant.now(clock)));
        if (runId == null) return false;
        attemptedRun.set(runId);
        var row = readRunById(runId, true);
        if (row == null || !"READY".equals(row.status())) return true;
        var sub = requireSubscription(row.tenantId(), row.workspaceId(), row.ownerId(), row.subscriptionId(), true);
        var now = Instant.now(clock);
        if (sub.activeRunId() != null && !sub.activeRunId().equals(runId)) {
            closeRun(row.id(), "SKIPPED", "SKIPPED_BUSY", now);
            return true;
        }
        if (!"ACTIVE".equals(sub.status()) || row.authorizationEpoch() != sub.authorizationEpoch()) {
            closeRun(row.id(), "SKIPPED", "SUBSCRIPTION_STOPPED", now);
            clearActiveRun(sub.id(), row.id(), now);
            return true;
        }
        if (!sub.expiresAt().isAfter(now)) {
            expireSubscription(sub, now);
            closeRun(row.id(), "SKIPPED", "EXPIRED", now);
            return true;
        }
        if (sub.admittedRuns() >= sub.maxRuns()) {
            exhaustSubscription(sub, now);
            closeRun(row.id(), "SKIPPED", "EXHAUSTED", now);
            return true;
        }
        if (row.plannedAt().plus(GRACE).isBefore(now)) {
            closeRun(row.id(), "SKIPPED", "MISSED", now);
            clearActiveRun(sub.id(), row.id(), now);
            return true;
        }
        var actor = currentActor(sub);
        requireCurrentActions(actor, sub.workspaceId());
        var fixed = fixedAssets(actor, sub.workspaceId());
        var capture = capture(actor, sub, row, now);
        var taskInput = taskInput(capture);
        var inputHash = Hashing.sha256(taskInput);
        jdbc.update("update workflow.automation_run set input_snapshot = ?::jsonb, input_hash = ?, updated_at = ? where id = ? and status = 'READY'",
                taskInput, inputHash, Timestamp.from(now), row.id());
        var assetBinding = new TaskAssetBinding(sub.capabilityId(), sub.capabilityVersion(), sub.capabilityHash(),
                sub.skillId(), sub.skillVersion(), sub.skillHash());
        if (!fixed.capability().contentHash().equals(assetBinding.capabilityHash())
                || !fixed.capability().skillContentHash().equals(assetBinding.skillHash()))
            throw EafException.conflict("AUTOMATION_ASSET_CHANGED", "自动化固定 Capability/Skill 版本已变化。");
        var task = tasks.createAutomationReadTask(new CreateAutomationReadTaskCommand(actor, sub.workspaceId(), row.id(),
                sub.id(), sub.authorizationEpoch(), AGENT_ID, VERSION, taskInput, assetBinding, inputHash, sub.profileHash(),
                "p30:" + row.id()));
        var newCount = sub.admittedRuns() + 1;
        jdbc.update("update workflow.automation_run set task_id = ?, task_status = ?, status = 'ADMITTED', reason = null, "
                        + "attempts = attempts + 1, next_attempt_at = null, admitted_at = ?, updated_at = ? where id = ? and status = 'READY'",
                task.id(), task.status().name(), Timestamp.from(now), Timestamp.from(now), row.id());
        jdbc.update("update workflow.automation_subscription set admitted_runs = ?, status = ?, reason = null, "
                        + "active_run_id = ?, row_version = row_version + 1, updated_at = ? where id = ? and row_version = ?",
                newCount, newCount >= sub.maxRuns() ? "EXHAUSTED" : "ACTIVE", row.id(), Timestamp.from(now), sub.id(), sub.rowVersion());
        return true;
    }

    private boolean prepareOneWeeklySlot() {
        var now = Instant.now(clock);
        var subId = jdbc.query("select id from workflow.automation_subscription where status = 'ACTIVE' "
                        + "and trigger_kind = 'WEEKLY' and next_fire_at <= ? order by next_fire_at, id for update skip locked limit 1",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, Timestamp.from(now));
        if (subId == null) return false;
        var sub = readSubscriptionById(subId, true);
        if (sub == null || !"ACTIVE".equals(sub.status())) return true;
        if (!sub.expiresAt().isAfter(now)) {
            expireSubscription(sub, now);
            return true;
        }
        if (sub.admittedRuns() >= sub.maxRuns()) {
            exhaustSubscription(sub, now);
            return true;
        }
        var day = DayOfWeek.valueOf(sub.dayOfWeek());
        var time = LocalTime.parse(sub.localTime());
        var zone = ZoneId.of(sub.timeZone());
        var latest = latestSlotAtOrBefore(now, day, time, zone);
        if (latest == null || latest.instant().isBefore(sub.nextFireAt())) return false;
        var triggerKey = weeklyTriggerKey(latest, time, zone);
        if (latest.instant().plus(GRACE).isBefore(now)) {
            insertSkippedRun(sub, triggerKey, latest.instant(), "MISSED", now);
            moveToNextWeek(sub, now, day, time, zone);
            return true;
        }
        if (sub.activeRunId() != null) {
            insertSkippedRun(sub, triggerKey, latest.instant(), "SKIPPED_BUSY", now);
            moveToNextWeek(sub, now, day, time, zone);
            return true;
        }
        var runId = UUID.randomUUID();
        jdbc.update("insert into workflow.automation_run(id, tenant_id, workspace_id, owner_id, subscription_id, authorization_epoch, "
                        + "trigger_kind, trigger_key, planned_at, status) values (?, ?, ?, ?, ?, ?, 'WEEKLY', ?, ?, 'READY') "
                        + "on conflict (subscription_id, trigger_key) do nothing",
                runId, sub.tenantId(), sub.workspaceId(), sub.ownerId(), sub.id(), sub.authorizationEpoch(), triggerKey,
                Timestamp.from(latest.instant()));
        var actualRun = jdbc.query("select id from workflow.automation_run where subscription_id = ? and trigger_key = ?",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, sub.id(), triggerKey);
        if (actualRun != null) {
            jdbc.update("update workflow.automation_subscription set active_run_id = ?, next_fire_at = ?, row_version = row_version + 1, updated_at = ? where id = ?",
                    actualRun, Timestamp.from(nextSlotAfter(now, day, time, zone).instant()), Timestamp.from(now), sub.id());
        }
        return true;
    }

    private boolean processOneEvent() {
        var event = jdbc.query("select id, tenant_id, workspace_id, source, event_id, event_type, work_item_id, "
                        + "source_row_version, status, old_assignee_id, new_assignee_id, occurred_at "
                        + "from workflow.p16_work_item_event where processed_at is null order by occurred_at, id "
                        + "for update skip locked limit 1",
                rs -> rs.next() ? new EventRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getString("source"), rs.getString("event_id"),
                        rs.getString("event_type"), rs.getObject("work_item_id", UUID.class), rs.getLong("source_row_version"),
                        rs.getString("status"), rs.getObject("old_assignee_id", UUID.class),
                        rs.getObject("new_assignee_id", UUID.class), instant(rs, "occurred_at")) : null);
        if (event == null) return false;
        var now = Instant.now(clock);
        var subs = jdbc.query("select id from workflow.automation_subscription where tenant_id = ? and workspace_id = ? "
                        + "and work_item_id = ? and trigger_kind = 'P16_WORK_ITEM_CHANGED' and status = 'ACTIVE' order by id",
                (rs, row) -> rs.getObject(1, UUID.class), event.tenantId(), event.workspaceId(), event.workItemId());
        var outcome = "NO_SUBSCRIPTION";
        if (!EVENT_SOURCE.equals(event.source()) || !EVENT_TYPE.equals(event.type())
                || event.occurredAt().isAfter(now.plusSeconds(60))) {
            outcome = "UNTRUSTED";
        } else if (event.occurredAt().plus(GRACE).isBefore(now)) {
            outcome = "EXPIRED";
            for (var id : subs) skipEventForSubscription(event, id, "MISSED", now);
        } else {
            for (var id : subs) {
                var sub = readSubscriptionById(id, true);
                if (sub == null || !"ACTIVE".equals(sub.status())) continue;
                outcome = "MATCHED";
                if (!event.occurredAt().isAfter(sub.epochStartedAt()) || !event.occurredAt().isAfter(sub.createdAt())) {
                    insertEventRun(sub, event, "SKIPPED", "SUBSCRIPTION_NOT_ACTIVE", now);
                    continue;
                }
                if (sub.lastObservedSourceRowVersion() != null
                        && event.sourceRowVersion() <= sub.lastObservedSourceRowVersion()) {
                    insertEventRun(sub, event, "SKIPPED", "STALE_EVENT", now);
                    continue;
                }
                if (!sub.expiresAt().isAfter(now)) {
                    expireSubscription(sub, now);
                    insertEventRun(sub, event, "SKIPPED", "EXPIRED", now);
                    continue;
                }
                if (sub.admittedRuns() >= sub.maxRuns()) {
                    exhaustSubscription(sub, now);
                    insertEventRun(sub, event, "SKIPPED", "EXHAUSTED", now);
                    continue;
                }
                var actor = currentActor(sub);
                try {
                    requireCurrentActions(actor, sub.workspaceId());
                    var current = workflows.readP16DigestSource(actor, sub.workspaceId(), sub.workItemId());
                    if (!actor.actorId().equals(current.assigneeId()))
                        throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "P16 工作项已改派。");
                    var observedVersion = Math.max(event.sourceRowVersion(), current.rowVersion());
                    if (sub.activeRunId() != null) {
                        insertEventRun(sub, event, "SKIPPED", "SKIPPED_BUSY", now);
                        updateObservedVersion(sub, observedVersion, now);
                        continue;
                    }
                    var runId = insertEventRun(sub, event, "READY", null, now);
                    if (runId != null) {
                        jdbc.update("update workflow.automation_subscription set active_run_id = ?, last_observed_source_row_version = ?, "
                                        + "row_version = row_version + 1, updated_at = ? where id = ?",
                                runId, observedVersion, Timestamp.from(now), sub.id());
                    } else updateObservedVersion(sub, observedVersion, now);
                } catch (EafException denied) {
                    var reason = safeBlockReason(denied);
                    blockSubscription(sub, reason, now);
                    insertEventRun(sub, event, "SKIPPED", reason, now);
                }
            }
        }
        jdbc.update("update workflow.p16_work_item_event set processed_at = ?, outcome = ? where id = ? and processed_at is null",
                Timestamp.from(now), outcome, event.id());
        return true;
    }

    private void skipEventForSubscription(EventRow event, UUID subscriptionId, String reason, Instant now) {
        var sub = readSubscriptionById(subscriptionId, true);
        if (sub != null) insertEventRun(sub, event, "SKIPPED", reason, now);
    }

    private UUID insertEventRun(SubscriptionRow sub, EventRow event, String status, String reason, Instant now) {
        var key = "event:" + event.source() + ":" + event.eventId();
        var id = UUID.randomUUID();
        jdbc.update("insert into workflow.automation_run(id, tenant_id, workspace_id, owner_id, subscription_id, authorization_epoch, "
                        + "trigger_kind, trigger_key, planned_at, occurred_at, event_source, event_id, source_row_version, status, reason, finished_at) "
                        + "values (?, ?, ?, ?, ?, ?, 'P16_WORK_ITEM_CHANGED', ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        + "on conflict (subscription_id, trigger_key) do nothing",
                id, sub.tenantId(), sub.workspaceId(), sub.ownerId(), sub.id(), sub.authorizationEpoch(), key,
                Timestamp.from(event.occurredAt()), Timestamp.from(event.occurredAt()), event.source(), event.eventId(),
                event.sourceRowVersion(), status, reason, "READY".equals(status) ? null : Timestamp.from(now));
        return jdbc.query("select id from workflow.automation_run where subscription_id = ? and trigger_key = ?",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, sub.id(), key);
    }

    private void moveToNextWeek(SubscriptionRow sub, Instant now, DayOfWeek day, LocalTime time, ZoneId zone) {
        var next = nextSlotAfter(now, day, time, zone).instant();
        jdbc.update("update workflow.automation_subscription set next_fire_at = ?, row_version = row_version + 1, updated_at = ? where id = ?",
                Timestamp.from(next), Timestamp.from(now), sub.id());
    }

    private void insertSkippedRun(SubscriptionRow sub, String triggerKey, Instant plannedAt, String reason, Instant now) {
        jdbc.update("insert into workflow.automation_run(id, tenant_id, workspace_id, owner_id, subscription_id, authorization_epoch, "
                        + "trigger_kind, trigger_key, planned_at, status, reason, finished_at) "
                        + "values (?, ?, ?, ?, ?, ?, 'WEEKLY', ?, ?, 'SKIPPED', ?, ?) on conflict (subscription_id, trigger_key) do nothing",
                UUID.randomUUID(), sub.tenantId(), sub.workspaceId(), sub.ownerId(), sub.id(), sub.authorizationEpoch(),
                triggerKey, Timestamp.from(plannedAt), reason, Timestamp.from(now));
    }

    @Override
    @Transactional(readOnly = true)
    public void requireTaskCreation(CreateAutomationReadTaskCommand command) {
        if (command == null || command.actor() == null || command.runId() == null || command.subscriptionId() == null)
            throw EafException.forbidden("P30 Task 必须绑定已登记的订阅运行。");
        requireDirectHuman(command.actor());
        var sub = requireSubscription(command.actor().tenantId(), command.workspaceId(), command.actor().actorId(),
                command.subscriptionId(), false);
        var run = readRunById(command.runId(), false);
        if (run == null || run.inputHash() == null || command.assetBinding() == null
                || !run.subscriptionId().equals(sub.id()) || !"READY".equals(run.status())
                || !"ACTIVE".equals(sub.status()) || run.authorizationEpoch() != sub.authorizationEpoch()
                || command.authorizationEpoch() != sub.authorizationEpoch() || !sub.expiresAt().isAfter(Instant.now(clock))
                || sub.admittedRuns() >= sub.maxRuns() || !run.inputHash().equals(command.inputHash())
                || !Hashing.sha256(command.input()).equals(command.inputHash())
                || !sub.profileHash().equals(command.profileHash()) || !sub.agentId().equals(command.agentId())
                || !sub.agentVersion().equals(command.agentVersion())
                || !CAPABILITY_ID.equals(command.assetBinding().capabilityId())
                || !sub.capabilityVersion().equals(command.assetBinding().capabilityVersion())
                || !sub.capabilityHash().equals(command.assetBinding().capabilityHash())
                || !sub.skillVersion().equals(command.assetBinding().skillVersion())
                || !sub.skillHash().equals(command.assetBinding().skillHash()))
            throw EafException.forbidden("P30 Task 输入、固定资产或运行授权绑定不匹配。");
        requireCurrentActions(command.actor(), command.workspaceId());
        fixedAssets(command.actor(), command.workspaceId(), sub);
        verifySnapshotCurrent(command.actor(), sub, run);
    }

    @Override
    @Transactional(readOnly = true)
    public AutomationTaskSource requireTaskSource(ActorContext actor, UUID workspaceId, UUID taskId, int attempt) {
        requireDirectHuman(actor);
        var row = jdbc.query("select " + RUN_COLUMNS + " from workflow.automation_run r "
                        + "where r.task_id = ? and r.tenant_id = ? and r.workspace_id = ? and r.owner_id = ?",
                (rs, n) -> mapRun(rs), taskId, actor.tenantId(), workspaceId, actor.actorId()).stream().findFirst().orElse(null);
        if (row == null || row.taskId() == null) throw EafException.notFound();
        var sub = requireSubscription(actor.tenantId(), workspaceId, actor.actorId(), row.subscriptionId(), false);
        workspaces.require(actor, workspaceId, "task:read");
        workspaces.require(actor, workspaceId, "work-item:read");
        requireActiveOwner(actor);
        fixedAssets(actor, workspaceId, sub);
        verifySnapshotCurrent(actor, sub, row);
        var items = readDigestItems(row.inputSnapshot());
        return new AutomationTaskSource(row.id(), sub.id(), row.authorizationEpoch(), row.inputSnapshot(), row.inputHash(),
                sub.profileHash(), row.generationAttempted(), row.resultJson(), items);
    }

    @Override
    public void requireTaskResultCurrent(ActorContext actor, UUID workspaceId, UUID taskId, int attempt) {
        requireTaskSource(actor, workspaceId, taskId, attempt);
    }

    @Override
    public void requireTaskExecutionCurrent(ActorContext actor, UUID workspaceId, UUID taskId, int attempt) {
        if (!deterministicMode)
            throw EafException.conflict("AUTOMATION_MODE_UNSUPPORTED", "自动摘要仅允许 deterministic 模式运行。");
        var source = requireTaskSource(actor, workspaceId, taskId, attempt);
        var sub = requireSubscription(actor.tenantId(), workspaceId, actor.actorId(), source.subscriptionId(), false);
        var run = readRunById(source.runId(), false);
        if (attempt != 1 || run == null || !"ADMITTED".equals(run.status())
                || !Set.of("ACTIVE", "EXHAUSTED").contains(sub.status())
                || !sub.expiresAt().isAfter(Instant.now(clock))
                || source.authorizationEpoch() != sub.authorizationEpoch())
            throw EafException.conflict("AUTOMATION_SUBSCRIPTION_STOPPED", "自动化订阅已暂停、撤回、到期或超出授权 epoch。");
        requireCurrentActions(actor, workspaceId);
    }

    @Override
    @Transactional
    public boolean beginGeneration(ActorContext actor, UUID workspaceId, UUID taskId, int attempt) {
        requireTaskExecutionCurrent(actor, workspaceId, taskId, attempt);
        if (attempt != 1) return false;
        return jdbc.update("update workflow.automation_run set generation_attempted = true, generation_task_id = ?, "
                        + "generation_attempt = ?, updated_at = ? where task_id = ? and status = 'ADMITTED' "
                        + "and generation_attempted = false and result_json is null",
                taskId, attempt, Timestamp.from(Instant.now(clock)), taskId) == 1;
    }

    @Override
    @Transactional
    public String recordGenerationResult(ActorContext actor, UUID workspaceId, UUID taskId, int attempt, String resultJson) {
        requireTaskExecutionCurrent(actor, workspaceId, taskId, attempt);
        var source = requireTaskSource(actor, workspaceId, taskId, attempt);
        var modelResult = parseJson(resultJson, "automation.result");
        var overview = modelResult.path("overview").asText(null);
        var attention = modelResult.path("attentionItems");
        if (overview == null || overview.length() > 1_000 || !attention.isArray() || attention.size() > 5)
            throw EafException.invalid("固定待办摘要响应格式无效。");
        var evidence = new LinkedHashSet<String>();
        source.items().forEach(item -> evidence.add(item.evidenceId()));
        if (source.items().isEmpty() && ("当前无匹配待办".equals(overview) == false || attention.size() != 0))
            throw EafException.invalid("空待办摘要只能返回固定的无匹配结果。");
        var seen = new LinkedHashSet<String>();
        for (var item : attention) {
            var text = item.path("text").asText(null);
            var refs = item.path("evidenceIds");
            if (text == null || text.isBlank() || text.length() > 300 || !refs.isArray() || refs.isEmpty() || refs.size() > 20)
                throw EafException.invalid("待办关注项必须包含有界文字和证据引用。");
            for (var ref : refs) {
                var id = ref.asText();
                if (!evidence.contains(id)) throw EafException.invalid("待办关注项引用了当前证据包以外的来源。");
            }
            seen.add(text);
        }
        var row = readRunById(source.runId(), true);
        if (row == null || !taskId.equals(row.taskId()) || !"ADMITTED".equals(row.status()))
            throw EafException.conflict("AUTOMATION_RUN_BINDING_INVALID", "摘要结果与当前原 Task 不匹配。");
        if (row.modelResultJson() != null) {
            if (!canonical(parseJson(row.modelResultJson(), "automation.savedModelResult")).equals(canonical(modelResult)))
                throw EafException.conflict("AUTOMATION_RESULT_CONFLICT", "同一运行的结构化摘要结果不可替换。");
            return row.resultJson();
        }
        if (!source.items().isEmpty() && (!row.generationAttempted() || !taskId.equals(row.generationTaskId())
                || !Integer.valueOf(attempt).equals(row.generationAttempt())))
            throw EafException.conflict("AUTOMATION_GENERATION_BINDING_INVALID", "摘要结果没有唯一生成调用绑定。");
        if (source.items().isEmpty() && row.generationAttempted())
            throw EafException.conflict("AUTOMATION_GENERATION_BINDING_INVALID", "空待办结果不应发起模型生成。");
        var markdown = renderDigest(source, overview, attention);
        if (markdown.getBytes(StandardCharsets.UTF_8).length > 65_536)
            throw EafException.invalid("MY_P16_WORK_DIGEST_MARKDOWN_V1 超过 64 KiB。");
        var projection = json.createObjectNode().put("format", "MY_P16_WORK_DIGEST_MARKDOWN_V1")
                .put("markdown", markdown).put("sourceHash", snapshotHash(row.inputSnapshot()))
                .put("sourceCount", source.items().size());
        var normalizedModel = canonical(modelResult);
        var finalJson = canonical(projection);
        jdbc.update("update workflow.automation_run set model_result_json = ?::jsonb, result_json = ?::jsonb, "
                        + "result_markdown = ?, updated_at = ? where id = ? and status = 'ADMITTED' and model_result_json is null",
                normalizedModel, finalJson, markdown, Timestamp.from(Instant.now(clock)), row.id());
        return finalJson;
    }

    private Capture capture(ActorContext actor, SubscriptionRow sub, RunRow run, Instant capturedAt) {
        List<P16DigestSource> sources;
        boolean hasMore;
        if ("WEEKLY".equals(sub.triggerKind())) {
            var page = workflows.readMyP16DigestSources(actor, sub.workspaceId(), sub.maxItems());
            sources = page.items();
            hasMore = page.hasMore();
        } else {
            var source = workflows.readP16DigestSource(actor, sub.workspaceId(), sub.workItemId());
            if (!actor.actorId().equals(source.assigneeId())) throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "原工作项已改派。");
            if (run.sourceRowVersion() != null && source.rowVersion() < run.sourceRowVersion())
                throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "事件所指版本尚未成为当前 P16 状态。");
            sources = List.of(source);
            hasMore = false;
        }
        var items = new ArrayList<AutomationDigestItem>(sources.size());
        for (var index = 0; index < sources.size(); index++) items.add(digestItem(sources.get(index), index + 1));
        var snapshot = json.createObjectNode().put("capturedAt", capturedAt.toString()).put("triggerKind", sub.triggerKind())
                .put("maxItems", sub.maxItems()).put("hasMore", hasMore);
        var array = snapshot.putArray("items");
        items.forEach(item -> array.add(json.valueToTree(item)));
        var snapshotJson = canonical(snapshot);
        if (snapshotJson.codePointCount(0, snapshotJson.length()) / 4 > 2_000)
            throw EafException.invalid("待办证据超过 2,000 个估算输入 token 上限。");
        return new Capture(snapshotJson, Hashing.sha256(snapshotJson), hasMore, items, capturedAt);
    }

    private String taskInput(Capture capture) {
        var input = json.createObjectNode().put("snapshotJson", capture.snapshotJson()).put("snapshotHash", capture.snapshotHash());
        var result = canonical(input);
        if (result.length() > 8_000) throw EafException.invalid("自动化摘要 Task 输入超过 8,000 字符限制。");
        return result;
    }

    private AutomationDigestItem digestItem(P16DigestSource source, int ordinal) {
        var sourceHash = Hashing.sha256(String.join("\u001f", source.workItemId().toString(), source.instanceId().toString(),
                String.valueOf(source.requestId()), source.status(), Long.toString(source.rowVersion()), source.assigneeId().toString(),
                String.valueOf(source.deadlineAt()), source.createdAt().toString(), String.valueOf(source.sharedBrief()),
                String.valueOf(source.handlingAdvice()), String.valueOf(source.cautions())));
        return new AutomationDigestItem("W" + ordinal, source.workItemId(), source.instanceId(), source.requestId(),
                source.status(), source.rowVersion(), source.assigneeId(), source.deadlineAt(), source.createdAt(),
                sourceHash, firstCodePoints(source.sharedBrief(), 200));
    }

    private void verifySnapshotCurrent(ActorContext actor, SubscriptionRow sub, RunRow run) {
        if (run.inputSnapshot() == null || run.inputHash() == null || !run.inputHash().matches("[0-9a-f]{64}"))
            throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "运行没有有效的证据快照绑定。");
        var taskInput = parseJson(run.inputSnapshot(), "automation.taskInput");
        var snapshotJson = taskInput.path("snapshotJson").asText(null);
        var expectedSnapshotHash = taskInput.path("snapshotHash").asText(null);
        if (snapshotJson == null || expectedSnapshotHash == null || !expectedSnapshotHash.equals(Hashing.sha256(snapshotJson)))
            throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "待办证据包摘要校验失败。");
        var snapshot = parseJson(snapshotJson, "automation.snapshot");
        var refs = readDigestItems(run.inputSnapshot());
        List<P16DigestSource> current;
        boolean hasMore;
        if ("WEEKLY".equals(sub.triggerKind())) {
            var page = workflows.readMyP16DigestSources(actor, sub.workspaceId(), sub.maxItems());
            current = page.items();
            hasMore = page.hasMore();
        } else {
            current = List.of(workflows.readP16DigestSource(actor, sub.workspaceId(), sub.workItemId()));
            hasMore = false;
        }
        var nowRefs = new ArrayList<AutomationDigestItem>();
        for (int i = 0; i < current.size(); i++) nowRefs.add(digestItem(current.get(i), i + 1));
        if (nowRefs.size() != refs.size() || snapshot.path("hasMore").asBoolean() != hasMore)
            throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "本人待办范围或数量已变化。");
        for (int i = 0; i < refs.size(); i++) {
            var old = refs.get(i);
            var fresh = nowRefs.get(i);
            if (!old.workItemId().equals(fresh.workItemId()) || old.rowVersion() != fresh.rowVersion()
                    || !old.sourceHash().equals(fresh.sourceHash()) || !old.status().equals(fresh.status())
                    || !old.assigneeId().equals(actor.actorId()))
                throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "待办来源版本或当前授权已变化。");
        }
        if ("P16_WORK_ITEM_CHANGED".equals(sub.triggerKind()) && (refs.size() != 1
                || !sub.workItemId().equals(refs.getFirst().workItemId())))
            throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "事件摘要来源与订阅绑定不匹配。");
    }

    private List<AutomationDigestItem> readDigestItems(String taskInputJson) {
        if (taskInputJson == null) return List.of();
        var taskInput = parseJson(taskInputJson, "automation.taskInput");
        var snapshot = parseJson(taskInput.path("snapshotJson").asText("{}"), "automation.snapshot");
        var result = new ArrayList<AutomationDigestItem>();
        for (var item : snapshot.path("items")) {
            result.add(new AutomationDigestItem(item.path("evidenceId").asText(),
                    uuid(item.path("workItemId").asText()), uuid(item.path("instanceId").asText()),
                    item.path("requestId").asText(null), item.path("status").asText(), item.path("rowVersion").asLong(),
                    uuid(item.path("assigneeId").asText()), instant(item.path("deadlineAt").asText(null)),
                    instant(item.path("createdAt").asText(null)), item.path("sourceHash").asText(),
                    item.path("sharedBriefPreview").asText()));
        }
        return List.copyOf(result);
    }

    private String renderDigest(AutomationTaskSource source, String overview, JsonNode attention) {
        var input = parseJson(source.inputSnapshotJson(), "automation.taskInput");
        var snapshot = parseJson(input.path("snapshotJson").asText("{}"), "automation.snapshot");
        var out = new StringBuilder("# 本人 P16 待办摘要\n\n");
        out.append("读取时间：").append(markdown(snapshot.path("capturedAt").asText("未知"))).append("\n");
        out.append("范围：本人受派 P16 工作项；来源版本与摘要引用见下。\n\n## 当前待办事实\n\n");
        if (source.items().isEmpty()) out.append("当前无匹配待办。\n");
        for (var item : source.items()) {
            out.append("- [").append(item.evidenceId()).append("] 工作项 ").append(item.workItemId())
                    .append("；状态 ").append(markdown(item.status())).append("；版本 ").append(item.rowVersion());
            if (item.deadlineAt() != null) out.append("；截止 ").append(item.deadlineAt());
            out.append("\n  摘要预览：").append(markdown(item.sharedBriefPreview())).append("\n");
        }
        out.append("\n## 整理意见\n\n").append(markdown(overview)).append("\n");
        if (!attention.isEmpty()) {
            out.append("\n## 关注事项\n\n");
            for (var item : attention) {
                out.append("- ").append(markdown(item.path("text").asText()));
                var refs = new ArrayList<String>();
                item.path("evidenceIds").forEach(ref -> refs.add(ref.asText()));
                out.append("（依据：").append(String.join(", ", refs)).append("）\n");
            }
        }
        if (snapshot.path("hasMore").asBoolean()) out.append("\n本次只展示订阅上限内的来源，仍有其他本人待办未纳入。\n");
        out.append("\n本摘要只读，不提交人工结果或改变工作项状态。\n");
        return out.toString();
    }

    private String snapshotHash(String taskInputJson) {
        var input = parseJson(taskInputJson, "automation.taskInput");
        return input.path("snapshotHash").asText();
    }

    static String markdown(String text) {
        if (text == null) return "";
        return text.replace("\\", "\\\\").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\r", " ").replace("\n", " ").replace("`", "\\`")
                .replace("[", "\\[").replace("]", "\\]").replace("(", "\\(").replace(")", "\\)");
    }

    private String firstCodePoints(String text, int count) {
        if (text == null) return "";
        var codePoints = text.codePointCount(0, text.length());
        return codePoints <= count ? text : text.substring(0, text.offsetByCodePoints(0, count));
    }

    private boolean reconcileOne() {
        var candidate = transactions.execute(status -> jdbc.query("select " + RUN_COLUMNS + " from workflow.automation_run r "
                        + "where r.status = 'ADMITTED' and r.task_id is not null order by r.admitted_at, r.id limit 1",
                (rs, row) -> mapRun(rs)).stream().findFirst().orElse(null));
        if (candidate == null) return false;
        var actor = identities.isActiveHuman(candidate.tenantId(), candidate.ownerId())
                ? new ActorContext(candidate.ownerId(), candidate.tenantId(), ActorType.HUMAN,
                        workspaces.actions(candidate.tenantId(), candidate.ownerId(), candidate.workspaceId())) : null;
        TaskSnapshot task = null;
        EafException failure = null;
        if (actor != null) {
            try { task = tasks.get(actor, candidate.workspaceId(), candidate.taskId()); }
            catch (EafException denied) { failure = denied; }
        } else failure = EafException.forbidden("自动化 Owner 当前不可用。");
        var taskSnapshot = task;
        var taskFailure = failure;
        transactions.executeWithoutResult(status -> {
            var run = readRunById(candidate.id(), true);
            if (run == null || !"ADMITTED".equals(run.status()) || !candidate.taskId().equals(run.taskId())) return;
            var sub = requireSubscription(run.tenantId(), run.workspaceId(), run.ownerId(), run.subscriptionId(), true);
            var now = Instant.now(clock);
            if (taskFailure != null) {
                blockSubscription(sub, "AUTOMATION_SOURCE_UNAVAILABLE", now);
                closeRun(run.id(), "FAILED", "SOURCE_UNAVAILABLE", now);
                clearActiveRun(sub.id(), run.id(), now);
                if (actor != null) cancelAfterCommit(actor, run.workspaceId(), run.taskId());
                return;
            }
            var statusName = taskSnapshot.status().name();
            if (TERMINAL_TASKS.contains(statusName)) {
                var runStatus = "SUCCEEDED".equals(statusName) ? "SUCCEEDED"
                        : "CANCELLED".equals(statusName) ? "CANCELLED" : "FAILED";
                jdbc.update("update workflow.automation_run set task_status = ?, status = ?, finished_at = ?, updated_at = ? "
                                + "where id = ? and status = 'ADMITTED'", statusName, runStatus, Timestamp.from(now),
                        Timestamp.from(now), run.id());
                clearActiveRun(sub.id(), run.id(), now);
            } else {
                jdbc.update("update workflow.automation_run set task_status = ?, updated_at = ? where id = ? and status = 'ADMITTED'",
                        statusName, Timestamp.from(now), run.id());
            }
        });
        return true;
    }

    private void recordAdmissionFailure(UUID runId, EafException failure) {
        transactions.executeWithoutResult(status -> {
            var unbound = readRunById(runId, false);
            if (unbound == null || !"READY".equals(unbound.status())) return;
            var sub = requireSubscription(unbound.tenantId(), unbound.workspaceId(), unbound.ownerId(), unbound.subscriptionId(), true);
            var run = readRunById(runId, true);
            if (run == null || !"READY".equals(run.status())) return;
            var now = Instant.now(clock);
            if (failure.status() == 429 || failure.code().contains("CAPACITY")) {
                var attempts = run.attempts() + 1;
                if (attempts >= 3 || !run.plannedAt().plus(GRACE).isAfter(now)) {
                    closeRun(run.id(), "SKIPPED", "CAPACITY", now);
                    clearActiveRun(sub.id(), run.id(), now);
                } else {
                    jdbc.update("update workflow.automation_run set attempts = ?, reason = 'CAPACITY', next_attempt_at = ?, updated_at = ? "
                                    + "where id = ? and status = 'READY'",
                            attempts, Timestamp.from(now.plusSeconds(30)), Timestamp.from(now), run.id());
                }
                return;
            }
            if (failure.status() == 403 || Set.of("AUTOMATION_SOURCE_CHANGED", "AUTOMATION_ASSET_CHANGED",
                    "AUTOMATION_MODE_UNSUPPORTED", "RESOURCE_NOT_FOUND").contains(failure.code())) {
                blockSubscription(sub, safeBlockReason(failure), now);
                closeRun(run.id(), "SKIPPED", safeBlockReason(failure), now);
                clearActiveRun(sub.id(), run.id(), now);
                return;
            }
            closeRun(run.id(), "FAILED", "AUTOMATION_RUN_FAILED", now);
            clearActiveRun(sub.id(), run.id(), now);
            LOG.log(Level.WARNING, "P30 run {0} failed before Task admission: {1}", new Object[]{run.id(), failure.code()});
        });
    }

    private void skipReadyRuns(UUID subscriptionId, String reason) {
        var now = Timestamp.from(Instant.now(clock));
        jdbc.update("update workflow.automation_run set status = 'SKIPPED', reason = ?, finished_at = ?, updated_at = ? "
                        + "where subscription_id = ? and status = 'READY'",
                reason, now, now, subscriptionId);
        jdbc.update("update workflow.automation_subscription set active_run_id = null where id = ? and active_run_id in "
                        + "(select id from workflow.automation_run where subscription_id = ? and status = 'SKIPPED')",
                subscriptionId, subscriptionId);
    }

    private void closeRun(UUID runId, String status, String reason, Instant now) {
        jdbc.update("update workflow.automation_run set status = ?, reason = ?, finished_at = ?, next_attempt_at = null, updated_at = ? "
                        + "where id = ? and status in ('READY','ADMITTED')",
                status, reason, Timestamp.from(now), Timestamp.from(now), runId);
    }

    private void clearActiveRun(UUID subscriptionId, UUID runId, Instant now) {
        jdbc.update("update workflow.automation_subscription set active_run_id = null, row_version = row_version + 1, updated_at = ? "
                        + "where id = ? and active_run_id = ?", Timestamp.from(now), subscriptionId, runId);
    }

    private void updateObservedVersion(SubscriptionRow sub, long version, Instant now) {
        jdbc.update("update workflow.automation_subscription set last_observed_source_row_version = greatest(coalesce(last_observed_source_row_version, 0), ?), "
                        + "row_version = row_version + 1, updated_at = ? where id = ?",
                version, Timestamp.from(now), sub.id());
    }

    private void expireSubscription(SubscriptionRow sub, Instant now) {
        jdbc.update("update workflow.automation_subscription set status = 'EXPIRED', reason = 'EXPIRED', next_fire_at = null, "
                        + "authorization_epoch = authorization_epoch + 1, row_version = row_version + 1, updated_at = ? "
                        + "where id = ? and status in ('ACTIVE','PAUSED','BLOCKED')", Timestamp.from(now), sub.id());
        var taskId = taskId(sub.activeRunId());
        if (taskId != null && identities.isActiveHuman(sub.tenantId(), sub.ownerId())) {
            var actor = new ActorContext(sub.ownerId(), sub.tenantId(), ActorType.HUMAN,
                    workspaces.actions(sub.tenantId(), sub.ownerId(), sub.workspaceId()));
            cancelAfterCommit(actor, sub.workspaceId(), taskId);
        }
    }

    private void exhaustSubscription(SubscriptionRow sub, Instant now) {
        jdbc.update("update workflow.automation_subscription set status = 'EXHAUSTED', reason = 'RUN_LIMIT_REACHED', next_fire_at = null, "
                        + "row_version = row_version + 1, updated_at = ? where id = ? and admitted_runs >= max_runs "
                        + "and status = 'ACTIVE'", Timestamp.from(now), sub.id());
    }

    private void blockSubscription(SubscriptionRow sub, String reason, Instant now) {
        jdbc.update("update workflow.automation_subscription set status = 'BLOCKED', reason = ?, next_fire_at = null, "
                        + "authorization_epoch = authorization_epoch + 1, row_version = row_version + 1, updated_at = ? "
                        + "where id = ? and status in ('ACTIVE','PAUSED')", reason, Timestamp.from(now), sub.id());
        skipReadyRuns(sub.id(), reason);
        var taskId = taskId(sub.activeRunId());
        if (taskId != null && identities.isActiveHuman(sub.tenantId(), sub.ownerId())) {
            var actor = new ActorContext(sub.ownerId(), sub.tenantId(), ActorType.HUMAN,
                    workspaces.actions(sub.tenantId(), sub.ownerId(), sub.workspaceId()));
            cancelAfterCommit(actor, sub.workspaceId(), taskId);
        }
    }

    static String safeBlockReason(EafException failure) {
        return switch (failure.code()) {
            case "AUTOMATION_MODE_UNSUPPORTED" -> "AUTOMATION_MODE_UNSUPPORTED";
            case "AUTOMATION_SOURCE_CHANGED", "RESOURCE_NOT_FOUND" -> "SOURCE_UNAVAILABLE";
            case "AUTOMATION_ASSET_CHANGED" -> "ASSET_UNAVAILABLE";
            default -> "AUTHORIZATION_REVOKED";
        };
    }

    private UUID taskId(UUID runId) {
        if (runId == null) return null;
        return jdbc.query("select task_id from workflow.automation_run where id = ? and status = 'ADMITTED'",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, runId);
    }

    private void cancelAfterCommit(ActorContext actor, UUID workspaceId, UUID taskId) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { tasks.cancelAutomationTask(actor, workspaceId, taskId); }
                catch (RuntimeException failure) {
                    LOG.log(Level.WARNING, "P30 cancellation request did not complete for Task {0}; source checks still block new generation.", taskId);
                }
            }
        });
    }

    private void requireDirectHuman(ActorContext actor) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated()
                || !identities.isActiveHuman(actor.tenantId(), actor.actorId()))
            throw EafException.forbidden("P30 自动化只允许当前活跃的直接 HUMAN 身份。 ");
    }

    private void requireActiveOwner(ActorContext actor) {
        requireDirectHuman(actor);
    }

    private void requireManager(ActorContext actor, UUID workspaceId) {
        if (workspaceId == null) throw EafException.invalid("workspaceId 不能为空。");
        workspaces.require(actor, workspaceId, "workflow:automation:manage");
    }

    private void requireCurrentActions(ActorContext actor, UUID workspaceId) {
        requireDirectHuman(actor);
        for (var action : REQUIRED_ACTIONS) workspaces.require(actor, workspaceId, action);
    }

    private ActorContext currentActor(SubscriptionRow sub) {
        if (!identities.isActiveHuman(sub.tenantId(), sub.ownerId()))
            throw EafException.forbidden("自动化 Owner 当前不可用。");
        return new ActorContext(sub.ownerId(), sub.tenantId(), ActorType.HUMAN,
                workspaces.actions(sub.tenantId(), sub.ownerId(), sub.workspaceId()));
    }

    private FixedAssets fixedAssets(ActorContext actor, UUID workspaceId) {
        var agent = agents.requirePublished(actor.tenantId(), workspaceId, AGENT_ID, VERSION);
        var capability = capabilities.requirePublished(actor, workspaceId, CAPABILITY_ID, VERSION);
        if (!AGENT_ID.equals(agent.id()) || !VERSION.equals(agent.version()) || !PROFILE.equals(agent.responseProfile())
                || agent.ragEnabled() || !"NONE".equals(agent.evidencePolicy())
                || !CAPABILITY_ID.equals(capability.id()) || !VERSION.equals(capability.version())
                || !AGENT_ID.equals(capability.agentId()) || !VERSION.equals(capability.agentVersion())
                || !SKILL_ID.equals(capability.skillId()) || !VERSION.equals(capability.skillVersion())
                || !capability.toolDependencies().isEmpty()
                || !agents.tools(actor.tenantId(), workspaceId, AGENT_ID, VERSION).isEmpty())
            throw EafException.conflict("AUTOMATION_ASSET_CHANGED", "固定待办摘要 Agent/Capability/Skill 资产不可用或已变化。");
        return new FixedAssets(agent, capability);
    }

    private FixedAssets fixedAssets(ActorContext actor, UUID workspaceId, SubscriptionRow sub) {
        var fixed = fixedAssets(actor, workspaceId);
        if (!sub.agentId().equals(fixed.agent().id()) || !sub.agentVersion().equals(fixed.agent().version())
                || !sub.capabilityId().equals(fixed.capability().id())
                || !sub.capabilityVersion().equals(fixed.capability().version())
                || !sub.capabilityHash().equals(fixed.capability().contentHash())
                || !sub.skillId().equals(fixed.capability().skillId())
                || !sub.skillVersion().equals(fixed.capability().skillVersion())
                || !sub.skillHash().equals(fixed.capability().skillContentHash())
                || !sub.profileHash().equals(profileHash(fixed)))
            throw EafException.conflict("AUTOMATION_ASSET_CHANGED", "订阅绑定的固定摘要资产版本已变化。");
        return fixed;
    }

    private String profileHash(FixedAssets fixed) {
        return Hashing.sha256(String.join("\u001f", PROFILE, VERSION, fixed.agent().promptId().toString(),
                fixed.agent().promptVersion(), fixed.capability().contentHash(), fixed.capability().skillContentHash()));
    }

    private SubscriptionRow requireSubscription(UUID tenantId, UUID workspaceId, UUID ownerId, UUID id, boolean lock) {
        var sql = "select " + SUB_COLUMNS + " from workflow.automation_subscription where tenant_id = ? and workspace_id = ? "
                + "and owner_id = ? and id = ?" + (lock ? " for update" : "");
        var row = jdbc.query(sql, (rs, n) -> mapSubscription(rs), tenantId, workspaceId, ownerId, id)
                .stream().findFirst().orElse(null);
        if (row == null) throw EafException.notFound();
        return row;
    }

    private SubscriptionRow readSubscriptionById(UUID id, boolean lock) {
        if (id == null) return null;
        var sql = "select " + SUB_COLUMNS + " from workflow.automation_subscription where id = ?" + (lock ? " for update" : "");
        return jdbc.query(sql, (rs, n) -> mapSubscription(rs), id).stream().findFirst().orElse(null);
    }

    private RunRow readRun(UUID tenantId, UUID workspaceId, UUID ownerId, UUID subscriptionId, UUID runId, boolean lock) {
        var sql = "select " + RUN_COLUMNS + " from workflow.automation_run r where r.tenant_id = ? and r.workspace_id = ? "
                + "and r.owner_id = ? and r.subscription_id = ? and r.id = ?" + (lock ? " for update" : "");
        return jdbc.query(sql, (rs, n) -> mapRun(rs), tenantId, workspaceId, ownerId, subscriptionId, runId)
                .stream().findFirst().orElse(null);
    }

    private RunRow readRunById(UUID runId, boolean lock) {
        if (runId == null) return null;
        var sql = "select " + RUN_COLUMNS + " from workflow.automation_run r where r.id = ?" + (lock ? " for update" : "");
        return jdbc.query(sql, (rs, n) -> mapRun(rs), runId).stream().findFirst().orElse(null);
    }

    private SubscriptionRow mapSubscription(java.sql.ResultSet rs) throws java.sql.SQLException {
        var local = rs.getTime("local_time");
        return new SubscriptionRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("name"),
                rs.getString("trigger_kind"), rs.getString("day_of_week"), local == null ? null : local.toLocalTime().toString(),
                rs.getString("time_zone"), rs.getObject("work_item_id", UUID.class), rs.getInt("max_items"),
                instant(rs, "expires_at"), rs.getInt("max_runs"), rs.getInt("admitted_runs"), instant(rs, "next_fire_at"),
                rs.getString("status"), rs.getString("reason"), rs.getLong("authorization_epoch"), instant(rs, "epoch_started_at"),
                rs.getObject("active_run_id", UUID.class), nullableLong(rs, "last_observed_source_row_version"),
                rs.getString("request_hash"), rs.getObject("agent_id", UUID.class), rs.getString("agent_version"),
                rs.getObject("capability_id", UUID.class), rs.getString("capability_version"), rs.getString("capability_hash"),
                rs.getObject("skill_id", UUID.class), rs.getString("skill_version"), rs.getString("skill_hash"),
                rs.getString("profile_hash"), rs.getLong("row_version"), instant(rs, "created_at"), instant(rs, "updated_at"));
    }

    private RunRow mapRun(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new RunRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class),
                rs.getObject("subscription_id", UUID.class), rs.getLong("authorization_epoch"), rs.getString("trigger_kind"),
                rs.getString("trigger_key"), instant(rs, "planned_at"), instant(rs, "occurred_at"), rs.getString("event_source"),
                rs.getString("event_id"), nullableLong(rs, "source_row_version"), rs.getString("status"), rs.getString("reason"),
                rs.getString("input_snapshot"), rs.getString("input_hash"), rs.getObject("task_id", UUID.class),
                rs.getString("task_status"), rs.getInt("attempts"), instant(rs, "next_attempt_at"),
                rs.getBoolean("generation_attempted"), rs.getObject("generation_task_id", UUID.class),
                (Integer) rs.getObject("generation_attempt"), rs.getString("result_json"), rs.getString("model_result_json"),
                rs.getString("result_markdown"), instant(rs, "admitted_at"), instant(rs, "finished_at"), instant(rs, "created_at"));
    }

    private AutomationSubscription subscriptionSnapshot(SubscriptionRow sub) {
        var actions = new LinkedHashSet<String>();
        if (identities.isActiveHuman(sub.tenantId(), sub.ownerId())
                && workspaces.isAuthorized(sub.tenantId(), sub.ownerId(), sub.workspaceId(), "workflow:automation:manage")) {
            if ("ACTIVE".equals(sub.status())) actions.add("pause");
            if (Set.of("PAUSED", "BLOCKED").contains(sub.status())) actions.add("resume");
            if (!Set.of("DELETED", "EXPIRED", "EXHAUSTED").contains(sub.status())) actions.add("delete");
        }
        return new AutomationSubscription(sub.id(), sub.workspaceId(), sub.ownerId(), sub.name(), sub.triggerKind(),
                sub.dayOfWeek(), sub.localTime(), sub.timeZone(), sub.workItemId(), sub.maxItems(), sub.expiresAt(),
                sub.maxRuns(), sub.admittedRuns(), sub.nextFireAt(), sub.status(), sub.reason(), sub.activeRunId(),
                sub.rowVersion(), sub.createdAt(), sub.updatedAt(), actions);
    }

    private AutomationSubscription replayAction(ActorContext actor, UUID workspaceId, String keyHash, String requestHash) {
        var saved = jdbc.query("select request_hash, response_json::text from workflow.automation_action where tenant_id = ? "
                        + "and workspace_id = ? and owner_id = ? and key_hash = ?",
                (rs, n) -> new Object[]{rs.getString(1), rs.getString(2)}, actor.tenantId(), workspaceId, actor.actorId(), keyHash)
                .stream().findFirst().orElse(null);
        if (saved == null) return null;
        if (!requestHash.equals(saved[0])) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "Idempotency-Key 已用于其他自动化请求。");
        try {
            var stored = json.readValue((String) saved[1], AutomationSubscription.class);
            var current = requireSubscription(actor.tenantId(), workspaceId, actor.actorId(), stored.id(), true);
            return new AutomationSubscription(stored.id(), stored.workspaceId(), stored.ownerId(), stored.name(),
                    stored.triggerKind(), stored.dayOfWeek(), stored.localTime(), stored.timeZone(), stored.workItemId(),
                    stored.maxItems(), stored.expiresAt(), stored.maxRuns(), stored.admittedRuns(), stored.nextFireAt(),
                    stored.status(), stored.reason(), stored.activeRunId(), stored.version(), stored.createdAt(), stored.updatedAt(),
                    subscriptionSnapshot(current).allowedActions());
        } catch (Exception invalid) {
            if (invalid instanceof EafException eaf) throw eaf;
            throw EafException.conflict("IDEMPOTENCY_RECORD_INVALID", "自动化幂等记录无法恢复。");
        }
    }

    private void saveAction(ActorContext actor, UUID workspaceId, String keyHash, String requestHash,
            UUID subscriptionId, AutomationSubscription response) {
        try {
            jdbc.update("insert into workflow.automation_action(tenant_id, workspace_id, owner_id, key_hash, request_hash, "
                            + "subscription_id, response_json) values (?, ?, ?, ?, ?, ?, ?::jsonb)",
                    actor.tenantId(), workspaceId, actor.actorId(), keyHash, requestHash, subscriptionId,
                    json.writeValueAsString(response));
        } catch (JsonProcessingException invalid) {
            throw EafException.conflict("IDEMPOTENCY_RECORD_INVALID", "自动化响应不能保存为幂等快照。");
        }
    }

    private String idempotencyHash(ActorContext actor, UUID workspaceId, String key) {
        if (key == null || key.isBlank() || key.length() > 200)
            throw EafException.invalid("Idempotency-Key 必须为 1—200 个字符。");
        return Hashing.sha256(key.trim());
    }

    static Slot nextSlotAfter(Instant after, DayOfWeek day, LocalTime time, ZoneId zone) {
        var date = after.atZone(zone).toLocalDate();
        for (int offset = 0; offset <= 7; offset++) {
            var candidateDate = date.plusDays(offset);
            if (candidateDate.getDayOfWeek() != day) continue;
            var candidate = slot(candidateDate, time, zone);
            if (candidate.instant().isAfter(after)) return candidate;
        }
        throw new IllegalStateException("No future weekly slot found.");
    }

    static Slot latestSlotAtOrBefore(Instant at, DayOfWeek day, LocalTime time, ZoneId zone) {
        var date = at.atZone(zone).toLocalDate();
        var candidateDate = date.with(java.time.temporal.TemporalAdjusters.previousOrSame(day));
        var candidate = slot(candidateDate, time, zone);
        if (candidate.instant().isAfter(at)) candidate = slot(candidateDate.minusWeeks(1), time, zone);
        return candidate;
    }

    private static Slot slot(LocalDate date, LocalTime time, ZoneId zone) {
        // LocalDateTime.atZone 在 DST 间隙顺延，并在重叠时选择一个稳定偏移量。
        return new Slot(date, LocalDateTime.of(date, time).atZone(zone).toInstant());
    }

    private String weeklyTriggerKey(Slot slot, LocalTime time, ZoneId zone) {
        return "weekly:" + slot.localDate() + ":" + time + ":" + zone.getId() + ":" + slot.instant();
    }

    private String canonical(JsonNode node) {
        try { return json.writeValueAsString(node); }
        catch (JsonProcessingException invalid) { throw EafException.invalid("摘要 JSON 无法规范化。"); }
    }

    private JsonNode parseJson(String value, String field) {
        try { return json.readTree(value); }
        catch (Exception invalid) { throw EafException.invalid(field + " JSON 无效。"); }
    }

    private Instant instant(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        var value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private Instant instant(String value) {
        if (value == null || value.isBlank()) return null;
        try { return Instant.parse(value); }
        catch (RuntimeException invalid) { return null; }
    }

    private Long nullableLong(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        var value = rs.getObject(column);
        return value == null ? null : ((Number) value).longValue();
    }

    private UUID uuid(String value) {
        try { return UUID.fromString(value); }
        catch (RuntimeException invalid) { throw EafException.conflict("AUTOMATION_SOURCE_CHANGED", "摘要来源 UUID 无效。"); }
    }

    private record FixedAssets(AgentDefinition agent, CapabilityDefinition capability) { }
    record Slot(LocalDate localDate, Instant instant) { }
    private record Capture(String snapshotJson, String snapshotHash, boolean hasMore,
            List<AutomationDigestItem> items, Instant capturedAt) { }
    private record EventRow(UUID id, UUID tenantId, UUID workspaceId, String source, String eventId, String type,
            UUID workItemId, long sourceRowVersion, String status, UUID oldAssigneeId, UUID newAssigneeId,
            Instant occurredAt) { }
    private record SubscriptionRow(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String name,
            String triggerKind, String dayOfWeek, String localTime, String timeZone, UUID workItemId, int maxItems,
            Instant expiresAt, int maxRuns, int admittedRuns, Instant nextFireAt, String status, String reason,
            long authorizationEpoch, Instant epochStartedAt, UUID activeRunId, Long lastObservedSourceRowVersion,
            String requestHash, UUID agentId, String agentVersion, UUID capabilityId, String capabilityVersion,
            String capabilityHash, UUID skillId, String skillVersion, String skillHash, String profileHash,
            long rowVersion, Instant createdAt, Instant updatedAt) { }
    private record RunRow(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, UUID subscriptionId,
            long authorizationEpoch, String triggerKind, String triggerKey, Instant plannedAt, Instant occurredAt,
            String eventSource, String eventId, Long sourceRowVersion, String status, String reason,
            String inputSnapshot, String inputHash, UUID taskId, String taskStatus, int attempts,
            Instant nextAttemptAt, boolean generationAttempted, UUID generationTaskId, Integer generationAttempt,
            String resultJson, String modelResultJson, String resultMarkdown, Instant admittedAt,
            Instant finishedAt, Instant createdAt) {
        AutomationRun snapshot() {
            return new AutomationRun(id, subscriptionId, triggerKind, triggerKey, status, reason, plannedAt,
                    occurredAt, sourceRowVersion, taskId, taskStatus, attempts, nextAttemptAt, admittedAt, finishedAt, createdAt);
        }
    }
}
