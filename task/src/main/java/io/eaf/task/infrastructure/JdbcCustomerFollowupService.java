package io.eaf.task.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.identity.api.IdentityDirectory;
import io.eaf.organization.api.OrganizationDirectory;
import io.eaf.policy.api.CustomerResourceAccess;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.task.api.CustomerFollowupService;
import io.eaf.task.api.WorkflowTaskProvenance;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcCustomerFollowupService implements CustomerFollowupService {
    private static final Set<String> OUTCOMES = Set.of("CONTACTED", "NO_RESPONSE", "RESOLVED", "OTHER");
    private static final UUID RESULT_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-00000000000d");
    private static final String RESULT_WORKFLOW_VERSION = "1.0.0";
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final CustomerResourceAccess customers;
    private final IdentityDirectory identities;
    private final OrganizationDirectory organizations;
    private final ObjectMapper json;

    public JdbcCustomerFollowupService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces,
                                       CustomerResourceAccess customers, IdentityDirectory identities,
                                       OrganizationDirectory organizations, ObjectMapper json) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.customers = customers;
        this.identities = identities;
        this.organizations = organizations;
        this.json = json;
    }

    @Override
    @Transactional
    public CardSnapshot create(CreateCardCommand command) {
        requireHuman(command == null ? null : command.actor());
        validateCreate(command);
        var actor = command.actor();
        workspaces.require(actor, command.workspaceId(), "customer-followup:read");
        workspaces.require(actor, command.workspaceId(), "customer-followup:write");
        workspaces.require(actor, command.workspaceId(), "crm:followup:create");
        requireCustomerRead(actor, command.workspaceId(), command.customerId());
        var assigneeId = command.assigneeId() == null ? actor.actorId() : command.assigneeId();
        requireEligibleAssignee(actor.tenantId(), command.workspaceId(), command.customerId(), assigneeId);
        var hash = Hashing.sha256(String.join("\u001f", command.customerId(), command.sourceConversationId().toString(),
                command.sourceTaskId().toString(), Long.toString(command.sourceTaskVersion()),
                Integer.toString(command.sourceBriefRevision()), assigneeId.toString(), command.summary().trim(),
                String.valueOf(command.dueAt())));
        var commandId = UUID.randomUUID();
        var inserted = jdbc.update("insert into task.customer_followup_command(id, tenant_id, workspace_id, actor_id, action, request_key, request_hash) "
                        + "values (?, ?, ?, ?, 'CREATE', ?, ?) on conflict (tenant_id, workspace_id, actor_id, action, request_key) do nothing",
                commandId, actor.tenantId(), command.workspaceId(), actor.actorId(), command.requestKey(), hash);
        if (inserted == 0) {
            var replay = findCommand(actor, command.workspaceId(), "CREATE", command.requestKey());
            if (replay == null || !hash.equals(replay.requestHash()) || replay.followupId() == null)
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一跟进请求键已用于不同内容。");
            var existing = findCard(actor.tenantId(), command.workspaceId(), replay.followupId(), true);
            requireCustomerRead(actor, command.workspaceId(), existing.customerId());
            return existing;
        }
        var id = UUID.randomUUID();
        var now = Timestamp.from(Instant.now());
        jdbc.update("insert into task.customer_followup(id, tenant_id, workspace_id, customer_id, creator_id, assignee_id, summary, due_at, "
                        + "business_status, source_conversation_id, source_task_id, source_task_version, source_brief_revision, created_at, updated_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?::timestamptz, 'OPEN', ?, ?, ?, ?, ?, ?)",
                id, actor.tenantId(), command.workspaceId(), command.customerId(), actor.actorId(), assigneeId,
                command.summary().trim(), command.dueAt() == null ? null : Timestamp.from(command.dueAt()),
                command.sourceConversationId(), command.sourceTaskId(), command.sourceTaskVersion(),
                command.sourceBriefRevision(), now, now);
        jdbc.update("update task.customer_followup_command set followup_id = ? where id = ?", id, commandId);
        return findCard(actor.tenantId(), command.workspaceId(), id, false);
    }

    @Override
    @Transactional
    public CardSnapshot attachCreationWorkflow(ActorContext actor, UUID workspaceId, UUID followupId, UUID workflowId) {
        requireHuman(actor);
        var card = requireReadableCard(actor, workspaceId, followupId, true);
        if (!card.creatorId().equals(actor.actorId())) throw EafException.notFound();
        if (card.creationWorkflowId() != null && !card.creationWorkflowId().equals(workflowId))
            throw EafException.conflict("FOLLOWUP_WORKFLOW_CONFLICT", "团队跟进已绑定另一创建流程。");
        if (workflowId == null) throw EafException.invalid("创建 Workflow ID 必填。");
        jdbc.update("update task.customer_followup set create_workflow_id = ?, row_version = row_version + 1, updated_at = now() "
                        + "where id = ? and tenant_id = ? and workspace_id = ? and create_workflow_id is null",
                workflowId, followupId, actor.tenantId(), workspaceId);
        return findCard(actor.tenantId(), workspaceId, followupId, false);
    }

    @Override
    @Transactional(readOnly = true)
    public FollowupPage list(ActorContext actor, UUID workspaceId, ListQuery query) {
        requireHuman(actor);
        workspaces.require(actor, workspaceId, "customer-followup:read");
        validateListQuery(query);
        var where = new StringBuilder(" where tenant_id = ? and workspace_id = ?");
        var base = new ArrayList<Object>(List.of(actor.tenantId(), workspaceId));
        switch (query.scope()) {
            case "mine" -> { where.append(" and assignee_id = ?"); base.add(actor.actorId()); }
            case "created" -> { where.append(" and creator_id = ?"); base.add(actor.actorId()); }
            case "team" -> { }
            default -> throw EafException.invalid("scope 仅允许 mine、created 或 team。");
        }
        if (query.customerId() != null) { where.append(" and customer_id = ?"); base.add(query.customerId()); }
        if (query.assigneeId() != null) { where.append(" and assignee_id = ?"); base.add(query.assigneeId()); }
        if (query.status() != null) { where.append(" and business_status = ?"); base.add(query.status()); }
        if (Boolean.TRUE.equals(query.overdue()))
            where.append(" and due_at < now() and business_status in ('OPEN', 'IN_PROGRESS')");
        // 先在 Task 域分页，再用 Policy 公共 API 过滤；扫描游标不暴露不可见行数或 ID。
        var visible = new ArrayList<CardSnapshot>();
        Instant scanAt = query.cursorUpdatedAt();
        UUID scanId = query.cursorId();
        boolean exhausted = false;
        while (visible.size() <= query.limit() && !exhausted) {
            var args = new ArrayList<>(base);
            var batchWhere = where.toString();
            if (scanAt != null) {
                batchWhere += " and (updated_at, id) < (?, ?)";
                args.add(Timestamp.from(scanAt)); args.add(scanId);
            }
            args.add(200);
            var batch = jdbc.query("select * from task.customer_followup" + batchWhere
                            + " order by updated_at desc, id desc limit ?", (rs, row) -> mapCard(rs), args.toArray());
            if (batch.isEmpty()) break;
            for (var candidate : batch) {
                scanAt = candidate.updatedAt(); scanId = candidate.id();
                if (customers.canRead(actor, workspaceId, candidate.customerId())) visible.add(candidate);
                if (visible.size() > query.limit()) break;
            }
            exhausted = batch.size() < 200;
        }
        var hasMore = visible.size() > query.limit();
        var items = hasMore ? List.copyOf(visible.subList(0, query.limit())) : List.copyOf(visible);
        var last = items.isEmpty() ? null : items.get(items.size() - 1);
        return new FollowupPage(items, hasMore && last != null ? last.updatedAt() : null,
                hasMore && last != null ? last.id() : null);
    }

    @Override
    @Transactional(readOnly = true)
    public FollowupDetail get(ActorContext actor, UUID workspaceId, UUID followupId) {
        requireHuman(actor);
        var card = requireReadableCard(actor, workspaceId, followupId, false);
        var results = resultRows(actor.tenantId(), workspaceId, followupId, null, 20);
        var actions = new LinkedHashSet<String>();
        if (card.creatorId().equals(actor.actorId())) actions.add("UPDATE");
        if (card.assigneeId().equals(actor.actorId())) { actions.add("SAVE_RESULT"); actions.add("SYNC_RESULT"); }
        return new FollowupDetail(card, actions, results.items(), results.nextBeforeResultNo());
    }

    @Override
    @Transactional(readOnly = true)
    public ResultPage results(ActorContext actor, UUID workspaceId, UUID followupId, Integer beforeResultNo, int limit) {
        requireHuman(actor);
        requireReadableCard(actor, workspaceId, followupId, false);
        if (limit < 1 || limit > 50 || beforeResultNo != null && beforeResultNo < 1)
            throw EafException.invalid("结果时间线分页参数无效。");
        return resultRows(actor.tenantId(), workspaceId, followupId, beforeResultNo, limit);
    }

    @Override
    @Transactional
    public CardSnapshot update(UpdateCardCommand command) {
        requireHuman(command == null ? null : command.actor());
        if (command == null || command.workspaceId() == null || command.followupId() == null
                || command.expectedVersion() < 1 || !command.assigneeProvided() && !command.dueAtProvided())
            throw EafException.invalid("负责人或计划日期至少需要修改一项，并提供当前版本。");
        var actor = command.actor();
        var card = requireReadableCard(actor, command.workspaceId(), command.followupId(), true);
        card = findCard(actor.tenantId(), command.workspaceId(), command.followupId(), true);
        if (!card.creatorId().equals(actor.actorId())) throw EafException.forbidden("只有创建人可以分派负责人或调整计划日期。");
        if (card.rowVersion() != command.expectedVersion())
            throw EafException.conflict("VERSION_CONFLICT", "团队跟进已变化，请重新读取后再修改。");
        var assigneeId = command.assigneeProvided() ? command.assigneeId() : card.assigneeId();
        if (assigneeId == null) throw EafException.invalid("负责人不能清空。");
        if (command.assigneeProvided()) requireEligibleAssignee(actor.tenantId(), command.workspaceId(), card.customerId(), assigneeId);
        if (command.assigneeProvided() && !assigneeId.equals(card.assigneeId())) {
            var inFlight = jdbc.queryForObject("select exists(select 1 from task.customer_followup_sync "
                            + "where followup_id = ? and workflow_instance_id is not null)", Boolean.class, card.id());
            if (Boolean.TRUE.equals(inFlight))
                throw EafException.conflict("FOLLOWUP_SYNC_IN_PROGRESS", "结果同步已有流程关联，结束或核验该尝试后再转派。");
        }
        jdbc.update("update task.customer_followup set assignee_id = ?, due_at = case when ? then ?::timestamptz else due_at end, "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and row_version = ?",
                assigneeId, command.dueAtProvided(), command.dueAt() == null ? null : Timestamp.from(command.dueAt()),
                card.id(), command.expectedVersion());
        return findCard(actor.tenantId(), command.workspaceId(), card.id(), false);
    }

    @Override
    @Transactional
    public ResultReceipt appendResult(ResultCommand command) {
        requireHuman(command == null ? null : command.actor());
        validateResult(command);
        var actor = command.actor();
        var card = requireReadableCard(actor, command.workspaceId(), command.followupId(), true);
        card = findCard(actor.tenantId(), command.workspaceId(), command.followupId(), true);
        if (!card.assigneeId().equals(actor.actorId())) throw EafException.forbidden("只有当前负责人可以登记处理结果。");
        var hash = Hashing.sha256(String.join("\u001f", command.followupId().toString(),
                Long.toString(command.expectedVersion()), command.outcomeCode(), command.summary().trim(),
                String.valueOf(command.nextAction()), String.valueOf(command.nextContactAt()), command.disposition(),
                String.valueOf(command.correctsResultId())));
        var replay = findCommand(actor, command.workspaceId(), "RESULT", command.requestKey());
        if (replay != null) {
            if (!hash.equals(replay.requestHash()) || replay.resultId() == null)
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一结果请求键已用于不同内容。");
            var existingResult = findResult(actor.tenantId(), command.workspaceId(), card.id(), replay.resultId());
            return new ResultReceipt(findCard(actor.tenantId(), command.workspaceId(), card.id(), false), existingResult, false);
        }
        if (card.rowVersion() != command.expectedVersion())
            throw EafException.conflict("VERSION_CONFLICT", "团队跟进已变化，请重新读取后再保存结果。");
        if (command.correctsResultId() != null) {
            var previous = findResult(actor.tenantId(), command.workspaceId(), card.id(), command.correctsResultId());
            if (previous.resultNo() >= card.lastResultNo() + 1)
                throw EafException.invalid("更正只能引用同一跟进中更早的一条结果。");
        }
        var commandId = UUID.randomUUID();
        var inserted = jdbc.update("insert into task.customer_followup_command(id, tenant_id, workspace_id, actor_id, action, request_key, request_hash, followup_id) "
                        + "values (?, ?, ?, ?, 'RESULT', ?, ?, ?) on conflict (tenant_id, workspace_id, actor_id, action, request_key) do nothing",
                commandId, actor.tenantId(), command.workspaceId(), actor.actorId(), command.requestKey(), hash, card.id());
        if (inserted == 0) {
            replay = findCommand(actor, command.workspaceId(), "RESULT", command.requestKey());
            if (replay == null || !hash.equals(replay.requestHash()) || replay.resultId() == null)
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一结果请求键已用于不同内容。");
            var existingResult = findResult(actor.tenantId(), command.workspaceId(), card.id(), replay.resultId());
            return new ResultReceipt(findCard(actor.tenantId(), command.workspaceId(), card.id(), false), existingResult, false);
        }
        var resultId = UUID.randomUUID();
        var resultNo = card.lastResultNo() + 1;
        jdbc.update("insert into task.customer_followup_result(id, tenant_id, workspace_id, followup_id, result_no, recorded_by, outcome_code, summary, next_action, next_contact_at, disposition, corrects_result_id) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::timestamptz, ?, ?)",
                resultId, actor.tenantId(), command.workspaceId(), card.id(), resultNo, actor.actorId(), command.outcomeCode(),
                command.summary().trim(), blankToNull(command.nextAction()),
                command.nextContactAt() == null ? null : Timestamp.from(command.nextContactAt()),
                command.disposition(), command.correctsResultId());
        var status = "CLOSE".equals(command.disposition()) ? "CLOSED" : "IN_PROGRESS";
        jdbc.update("update task.customer_followup set last_result_no = ?, business_status = ?, due_at = ?::timestamptz, "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and row_version = ?",
                resultNo, status, command.nextContactAt() == null ? null : Timestamp.from(command.nextContactAt()),
                card.id(), command.expectedVersion());
        jdbc.update("update task.customer_followup_command set result_id = ? where id = ?", resultId, commandId);
        return new ResultReceipt(findCard(actor.tenantId(), command.workspaceId(), card.id(), false),
                findResult(actor.tenantId(), command.workspaceId(), card.id(), resultId), true);
    }

    @Override
    @Transactional
    public CardSnapshot recordCreationReceipt(ActorContext actor, UUID workspaceId, UUID followupId,
            UUID workflowId, UUID operationId, String externalId) {
        requireHuman(actor);
        var card = requireReadableCard(actor, workspaceId, followupId, true);
        card = findCard(actor.tenantId(), workspaceId, followupId, true);
        if (!card.creatorId().equals(actor.actorId())) throw EafException.forbidden("只有创建人可关联原 CRM 跟进回执。");
        if (card.creationWorkflowId() == null || !card.creationWorkflowId().equals(workflowId)
                || operationId == null || externalId == null || externalId.isBlank() || externalId.length() > 160)
            throw EafException.conflict("FOLLOWUP_CREATE_UNVERIFIED", "原 CRM 跟进缺少匹配的已核验创建回执。");
        var current = jdbc.query("select create_operation_id, create_external_id from task.customer_followup "
                        + "where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject(1, UUID.class), rs.getString(2)} : null,
                followupId, actor.tenantId(), workspaceId);
        if (current == null) throw EafException.notFound();
        if (current[0] != null && (!operationId.equals(current[0]) || !externalId.equals(current[1])))
            throw EafException.conflict("FOLLOWUP_CREATE_RECEIPT_CONFLICT", "原创建回执已固定为另一 CRM 跟进。");
        jdbc.update("update task.customer_followup set create_operation_id = ?, create_external_id = ?, "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and create_workflow_id = ?",
                operationId, externalId, followupId, workflowId);
        return findCard(actor.tenantId(), workspaceId, followupId, false);
    }

    @Override
    @Transactional
    public void recordCreationReceiptForTask(ActorContext actor, UUID workspaceId, UUID taskId,
            UUID operationId, String externalId) {
        requireHuman(actor);
        var provenance = jdbc.query("select actor_id, run_kind, tool_name, workflow_instance_id from task.task "
                        + "where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("actor_id", UUID.class), rs.getString("run_kind"),
                        rs.getString("tool_name"), rs.getObject("workflow_instance_id", UUID.class)} : null,
                taskId, actor.tenantId(), workspaceId);
        if (provenance == null || !actor.actorId().equals(provenance[0]) || !"TOOL_EXECUTION".equals(provenance[1])
                || !"crm.followup.create".equals(provenance[2]) || provenance[3] == null)
            return;
        var followupId = jdbc.query("select id from task.customer_followup where tenant_id = ? and workspace_id = ? "
                        + "and create_workflow_id = ?",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null,
                actor.tenantId(), workspaceId, provenance[3]);
        if (followupId == null) return;
        recordCreationReceipt(actor, workspaceId, followupId, (UUID) provenance[3], operationId, externalId);
    }

    @Override
    @Transactional
    public SyncAttempt startSync(SyncCommand command) {
        requireHuman(command == null ? null : command.actor());
        if (command == null || command.workspaceId() == null || command.followupId() == null
                || command.resultId() == null || command.requestKey() == null || command.requestKey().isBlank()
                || command.requestKey().length() > 200)
            throw EafException.invalid("结果同步请求字段无效。");
        var actor = command.actor();
        workspaces.require(actor, command.workspaceId(), "customer-followup:read");
        workspaces.require(actor, command.workspaceId(), "customer-followup:write");
        workspaces.require(actor, command.workspaceId(), "crm:followup:result");
        workspaces.require(actor, command.workspaceId(), "workflow:start");
        workspaces.require(actor, command.workspaceId(), "task:create");
        var card = findCard(actor.tenantId(), command.workspaceId(), command.followupId(), true);
        requireCustomerRead(actor, command.workspaceId(), card.customerId());
        if (!card.assigneeId().equals(actor.actorId())) throw EafException.forbidden("只有当前负责人可以申请同步结果。");
        var result = findResult(actor.tenantId(), command.workspaceId(), card.id(), command.resultId());
        var storedCreateReceipt = jdbc.queryForObject("select create_external_id is not null from task.customer_followup "
                + "where id = ? and tenant_id = ? and workspace_id = ?", Boolean.class,
                card.id(), actor.tenantId(), command.workspaceId());
        if (!Boolean.TRUE.equals(storedCreateReceipt))
            throw EafException.conflict("FOLLOWUP_CREATE_UNVERIFIED", "先确认原 CRM 跟进已由 Execution 核验成功。");
        var hash = Hashing.sha256(command.followupId() + "\u001f" + command.resultId());
        var replay = findCommand(actor, command.workspaceId(), "SYNC", command.requestKey());
        if (replay != null) {
            if (!hash.equals(replay.requestHash()) || replay.syncAttemptId() == null)
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同步请求键已用于其他客户结果。");
            return findSync(actor.tenantId(), command.workspaceId(), replay.syncAttemptId());
        }
        var previousForResult = jdbc.query("select * from task.customer_followup_sync where followup_id = ? and result_id = ? "
                        + "and tenant_id = ? and workspace_id = ? order by attempt_no desc limit 1",
                rs -> rs.next() ? mapSync(rs) : null, card.id(), result.id(), actor.tenantId(), command.workspaceId());
        if (previousForResult != null && "SUCCEEDED".equals(previousForResult.terminalState())) return previousForResult;
        if (previousForResult != null && !previousForResult.admissionOpen()
                && !"FAILED_SAFE".equals(previousForResult.terminalState()))
            throw EafException.conflict("FOLLOWUP_SYNC_UNRESOLVED", "上次同步结果尚未确认，必须先核验原 operationId。");
        var open = jdbc.query("select * from task.customer_followup_sync where followup_id = ? and tenant_id = ? "
                        + "and workspace_id = ? and admission_open order by created_at desc limit 1",
                rs -> rs.next() ? mapSync(rs) : null, card.id(), actor.tenantId(), command.workspaceId());
        if (open != null) throw EafException.conflict("FOLLOWUP_SYNC_IN_PROGRESS", "这张跟进已有结果同步在途，请先核验原操作。");

        var commandId = UUID.randomUUID();
        var insertedCommand = jdbc.update("insert into task.customer_followup_command(id, tenant_id, workspace_id, actor_id, action, request_key, request_hash, followup_id, result_id) "
                        + "values (?, ?, ?, ?, 'SYNC', ?, ?, ?, ?) on conflict (tenant_id, workspace_id, actor_id, action, request_key) do nothing",
                commandId, actor.tenantId(), command.workspaceId(), actor.actorId(), command.requestKey(), hash,
                card.id(), result.id());
        if (insertedCommand == 0) {
            replay = findCommand(actor, command.workspaceId(), "SYNC", command.requestKey());
            if (replay == null || !hash.equals(replay.requestHash()) || replay.syncAttemptId() == null)
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同步请求键已用于其他客户结果。");
            return findSync(actor.tenantId(), command.workspaceId(), replay.syncAttemptId());
        }
        var attemptNo = jdbc.queryForObject("select coalesce(max(attempt_no), 0) + 1 from task.customer_followup_sync "
                + "where followup_id = ? and result_id = ?", Integer.class, card.id(), result.id());
        var syncId = UUID.randomUUID();
        jdbc.update("insert into task.customer_followup_sync(id, tenant_id, workspace_id, followup_id, result_id, attempt_no, submitted_by) "
                        + "values (?, ?, ?, ?, ?, ?, ?)", syncId, actor.tenantId(), command.workspaceId(),
                card.id(), result.id(), attemptNo, actor.actorId());
        jdbc.update("update task.customer_followup_command set sync_attempt_id = ? where id = ?", syncId, commandId);
        return findSync(actor.tenantId(), command.workspaceId(), syncId);
    }

    @Override
    @Transactional
    public SyncAttempt attachSyncWorkflow(ActorContext actor, UUID workspaceId, UUID syncAttemptId, UUID workflowId) {
        requireHuman(actor);
        var sync = findSync(actor.tenantId(), workspaceId, syncAttemptId);
        if (!sync.submittedBy().equals(actor.actorId()) || workflowId == null)
            throw EafException.notFound();
        if (sync.workflowInstanceId() != null && !sync.workflowInstanceId().equals(workflowId))
            throw EafException.conflict("FOLLOWUP_SYNC_WORKFLOW_CONFLICT", "结果同步尝试已绑定另一 Workflow。");
        jdbc.update("update task.customer_followup_sync set workflow_instance_id = ? "
                        + "where id = ? and tenant_id = ? and workspace_id = ? and workflow_instance_id is null",
                workflowId, syncAttemptId, actor.tenantId(), workspaceId);
        return findSync(actor.tenantId(), workspaceId, syncAttemptId);
    }

    @Override
    @Transactional(readOnly = true)
    public List<SyncAttempt> syncAttempts(ActorContext actor, UUID workspaceId, UUID followupId) {
        requireHuman(actor);
        var card = requireReadableCard(actor, workspaceId, followupId, false);
        return jdbc.query("select * from task.customer_followup_sync where followup_id = ? and tenant_id = ? "
                        + "and workspace_id = ? order by created_at desc, id desc limit 50",
                (rs, row) -> mapSync(rs), followupId, actor.tenantId(), workspaceId);
    }

    @Override
    @Transactional
    public void closeSyncAdmission(ActorContext actor, UUID workspaceId, UUID syncAttemptId, String terminalState) {
        requireHuman(actor);
        if (!Set.of("SUCCEEDED", "FAILED_SAFE").contains(terminalState))
            throw EafException.invalid("同步准入只能以已核验成功或确认未写入的状态结束。");
        var sync = findSync(actor.tenantId(), workspaceId, syncAttemptId);
        var card = requireReadableCard(actor, workspaceId, sync.followupId(), true);
        if (!card.assigneeId().equals(actor.actorId()) || !sync.submittedBy().equals(actor.actorId()))
            throw EafException.forbidden("只有同步申请人仍为当前负责人时可以结束准入锁。");
        jdbc.update("update task.customer_followup_sync set admission_open = false, terminal_state = ? "
                        + "where id = ? and admission_open and tenant_id = ? and workspace_id = ?",
                terminalState, syncAttemptId, actor.tenantId(), workspaceId);
    }

    @Override
    @Transactional(readOnly = true)
    public void requireSyncTaskCreation(ActorContext actor, UUID workspaceId, WorkflowTaskProvenance provenance,
            String toolName, String toolArgumentsJson) {
        requireHuman(actor);
        if (provenance == null || !provenance.complete() || !RESULT_WORKFLOW_ID.equals(provenance.workflowId())
                || !RESULT_WORKFLOW_VERSION.equals(provenance.workflowVersion()) || !"record".equals(provenance.stepId())
                || !"crm.followup.result.record".equals(toolName))
            throw EafException.forbidden("结果 Tool Task 只能来自固定同步 Workflow 的 record 步骤。");
        var ids = parseSyncIds(toolArgumentsJson);
        var sync = jdbc.query("select s.*, f.assignee_id, f.customer_id from task.customer_followup_sync s "
                        + "join task.customer_followup f on f.id = s.followup_id and f.tenant_id = s.tenant_id "
                        + "and f.workspace_id = s.workspace_id where s.workflow_instance_id = ? and s.followup_id = ? "
                        + "and s.result_id = ? and s.tenant_id = ? and s.workspace_id = ? and s.submitted_by = ? and s.admission_open",
                rs -> rs.next() ? new Object[]{rs.getObject("id", UUID.class), rs.getObject("submitted_by", UUID.class),
                        rs.getObject("assignee_id", UUID.class), rs.getString("customer_id")} : null,
                provenance.workflowInstanceId(), ids.followupId(), ids.resultId(), actor.tenantId(), workspaceId, actor.actorId());
        if (sync == null || !actor.actorId().equals(sync[1]) || !actor.actorId().equals(sync[2]))
            throw EafException.forbidden("同步 Workflow 与当前处理人或已登记结果不匹配。");
        workspaces.require(actor, workspaceId, "crm:followup:result");
        requireCustomerRead(actor, workspaceId, (String) sync[3]);
    }

    @Override
    @Transactional(readOnly = true)
    public SyncWritePayload requireSyncWritePayload(ActorContext actor, UUID workspaceId, UUID taskId, int attempt,
            UUID followupId, UUID resultId) {
        requireHuman(actor);
        var task = jdbc.query("select actor_id, attempt, source, run_kind, workflow_instance_id, workflow_id, workflow_version, workflow_step_id "
                        + "from task.task where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("actor_id", UUID.class), rs.getInt("attempt"),
                        rs.getString("source"), rs.getString("run_kind"), rs.getObject("workflow_instance_id", UUID.class),
                        rs.getObject("workflow_id", UUID.class), rs.getString("workflow_version"), rs.getString("workflow_step_id")} : null,
                taskId, actor.tenantId(), workspaceId);
        if (task == null || !actor.actorId().equals(task[0]) || (int) task[1] != attempt || !"USER".equals(task[2])
                || !"TOOL_EXECUTION".equals(task[3]) || !RESULT_WORKFLOW_ID.equals(task[5])
                || !RESULT_WORKFLOW_VERSION.equals(task[6]) || !"record".equals(task[7]))
            throw EafException.forbidden("Execution Task 缺少可信 Workflow 来源。");
        var row = jdbc.query("select f.customer_id, f.create_external_id, f.assignee_id, r.result_no, r.recorded_by, "
                        + "r.outcome_code, r.summary, r.next_action, r.next_contact_at, r.disposition, s.id sync_attempt_id, s.submitted_by "
                        + "from task.customer_followup_sync s join task.customer_followup f on f.id = s.followup_id "
                        + "and f.tenant_id = s.tenant_id and f.workspace_id = s.workspace_id "
                        + "join task.customer_followup_result r on r.id = s.result_id and r.followup_id = f.id "
                        + "and r.tenant_id = f.tenant_id and r.workspace_id = f.workspace_id "
                        + "where s.workflow_instance_id = ? and s.followup_id = ? and s.result_id = ? "
                        + "and s.tenant_id = ? and s.workspace_id = ? and s.submitted_by = ? and s.admission_open",
                rs -> rs.next() ? new Object[]{rs.getString("customer_id"), rs.getString("create_external_id"),
                        rs.getObject("assignee_id", UUID.class), rs.getInt("result_no"), rs.getObject("recorded_by", UUID.class),
                        rs.getString("outcome_code"), rs.getString("summary"), rs.getString("next_action"),
                        rs.getTimestamp("next_contact_at") == null ? null : rs.getTimestamp("next_contact_at").toInstant(),
                        rs.getString("disposition"), rs.getObject("sync_attempt_id", UUID.class),
                        rs.getObject("submitted_by", UUID.class)} : null,
                task[4], followupId, resultId, actor.tenantId(), workspaceId, actor.actorId());
        if (row == null || !actor.actorId().equals(row[2]) || !actor.actorId().equals(row[11])
                || row[1] == null || ((String) row[1]).isBlank())
            throw EafException.forbidden("结果同步尝试已失效、失去负责人资格或缺少原 CRM 跟进回执。");
        workspaces.require(actor, workspaceId, "crm:followup:result");
        requireCustomerRead(actor, workspaceId, (String) row[0]);
        return new SyncWritePayload(followupId, resultId, (UUID) row[10], (String) row[0], (String) row[1],
                (int) row[3], (UUID) row[4], (String) row[5], (String) row[6], (String) row[7],
                (Instant) row[8], (String) row[9], (UUID) row[11]);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Assignee> eligibleAssignees(ActorContext actor, UUID workspaceId, String customerId, int limit) {
        requireHuman(actor);
        if (limit < 1 || limit > 50) throw EafException.invalid("负责人候选 limit 必须为 1—50。");
        requireReadableCustomer(actor, workspaceId, customerId);
        var ids = customers.activeReaders(actor.tenantId(), workspaceId, customerId).stream()
                .filter(id -> isEligibleAssignee(actor.tenantId(), workspaceId, customerId, id))
                .limit(limit).collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        var names = identities.displayNames(actor.tenantId(), ids);
        return ids.stream().map(id -> new Assignee(id, names.getOrDefault(id, id.toString()))).toList();
    }

    @Override
    @Transactional(readOnly = true)
    public List<AnalysisResult> selectForAnalysis(ActorContext actor, UUID workspaceId, String customerId,
                                                   List<UUID> resultIds) {
        requireHuman(actor);
        requireReadableCustomer(actor, workspaceId, customerId);
        if (resultIds == null || resultIds.isEmpty() || resultIds.size() > 3
                || resultIds.stream().anyMatch(java.util.Objects::isNull)
                || new LinkedHashSet<>(resultIds).size() != resultIds.size())
            throw EafException.invalid("最多选择 3 条不同的客户跟进结果。");
        var selected = new ArrayList<AnalysisResult>();
        var total = 0;
        for (var id : resultIds) {
            var row = jdbc.query("select r.*, f.customer_id, f.creator_id, "
                            + "case when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id and s.admission_open) "
                            + "then 'PENDING' when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id and s.terminal_state = 'SUCCEEDED') "
                            + "then 'VERIFIED' when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id and s.terminal_state = 'FAILED_SAFE') "
                            + "then 'FAILED_SAFE' when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id) "
                            + "then 'UNKNOWN' else 'NOT_REQUESTED' end sync_status "
                            + "from task.customer_followup_result r "
                            + "join task.customer_followup f on f.id = r.followup_id and f.tenant_id = r.tenant_id "
                            + "and f.workspace_id = r.workspace_id where r.id = ? and r.tenant_id = ? and r.workspace_id = ?",
                    rs -> rs.next() ? new Object[]{mapResult(rs), rs.getString("customer_id"),
                            rs.getObject("creator_id", UUID.class)} : null, id, actor.tenantId(), workspaceId);
            if (row == null || !customerId.equals(row[1])) throw EafException.notFound();
            var result = (ResultSnapshot) row[0];
            total += result.summary().length() + (result.nextAction() == null ? 0 : result.nextAction().length());
            selected.add(new AnalysisResult(result, customerId, (UUID) row[2], result.syncStatus()));
        }
        if (total > 2_000) throw EafException.invalid("所选结果正文合计超过 2000 字符，请减少选择。");
        return List.copyOf(selected);
    }

    private void requireEligibleAssignee(UUID tenantId, UUID workspaceId, String customerId, UUID actorId) {
        if (!organizations.isActiveMember(tenantId, actorId)
                || !workspaces.isAuthorized(tenantId, actorId, workspaceId, "customer-followup:read")
                || !workspaces.isAuthorized(tenantId, actorId, workspaceId, "customer-followup:write")
                || !workspaces.isAuthorized(tenantId, actorId, workspaceId, "crm:followup:result")
                || !workspaces.isAuthorized(tenantId, actorId, workspaceId, "workflow:start")
                || !workspaces.isAuthorized(tenantId, actorId, workspaceId, "task:create")
                || !customers.activeReaders(tenantId, workspaceId, customerId).contains(actorId))
            throw EafException.forbidden("负责人必须是当前有效成员，并具有该客户和结果同步所需权限。");
    }

    private boolean isEligibleAssignee(UUID tenantId, UUID workspaceId, String customerId, UUID actorId) {
        return organizations.isActiveMember(tenantId, actorId)
                && workspaces.isAuthorized(tenantId, actorId, workspaceId, "customer-followup:read")
                && workspaces.isAuthorized(tenantId, actorId, workspaceId, "customer-followup:write")
                && workspaces.isAuthorized(tenantId, actorId, workspaceId, "crm:followup:result")
                && workspaces.isAuthorized(tenantId, actorId, workspaceId, "workflow:start")
                && workspaces.isAuthorized(tenantId, actorId, workspaceId, "task:create")
                && customers.activeReaders(tenantId, workspaceId, customerId).contains(actorId);
    }

    private CardSnapshot requireReadableCard(ActorContext actor, UUID workspaceId, UUID followupId, boolean write) {
        workspaces.require(actor, workspaceId, "customer-followup:read");
        if (write) workspaces.require(actor, workspaceId, "customer-followup:write");
        var card = findCard(actor.tenantId(), workspaceId, followupId, false);
        requireCustomerRead(actor, workspaceId, card.customerId());
        return card;
    }

    private void requireCustomerRead(ActorContext actor, UUID workspaceId, String customerId) {
        if (!customers.canRead(actor, workspaceId, customerId)) throw EafException.notFound();
    }

    private void requireReadableCustomer(ActorContext actor, UUID workspaceId, String customerId) {
        workspaces.require(actor, workspaceId, "customer-followup:read");
        if (customerId == null || customerId.isBlank() || customerId.length() > 160)
            throw EafException.invalid("customerId 无效。");
        requireCustomerRead(actor, workspaceId, customerId);
    }

    private CardSnapshot findCard(UUID tenantId, UUID workspaceId, UUID id, boolean lock) {
        var suffix = lock ? " for update" : "";
        var value = jdbc.query("select * from task.customer_followup where id = ? and tenant_id = ? and workspace_id = ?" + suffix,
                rs -> rs.next() ? mapCard(rs) : null, id, tenantId, workspaceId);
        if (value == null) throw EafException.notFound();
        return value;
    }

    private ResultPage resultRows(UUID tenantId, UUID workspaceId, UUID followupId, Integer before, int limit) {
        if (limit < 1 || limit > 50) throw EafException.invalid("limit 必须为 1—50。");
        var args = new ArrayList<Object>(List.of(followupId, tenantId, workspaceId));
        var where = " where r.followup_id = ? and r.tenant_id = ? and r.workspace_id = ?";
        if (before != null) { where += " and r.result_no < ?"; args.add(before); }
        args.add(limit + 1);
        var rows = jdbc.query("select r.*, case when exists(select 1 from task.customer_followup_sync s "
                        + "where s.result_id = r.id and s.admission_open) then 'PENDING' "
                        + "when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id and s.terminal_state = 'SUCCEEDED') then 'VERIFIED' "
                        + "when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id and s.terminal_state = 'FAILED_SAFE') then 'FAILED_SAFE' "
                        + "when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id) then 'UNKNOWN' "
                        + "else 'NOT_REQUESTED' end sync_status "
                        + "from task.customer_followup_result r" + where + " order by r.result_no desc limit ?",
                (rs, row) -> mapResult(rs), args.toArray());
        var more = rows.size() > limit;
        var items = more ? List.copyOf(rows.subList(0, limit)) : List.copyOf(rows);
        var next = more && !items.isEmpty() ? items.get(items.size() - 1).resultNo() : null;
        return new ResultPage(items, next);
    }

    private ResultSnapshot findResult(UUID tenantId, UUID workspaceId, UUID followupId, UUID resultId) {
        var value = jdbc.query("select r.*, case when exists(select 1 from task.customer_followup_sync s "
                        + "where s.result_id = r.id and s.admission_open) then 'PENDING' "
                        + "when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id and s.terminal_state = 'SUCCEEDED') then 'VERIFIED' "
                        + "when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id and s.terminal_state = 'FAILED_SAFE') then 'FAILED_SAFE' "
                        + "when exists(select 1 from task.customer_followup_sync s where s.result_id = r.id) then 'UNKNOWN' "
                        + "else 'NOT_REQUESTED' end sync_status "
                        + "from task.customer_followup_result r where r.id = ? and r.followup_id = ? and r.tenant_id = ? and r.workspace_id = ?",
                rs -> rs.next() ? mapResult(rs) : null, resultId, followupId, tenantId, workspaceId);
        if (value == null) throw EafException.notFound();
        return value;
    }

    private CommandReplay findCommand(ActorContext actor, UUID workspaceId, String action, String requestKey) {
        return jdbc.query("select request_hash, followup_id, result_id, sync_attempt_id from task.customer_followup_command "
                        + "where tenant_id = ? and workspace_id = ? and actor_id = ? and action = ? and request_key = ?",
                rs -> rs.next() ? new CommandReplay(rs.getString("request_hash"),
                        rs.getObject("followup_id", UUID.class), rs.getObject("result_id", UUID.class),
                        rs.getObject("sync_attempt_id", UUID.class)) : null,
                actor.tenantId(), workspaceId, actor.actorId(), action, requestKey);
    }

    private SyncAttempt findSync(UUID tenantId, UUID workspaceId, UUID id) {
        var value = jdbc.query("select * from task.customer_followup_sync where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? mapSync(rs) : null, id, tenantId, workspaceId);
        if (value == null) throw EafException.notFound();
        return value;
    }

    private SyncAttempt mapSync(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new SyncAttempt(rs.getObject("id", UUID.class), rs.getObject("followup_id", UUID.class),
                rs.getObject("result_id", UUID.class), rs.getInt("attempt_no"),
                rs.getObject("submitted_by", UUID.class), rs.getObject("workflow_instance_id", UUID.class),
                rs.getBoolean("admission_open"), rs.getString("terminal_state"), rs.getTimestamp("created_at").toInstant());
    }

    private SyncIds parseSyncIds(String raw) {
        try {
            var root = json.readTree(raw);
            if (root == null || !root.isObject() || !root.hasNonNull("followupId") || !root.hasNonNull("resultId"))
                throw EafException.invalid("结果同步参数缺少跟进和结果标识。");
            return new SyncIds(UUID.fromString(root.get("followupId").asText()),
                    UUID.fromString(root.get("resultId").asText()));
        } catch (EafException e) {
            throw e;
        } catch (Exception e) {
            throw EafException.invalid("结果同步参数无效。");
        }
    }

    private CardSnapshot mapCard(java.sql.ResultSet rs) throws java.sql.SQLException {
        var dueAt = rs.getTimestamp("due_at");
        return new CardSnapshot(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getString("customer_id"), rs.getObject("creator_id", UUID.class),
                rs.getObject("assignee_id", UUID.class), rs.getString("summary"), dueAt == null ? null : dueAt.toInstant(),
                rs.getString("business_status"), rs.getObject("source_conversation_id", UUID.class),
                rs.getObject("source_task_id", UUID.class), rs.getLong("source_task_version"),
                rs.getInt("source_brief_revision"), rs.getObject("create_workflow_id", UUID.class),
                rs.getInt("last_result_no"), rs.getLong("row_version"), rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    private ResultSnapshot mapResult(java.sql.ResultSet rs) throws java.sql.SQLException {
        var nextContact = rs.getTimestamp("next_contact_at");
        return new ResultSnapshot(rs.getObject("id", UUID.class), rs.getObject("followup_id", UUID.class),
                rs.getInt("result_no"), rs.getObject("recorded_by", UUID.class), rs.getString("outcome_code"),
                rs.getString("summary"), rs.getString("next_action"), nextContact == null ? null : nextContact.toInstant(),
                rs.getString("disposition"), rs.getObject("corrects_result_id", UUID.class),
                rs.getTimestamp("created_at").toInstant(), rs.getString("sync_status"));
    }

    private void requireHuman(ActorContext actor) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("团队客户跟进仅允许当前直接 HUMAN 成员操作。");
    }

    private void validateCreate(CreateCardCommand c) {
        if (c == null || c.workspaceId() == null || c.sourceConversationId() == null || c.sourceTaskId() == null
                || c.sourceTaskVersion() < 1 || c.sourceBriefRevision() < 0 || c.summary() == null
                || c.summary().isBlank() || c.summary().trim().length() > 2_000 || c.requestKey() == null
                || c.requestKey().isBlank() || c.requestKey().length() > 200)
            throw EafException.invalid("团队跟进创建字段无效。");
        if (c.customerId() == null || c.customerId().isBlank() || c.customerId().length() > 160)
            throw EafException.invalid("客户绑定无效。");
    }

    private void validateListQuery(ListQuery q) {
        if (q == null || q.scope() == null || q.limit() < 1 || q.limit() > 50
                || (q.cursorUpdatedAt() == null) != (q.cursorId() == null)
                || q.status() != null && !Set.of("OPEN", "IN_PROGRESS", "CLOSED").contains(q.status()))
            throw EafException.invalid("团队跟进列表筛选或游标无效。");
    }

    private void validateResult(ResultCommand c) {
        if (c == null || c.workspaceId() == null || c.followupId() == null || c.expectedVersion() < 1
                || c.outcomeCode() == null || !OUTCOMES.contains(c.outcomeCode()) || c.summary() == null
                || c.summary().isBlank() || c.summary().trim().length() > 2_000
                || c.nextAction() != null && c.nextAction().length() > 500
                || c.disposition() == null || !Set.of("CONTINUE", "CLOSE").contains(c.disposition())
                || c.requestKey() == null || c.requestKey().isBlank() || c.requestKey().length() > 200)
            throw EafException.invalid("处理结果字段无效。");
    }

    private String blankToNull(String value) { return value == null || value.isBlank() ? null : value.trim(); }

    private record SyncIds(UUID followupId, UUID resultId) { }
    private record CommandReplay(String requestHash, UUID followupId, UUID resultId, UUID syncAttemptId) { }
}
