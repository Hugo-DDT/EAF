package io.eaf.approval.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.approval.api.ApprovalBinding;
import io.eaf.approval.api.ApprovalCreateCommand;
import io.eaf.approval.api.ApprovalDecisionCommand;
import io.eaf.approval.api.ApprovalService;
import io.eaf.approval.api.ApprovalSnapshot;
import io.eaf.approval.api.ApprovalOutboxItem;
import io.eaf.approval.api.ApprovalOutboxPage;
import io.eaf.approval.api.ApprovalPendingPage;
import io.eaf.approval.api.ApprovalActionablePage;
import io.eaf.identity.api.IdentityDirectory;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcApprovalService implements ApprovalService {
    private static final Set<String> OUTBOX_STATUSES = Set.of("PENDING", "SENT", "FAILED");
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final WorkspaceAuthorization workspaces;
    private final IdentityDirectory identities;
    private final Clock clock;

    public JdbcApprovalService(JdbcTemplate jdbc, ObjectMapper json, WorkspaceAuthorization workspaces,
                               IdentityDirectory identities, Clock clock) {
        this.jdbc = jdbc;
        this.json = json;
        this.workspaces = workspaces;
        this.identities = identities;
        this.clock = clock;
    }

    @Override
    @Transactional
    public ApprovalSnapshot request(ApprovalCreateCommand command) {
        if (command == null || command.actor() == null || command.binding() == null || command.taskId() == null)
            throw EafException.invalid("审批请求缺少绑定事实。");
        var binding = command.binding();
        if (!command.actor().tenantId().equals(binding.tenantId()) || !command.workspaceId().equals(binding.workspaceId())
                || !command.actor().actorId().equals(binding.requesterId()) || binding.executionId() == null
                || binding.expiresAt() == null || !binding.expiresAt().isAfter(Instant.now(clock)))
            throw EafException.invalid("审批绑定与当前请求不一致或已过期。");
        workspaces.require(command.actor(), command.workspaceId(), "task:create");
        var bindingJson = write(binding);
        var bindingHash = Hashing.sha256(bindingJson);
        var existing = jdbc.query("select id, tenant_id, workspace_id, task_id, binding_json::text, state, decided_by, decided_at, row_version, created_at from approval.request where tenant_id = ? and workspace_id = ? and execution_id = ?",
                rs -> rs.next() ? map(rs) : null, binding.tenantId(), binding.workspaceId(), binding.executionId());
        if (existing != null) return existing;
        var id = UUID.randomUUID();
        try {
            jdbc.update("insert into approval.request(id, tenant_id, workspace_id, task_id, execution_id, idempotency_key, binding_hash, binding_json, state, expires_at, row_version, created_at) values (?, ?, ?, ?, ?, ?, ?, ?::jsonb, 'PENDING', ?, 1, ?)",
                    id, binding.tenantId(), binding.workspaceId(), command.taskId(), binding.executionId(), command.idempotencyKey(), bindingHash,
                    bindingJson, Timestamp.from(binding.expiresAt()), Timestamp.from(Instant.now(clock)));
            jdbc.update("insert into approval.outbox(event_id, tenant_id, workspace_id, approval_id, event_type, payload_json) values (?, ?, ?, ?, 'eaf.approval.requested.v1', ?::jsonb)",
                    UUID.randomUUID(), binding.tenantId(), binding.workspaceId(), id, "{\"approvalId\":\"" + id + "\",\"executionId\":\"" + binding.executionId() + "\",\"bindingHash\":\"" + bindingHash + "\"}");
        } catch (DuplicateKeyException duplicate) {
            var same = jdbc.query("select id, tenant_id, workspace_id, task_id, binding_json::text, state, decided_by, decided_at, row_version, created_at from approval.request where tenant_id = ? and workspace_id = ? and execution_id = ?",
                    rs -> rs.next() ? map(rs) : null, binding.tenantId(), binding.workspaceId(), binding.executionId());
            if (same != null) return same;
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "审批幂等键已被其他操作占用。");
        }
        return getInternal(id);
    }

    @Override
    public ApprovalSnapshot get(io.eaf.shared.ActorContext actor, UUID workspaceId, UUID approvalId) {
        workspaces.require(actor, workspaceId, "approval:read");
        var result = getVisible(actor.tenantId(), workspaceId, approvalId);
        if (result == null) throw EafException.notFound();
        return result;
    }

    @Override
    public ApprovalPendingPage listPending(io.eaf.shared.ActorContext actor, UUID workspaceId,
                                           Instant cursorCreatedAt, UUID cursorId, int pageSize) {
        if (actor == null || actor.type() != ActorType.HUMAN)
            throw EafException.forbidden("待审批列表只向有权限的 HUMAN 审批人开放。");
        var access = workspaces.require(actor, workspaceId, "approval:read");
        if (pageSize < 1 || pageSize > 50 || (cursorCreatedAt == null) != (cursorId == null))
            throw EafException.invalid("待审批列表分页参数无效。");
        var args = new ArrayList<Object>(List.of(access.tenantId(), workspaceId));
        var where = new StringBuilder(" where tenant_id = ? and workspace_id = ? and state = 'PENDING'");
        if (cursorCreatedAt != null) {
            where.append(" and (created_at, id) < (?, ?)");
            args.add(Timestamp.from(cursorCreatedAt));
            args.add(cursorId);
        }
        args.add(pageSize + 1);
        var selected = jdbc.query("select id, task_id, state, expires_at, created_at from approval.request"
                        + where + " order by created_at desc, id desc limit ?",
                (rs, row) -> new ApprovalPendingPage.Item(rs.getObject("id", UUID.class),
                        rs.getObject("task_id", UUID.class), rs.getString("state"),
                        rs.getTimestamp("expires_at").toInstant(), rs.getTimestamp("created_at").toInstant()),
                args.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        return new ApprovalPendingPage(items, last == null ? null : last.createdAt(),
                last == null ? null : last.id());
    }

    @Override
    public ApprovalActionablePage listActionablePending(io.eaf.shared.ActorContext actor, UUID workspaceId,
            Instant cursorCreatedAt, UUID cursorId, int pageSize) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("员工收件箱只接受本人直接操作的 HUMAN 身份。");
        var access = workspaces.require(actor, workspaceId, "approval:read");
        workspaces.require(actor, workspaceId, "approval:decide");
        if (!identities.isActiveHuman(actor.tenantId(), actor.actorId())) throw EafException.notFound();
        if (pageSize < 1 || pageSize > 50 || (cursorCreatedAt == null) != (cursorId == null))
            throw EafException.invalid("可处理审批分页参数无效。");
        var args = new ArrayList<Object>(List.of(access.tenantId(), workspaceId));
        var where = new StringBuilder(" where tenant_id = ? and workspace_id = ? and state = 'PENDING' "
                + "and expires_at > ? and binding_json->>'requesterId' <> ?");
        args.add(Timestamp.from(Instant.now(clock)));
        args.add(actor.actorId().toString());
        if (cursorCreatedAt != null) {
            where.append(" and (created_at, id) < (?, ?)");
            args.add(Timestamp.from(cursorCreatedAt));
            args.add(cursorId);
        }
        args.add(pageSize + 1);
        var selected = jdbc.query("select id, task_id, expires_at, row_version, created_at from approval.request"
                        + where + " order by created_at desc, id desc limit ?",
                (rs, row) -> new ApprovalActionablePage.Item(rs.getObject("id", UUID.class),
                        rs.getObject("task_id", UUID.class), rs.getTimestamp("expires_at").toInstant(),
                        rs.getLong("row_version"), rs.getTimestamp("created_at").toInstant()), args.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        return new ApprovalActionablePage(items, last == null ? null : last.createdAt(), last == null ? null : last.id());
    }

    @Override
    public ApprovalOutboxPage listOutboxOperations(io.eaf.shared.ActorContext actor, UUID workspaceId,
                                                   Set<String> statuses, Instant createdAfter,
                                                   Instant cursorCreatedAt, UUID cursorEventId, int pageSize) {
        var access = workspaces.require(actor, workspaceId, "approval:read");
        if (pageSize < 1 || pageSize > 100 || (cursorCreatedAt == null) != (cursorEventId == null)
                || statuses != null && !OUTBOX_STATUSES.containsAll(statuses))
            throw EafException.invalid("Approval Outbox 运维分页或状态过滤无效。");
        var where = new StringBuilder(" where tenant_id = ? and workspace_id = ?");
        var filters = new ArrayList<Object>();
        filters.add(access.tenantId());
        filters.add(workspaceId);
        if (statuses != null) appendOutboxStatus(where, filters, statuses);
        if (createdAfter != null) {
            where.append(" and created_at >= ?");
            filters.add(Timestamp.from(createdAfter));
        }
        var totalSize = jdbc.queryForObject("select count(*) from approval.outbox" + where, Long.class, filters.toArray());
        var pageWhere = new StringBuilder(where);
        var pageArgs = new ArrayList<>(filters);
        if (cursorCreatedAt != null) {
            pageWhere.append(" and (created_at, event_id) < (?, ?)");
            pageArgs.add(Timestamp.from(cursorCreatedAt));
            pageArgs.add(cursorEventId);
        }
        pageArgs.add(pageSize + 1);
        var selected = jdbc.query("select event_id, approval_id, event_type, status, attempts, next_attempt_at, created_at "
                        + "from approval.outbox" + pageWhere + " order by created_at desc, event_id desc limit ?",
                (rs, row) -> new ApprovalOutboxItem(rs.getObject("event_id", UUID.class),
                        rs.getObject("approval_id", UUID.class), rs.getString("event_type"), rs.getString("status"),
                        rs.getInt("attempts"), rs.getTimestamp("next_attempt_at").toInstant(),
                        rs.getTimestamp("created_at").toInstant()), pageArgs.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        return new ApprovalOutboxPage(items, totalSize == null ? 0 : totalSize,
                last == null ? null : last.createdAt(), last == null ? null : last.eventId());
    }

    private void appendOutboxStatus(StringBuilder where, List<Object> filters, Set<String> statuses) {
        if (statuses.isEmpty()) where.append(" and 1 = 0");
        else {
            where.append(" and status in (").append(String.join(",", java.util.Collections.nCopies(statuses.size(), "?"))).append(')');
            statuses.forEach(filters::add);
        }
    }

    @Override
    @Transactional
    public ApprovalSnapshot decide(ApprovalDecisionCommand command) {
        if (command == null || command.actor() == null || command.decision() == null
                || !(command.decision().equals("APPROVED") || command.decision().equals("REJECTED")))
            throw EafException.invalid("审批决定只能是 APPROVED 或 REJECTED。");
        if (command.actor().type() != ActorType.HUMAN) throw EafException.forbidden("Agent 不能审批业务写入。");
        workspaces.require(command.actor(), command.workspaceId(), "approval:decide");
        var current = getVisible(command.actor().tenantId(), command.workspaceId(), command.approvalId());
        if (current == null) throw EafException.notFound();
        if (current.binding().requesterId().equals(command.actor().actorId())) throw EafException.forbidden("发起人不能审批自己的写请求。");
        if (!"PENDING".equals(current.state())) return current;
        if (current.binding().expiresAt().isBefore(Instant.now(clock))) {
            jdbc.update("update approval.request set state = 'EXPIRED', row_version = row_version + 1 where id = ? and state = 'PENDING'", command.approvalId());
            return getInternal(command.approvalId());
        }
        var changed = jdbc.update("update approval.request set state = ?, decided_by = ?, decided_at = ?, row_version = row_version + 1 where id = ? and row_version = ? and state = 'PENDING'",
                command.decision(), command.actor().actorId(), Timestamp.from(Instant.now(clock)), command.approvalId(), command.expectedVersion());
        if (changed == 0) throw EafException.conflict("VERSION_CONFLICT", "审批状态已被其他决定更新。");
        jdbc.update("insert into approval.outbox(event_id, tenant_id, workspace_id, approval_id, event_type, payload_json) select ?, tenant_id, workspace_id, id, 'eaf.approval.decided.v1', jsonb_build_object('approvalId', id, 'executionId', binding_json->>'executionId', 'decision', state, 'bindingHash', binding_hash) from approval.request where id = ?",
                UUID.randomUUID(), command.approvalId());
        return getInternal(command.approvalId());
    }

    @Override
    @Transactional
    public boolean cancelPending(UUID tenantId, UUID workspaceId, UUID taskId, UUID executionId) {
        // 仅按完整 Task/Execution 绑定取消仍待处理的请求，批准或拒绝后的终态不可被覆盖。
        var approvalId = jdbc.query("select id from approval.request where tenant_id = ? and workspace_id = ? and task_id = ? and binding_json->>'executionId' = ? and state = 'PENDING' order by created_at desc limit 1 for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null,
                tenantId, workspaceId, taskId, executionId.toString());
        if (approvalId == null) return false;
        var changed = jdbc.update("update approval.request set state = 'CANCELLED', decided_at = ?, row_version = row_version + 1 where id = ? and state = 'PENDING'",
                Timestamp.from(Instant.now(clock)), approvalId);
        if (changed == 1) {
            var eventId = UUID.nameUUIDFromBytes(("approval-cancelled:" + approvalId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            jdbc.update("insert into approval.outbox(event_id, tenant_id, workspace_id, approval_id, event_type, payload_json) values (?, ?, ?, ?, 'eaf.approval.cancelled.v1', jsonb_build_object('approvalId', ?, 'executionId', ?, 'decision', 'CANCELLED')) on conflict do nothing",
                    eventId, tenantId, workspaceId, approvalId, approvalId, executionId);
        }
        return changed == 1;
    }

    private ApprovalSnapshot getVisible(UUID tenantId, UUID workspaceId, UUID id) {
        return jdbc.query("select id, tenant_id, workspace_id, task_id, binding_json::text, state, decided_by, decided_at, row_version, created_at from approval.request where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? map(rs) : null, id, tenantId, workspaceId);
    }

    private ApprovalSnapshot getInternal(UUID id) {
        return jdbc.query("select id, tenant_id, workspace_id, task_id, binding_json::text, state, decided_by, decided_at, row_version, created_at from approval.request where id = ?",
                rs -> rs.next() ? map(rs) : null, id);
    }

    private ApprovalSnapshot map(java.sql.ResultSet rs) throws java.sql.SQLException {
        try {
            return new ApprovalSnapshot(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                    rs.getObject("task_id", UUID.class), json.readValue(rs.getString("binding_json"), ApprovalBinding.class), rs.getString("state"),
                    rs.getObject("decided_by", UUID.class), rs.getTimestamp("decided_at") == null ? null : rs.getTimestamp("decided_at").toInstant(),
                    rs.getLong("row_version"), rs.getTimestamp("created_at").toInstant());
        } catch (Exception e) {
            throw new java.sql.SQLException("审批绑定无法解析。", e);
        }
    }

    private String write(ApprovalBinding binding) {
        try { return json.writeValueAsString(binding); }
        catch (Exception e) { throw EafException.invalid("审批绑定无法序列化。"); }
    }
}
