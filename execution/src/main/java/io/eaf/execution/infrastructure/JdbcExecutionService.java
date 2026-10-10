package io.eaf.execution.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.eaf.approval.api.ApprovalBinding;
import io.eaf.approval.api.ApprovalCreateCommand;
import io.eaf.approval.api.ApprovalService;
import io.eaf.approval.api.ApprovalSnapshot;
import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.agent.api.RemoteAgentCatalog;
import io.eaf.agent.api.RemoteAgentRegistration;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.connector.api.ConnectorService;
import io.eaf.connector.api.ConnectorDefinition;
import io.eaf.connector.api.CustomerRecord;
import io.eaf.connector.api.ExternalWriteResult;
import io.eaf.connector.api.FollowupRecord;
import io.eaf.connector.api.FollowupOutcomeRecord;
import io.eaf.connector.api.ExternalOutcomeWriteResult;
import io.eaf.connector.api.P27BusinessConnectionService;
import io.eaf.connector.api.P27BusinessConnectionIntegrationPort;
import io.eaf.connector.api.RemoteA2aOutcome;
import io.eaf.connector.api.RemoteA2aResult;
import io.eaf.connector.api.ServiceRequestPayload;
import io.eaf.connector.api.ServiceRequestReceipt;
import io.eaf.connector.api.ServiceRequestWriteResult;
import io.eaf.execution.api.ExecutionCommand;
import io.eaf.execution.api.ExecutionService;
import io.eaf.execution.api.ExecutionSnapshot;
import io.eaf.execution.api.ExecutionVerificationReceipt;
import io.eaf.execution.api.ExecutionOperationsCursor;
import io.eaf.execution.api.ExecutionOperationsItem;
import io.eaf.execution.api.ExecutionOperationsPage;
import io.eaf.execution.api.ExecutionOutboxItem;
import io.eaf.execution.api.ExecutionOutboxPage;
import io.eaf.execution.api.ExecutionOutboxReplayReceipt;
import io.eaf.policy.api.PolicyDecision;
import io.eaf.policy.api.PolicyRequest;
import io.eaf.policy.api.PolicyService;
import io.eaf.policy.api.ServiceRequestPolicyRequest;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.task.api.TaskExecutionCheck;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.P27BusinessSourceVerifier;
import io.eaf.task.api.P27BusinessTaskSource;
import io.eaf.task.api.CustomerFollowupService;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.tool.api.ToolDefinition;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceOperationalControl;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.Optional;
import java.util.List;
import java.util.Set;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Execution 是唯一允许触达业务 Connector 的持久闸门：审批只改变授权事实，Connector 只在这里被调用。
 */
@Service
public class JdbcExecutionService implements ExecutionService {
    private static final Set<String> OPERATION_STATUSES = Set.of("RECEIVED", "VALIDATING", "READY", "AWAITING_APPROVAL",
            "EXECUTING", "VERIFYING", "AWAITING_REMOTE", "SUCCEEDED", "DENIED", "FAILED", "UNKNOWN",
            "VERIFICATION_FAILED", "CANCELLED");
    private static final Set<String> OUTBOX_STATUSES = Set.of("PENDING", "SENT", "FAILED");
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final ToolCatalog tools;
    private final RemoteAgentCatalog remoteAgents;
    private final CapabilityService capabilities;
    private final PolicyService policy;
    private final ApprovalService approvals;
    private final ConnectorService connectors;
    private final TaskService tasks;
    private final KnowledgeService knowledge;
    private final CustomerFollowupService customerFollowups;
    private final P27BusinessConnectionService p27Connections;
    private final P27BusinessSourceVerifier p27Sources;
    private final WorkspaceAuthorization workspaces;
    private final WorkspaceOperationalControl operationalControl;
    private final AuditPort audit;
    private final Clock clock;
    private final TransactionTemplate transactions;
    private final TransactionTemplate remoteTransactions;
    private final TransactionTemplate outsideTransaction;

    public JdbcExecutionService(JdbcTemplate jdbc, ObjectMapper json, ToolCatalog tools, PolicyService policy,
                                ApprovalService approvals, ConnectorService connectors, TaskService tasks,
                                CustomerFollowupService customerFollowups, KnowledgeService knowledge,
                                WorkspaceAuthorization workspaces, WorkspaceOperationalControl operationalControl,
                                AuditPort audit, Clock clock,
                                RemoteAgentCatalog remoteAgents, CapabilityService capabilities,
                                PlatformTransactionManager transactionManager,
                                P27BusinessConnectionService p27Connections,
                                @Lazy P27BusinessSourceVerifier p27Sources) {
        this.jdbc = jdbc; this.json = json; this.tools = tools; this.policy = policy; this.approvals = approvals;
        this.connectors = connectors; this.tasks = tasks; this.customerFollowups = customerFollowups; this.knowledge = knowledge;
        this.workspaces = workspaces;
        this.operationalControl = operationalControl; this.audit = audit; this.clock = clock;
        this.transactions = new TransactionTemplate(transactionManager);
        this.remoteTransactions = new TransactionTemplate(transactionManager);
        this.remoteTransactions.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRES_NEW);
        this.outsideTransaction = new TransactionTemplate(transactionManager);
        this.outsideTransaction.setPropagationBehavior(TransactionTemplate.PROPAGATION_NOT_SUPPORTED);
        this.remoteAgents = remoteAgents; this.capabilities = capabilities;
        this.p27Connections = p27Connections; this.p27Sources = p27Sources;
    }

    @Override
    public ExecutionSnapshot submit(ExecutionCommand c) {
        validateCommand(c);
        ToolDefinition tool = tools.requirePublished(c.actor().tenantId(), c.workspaceId(), c.toolName(), c.toolVersion());
        if (isRemoteReview(tool)) return submitRemote(c, tool);
        return transactions.execute(transaction -> submitLocal(c, tool));
    }

    private ExecutionSnapshot submitLocal(ExecutionCommand c, ToolDefinition tool) {
        NormalizedArguments args = normalizeForTask(c, tool);
        TaskExecutionCheck check = tasks.checkExecution(c.actor(), c.workspaceId(), c.taskId(), c.attempt());
        PolicyDecision decision = args.serviceRequest() != null
                ? evaluateServiceRequest(c.actor(), c.workspaceId(), c.agentId(), c.agentVersion(), tool, check.source())
                : evaluate(c, tool, args.customerId(), check.source());
        if (!decision.allowed()) return persistDenied(c, decision.policyVersion(), "POLICY_DENIED", decision.reason(), args);
        var existing = existing(c);
        if (existing != null) {
            // JSONB 可能重排字段；幂等比较按 JSON 结构而不是原始文本，避免恢复运行误报参数冲突。
            if (!sameJson(args.json(), existing.argumentsJson())) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一 Execution 幂等键对应了不同参数。");
            return existing;
        }
        if (!check.allowed()) return persistDenied(c, decision.policyVersion(), check.code(), check.detail(), args);
        // 新的 CRM 读取/写入意图受 Workspace 出站闸门约束；原幂等请求已在上方安全返回。
        if (!operationalControl.businessOutboundOpen(c.actor().tenantId(), c.workspaceId()))
            return persistDenied(c, decision.policyVersion(), "WORKSPACE_BUSINESS_OUTBOUND_STOPPED",
                    "Workspace 已停止新的业务出站动作。", args);
        if ("WRITE".equals(tool.effect())) return submitWrite(c, tool, args, decision);
        return submitRead(c, tool, args, decision);
    }

    private ExecutionSnapshot submitRemote(ExecutionCommand c, ToolDefinition tool) {
        // 固定只读 reviewer 的写入意图先独立提交，并与 Task 取消栅栏同事务；HTTP 永远发生在事务外。
        var reviewKind = remoteReviewKind(tool);
        if (reviewKind == null) throw EafException.forbidden("远端 reviewer Tool 不在固定 allowlist 中。");
        var args = normalizeRemote(c.argumentsJson());
        var check = tasks.checkExecution(c.actor(), c.workspaceId(), c.taskId(), c.attempt());
        if (!check.allowed()) return transactions.execute(tx -> persistDenied(c, "P6", check.code(), check.detail()));
        if ("USER".equals(reviewKind) && !"USER".equals(check.source()))
            return transactions.execute(tx -> persistDenied(c, "P6", "REMOTE_REVIEW_SOURCE_DENIED", "评测和非用户 Task 不能调用远端 reviewer。"));
        if ("EVALUATION".equals(reviewKind)) {
            if (!"EVALUATION".equals(check.source()))
                return transactions.execute(tx -> persistDenied(c, "P7", "EVALUATION_REVIEW_SOURCE_DENIED", "隔离 reviewer 仅接受 EVALUATION Task。"));
            try {
                // Task 域复核数据库中固定的协作清单 Workflow peer-review 步骤，不能由模型或调用方声明来源。
                tasks.requireEvaluationReviewerTask(c.actor(), c.workspaceId(), c.taskId());
            } catch (EafException denied) {
                return transactions.execute(tx -> persistDenied(c, "P7", "EVALUATION_REVIEW_SOURCE_DENIED", "隔离 reviewer 只能由活动协作清单的固定步骤调用。"));
            }
        }

        var evidence = tasks.evidence(c.actor().tenantId(), c.workspaceId(), c.taskId());
        if (!"TOOL_EXECUTION".equals(evidence.snapshot().runKind()))
            return transactions.execute(tx -> persistDenied(c, "P6", "REMOTE_REVIEW_FIXED_TOOL_REQUIRED", "远端 reviewer 只能由固定只读 Tool Task 调用。"));
        var decision = evaluate(c, tool, args.customerId(), check.source());
        if (!decision.allowed()) return transactions.execute(tx -> persistDenied(c, decision.policyVersion(), "POLICY_DENIED", decision.reason()));
        var binding = requireRemoteBinding(c.actor(), c.workspaceId(), evidence.snapshot().agentId(), evidence.snapshot().agentVersion(), reviewKind);
        var requestHash = remoteRequestHash(c, args, evidence.snapshot().rootTaskId(), binding);
        var prior = existing(c);
        if (prior != null) return existingRemote(args, requestHash, prior);

        RemoteIntent intent;
        try {
            intent = remoteTransactions.execute(tx -> {
                var concurrent = existing(c);
                if (concurrent != null) return new RemoteIntent(existingRemote(args, requestHash, concurrent), false);
                // A2A SendMessage 是新的外部动作；持久意图与闸门读取同事务排序。
                if (!operationalControl.businessOutboundOpen(c.actor().tenantId(), c.workspaceId()))
                    return new RemoteIntent(persistDenied(c, decision.policyVersion(),
                            "WORKSPACE_BUSINESS_OUTBOUND_STOPPED", "Workspace 已停止新的业务出站动作。"), false);
                var now = Instant.now(clock);
                var id = UUID.randomUUID();
                var messageId = id.toString();
                var operationKey = Hashing.sha256(String.join("\u001f", c.actor().tenantId().toString(), c.workspaceId().toString(),
                        c.actor().actorId().toString(), c.taskId().toString(), Integer.toString(c.attempt()), c.idempotencyKey()));
                var taskDeadline = tasks.deadlineAt(c.actor().tenantId(), c.workspaceId(), c.taskId());
                var pollDeadline = min(taskDeadline, now.plus(Duration.ofMinutes(15)));
                var traceId = c.traceId() == null ? evidence.snapshot().traceId() : c.traceId();
                jdbc.update("insert into execution.execution(id, tenant_id, workspace_id, actor_id, task_id, attempt, agent_id, agent_version, tool_name, tool_version, arguments_json, request_hash, idempotency_key, status, policy_version, operation_id, connector_id, connector_version, row_version, created_at, started_at, lease_until) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, 'EXECUTING', ?, ?, ?, ?, 1, ?, ?, ?)",
                        id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.taskId(), c.attempt(), evidence.snapshot().agentId(), evidence.snapshot().agentVersion(),
                        tool.name(), tool.version(), args.json(), requestHash, c.idempotencyKey(), decision.policyVersion(), id,
                        binding.connector().id(), binding.connector().provider(), Timestamp.from(now), Timestamp.from(now), Timestamp.from(now.plusSeconds(30)));
                jdbc.update("insert into execution.remote_a2a_operation(execution_id, tenant_id, workspace_id, task_id, attempt, actor_id, principal_id, delegation_id, authorization_hash, root_task_id, operation_key, message_id, request_hash, registration_id, capability_id, capability_version, connector_id, peer_skill_id, state, poll_limit, poll_deadline_at, remote_cost_status, trace_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'SEND_PENDING', 20, ?, 'UNKNOWN', ?)",
                        id, c.actor().tenantId(), c.workspaceId(), c.taskId(), c.attempt(), c.actor().actorId(), c.actor().principalIdOrActorId(),
                        c.actor().delegationId(), c.actor().authorizationHash(), evidence.snapshot().rootTaskId(), operationKey, messageId,
                        requestHash, binding.registration().id(), binding.capability().id(), binding.capability().version(), binding.connector().id(),
                        binding.registration().peerSkillId(), Timestamp.from(pollDeadline), traceId == null ? "trace-unavailable" : traceId);
                tasks.markExternalEffect(c.taskId(), c.attempt(), id, "IN_PROGRESS");
                audit.append(new AuditFact("remote-a2a-intent:" + id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(),
                        c.taskId(), "REMOTE_AGENT_SEND_INTENT", "SEND_PENDING", remoteEvidence(id, messageId, null, null, 0, null), traceId));
                return new RemoteIntent(getInternal(id), true);
            });
        } catch (DuplicateKeyException duplicate) {
            var winner = existing(c);
            if (winner == null) throw duplicate;
            return existingRemote(args, requestHash, winner);
        }
        if (!intent.created()) return intent.execution();

        // SendMessage 结果丢失时只能保留 UNKNOWN；messageId 绑定在已提交的 operationId 上，不能生成新请求重试。
        var body = json.createObjectNode();
        body.put("customerId", args.customerId());
        body.put("riskSummary", args.riskSummary());
        RemoteA2aResult response;
        try {
            response = outsideTransaction.execute(tx -> connectors.sendA2aTask(c.actor().tenantId(), c.workspaceId(),
                    binding.connector().id(), intent.execution().operationId().toString(), binding.registration().peerSkillId(),
                    intent.execution().operationId().toString(), write(body), min(Instant.now(clock).plusSeconds(5),
                            taskDeadline(c.actor().tenantId(), c.workspaceId(), c.taskId()))));
        } catch (RuntimeException failure) {
            response = new RemoteA2aResult(RemoteA2aOutcome.UNKNOWN, null, null, null, null, "A2A_SEND_UNKNOWN");
        }
        var result = response == null
                ? new RemoteA2aResult(RemoteA2aOutcome.UNKNOWN, null, null, null, null, "A2A_SEND_UNKNOWN") : response;
        return remoteTransactions.execute(tx -> completeRemoteResponse(getInternal(intent.execution().id()), result,
                c.actor(), true));
    }

    private ExecutionSnapshot existingRemote(RemoteArguments args, String requestHash, ExecutionSnapshot existing) {
        if (!sameJson(args.json(), existing.argumentsJson()))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一远端 Execution 幂等键对应了不同参数。");
        if ("DENIED".equals(existing.status())) return existing;
        var storedHash = jdbc.query("select request_hash from execution.execution where id = ?",
                rs -> rs.next() ? rs.getString(1) : null, existing.id());
        if (!requestHash.equals(storedHash))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "固定 reviewer、权限或 Connector 绑定已变化。");
        return existing;
    }

    @Override
    public ExecutionSnapshot pollRemote(ActorContext actor, UUID workspaceId, UUID executionId) {
        // 唤醒后的 Runtime 必须再次持有 Task 租约；这里还会复查授权和根预算，再沿已保存 remoteTaskId 查询。
        var current = getInternal(executionId);
        if (current == null || !actor.tenantId().equals(current.tenantId()) || !workspaceId.equals(current.workspaceId()))
            throw EafException.notFound();
        if (!"AWAITING_REMOTE".equals(current.status())) return current;
        var access = tasks.checkExecution(actor, workspaceId, current.taskId(), current.attempt());
        if (!access.allowed() || !"USER".equals(access.source()))
            throw EafException.forbidden("Task 当前身份无权轮询固定远端 reviewer。");
        var op = getRemoteOperation(executionId);
        if (op == null || !List.of("ACCEPTED", "WORKING").contains(op.state())) return finishRemoteUnknown(current, op, "REMOTE_OPERATION_UNAVAILABLE");

        var claim = remoteTransactions.execute(tx -> {
            var latest = getInternal(executionId);
            var locked = getRemoteOperation(executionId, true);
            if (latest == null || locked == null || !"AWAITING_REMOTE".equals(latest.status())
                    || !List.of("ACCEPTED", "WORKING").contains(locked.state())) return new RemotePollClaim(latest, locked, 0, false);
            var authority = remoteAuthority(actor, latest, locked);
            if (authority != null) return new RemotePollClaim(finishRemoteUnknown(latest, locked, authority), locked, 0, false);
            if (locked.pollsUsed() >= locked.pollLimit() || !Instant.now(clock).isBefore(locked.pollDeadline()))
                return new RemotePollClaim(finishRemoteUnknown(latest, locked, "REMOTE_POLL_LIMIT_EXCEEDED"), locked, 0, false);
            int nextPoll = locked.pollsUsed() + 1;
            var reservation = tasks.reserveRemotePoll(latest.taskId(), latest.attempt(), latest.operationId(),
                    "remote-poll:" + latest.id() + ":" + nextPoll);
            if (!reservation.allowed()) return new RemotePollClaim(
                    finishRemoteUnknown(latest, locked, reservation.code()), locked, 0, false);
            var changed = jdbc.update("update execution.remote_a2a_operation set polls_used = ?, next_poll_at = now() + interval '30 seconds', row_version = row_version + 1, updated_at = now() where execution_id = ? and row_version = ? and state in ('ACCEPTED','WORKING')",
                    nextPoll, latest.id(), locked.rowVersion());
            if (changed != 1) return new RemotePollClaim(getInternal(executionId), getRemoteOperation(executionId), 0, false);
            var claimed = getRemoteOperation(executionId);
            audit.append(new AuditFact("remote-a2a-poll-intent:" + latest.id() + ":" + nextPoll, actor.tenantId(), workspaceId,
                    actor.actorId(), latest.taskId(), "REMOTE_AGENT_POLL_INTENT", "GET_TASK", remoteEvidence(latest.id(), claimed.messageId(),
                    claimed.remoteTaskId(), claimed.remoteContextId(), nextPoll, null), claimed.traceId()));
            return new RemotePollClaim(latest, claimed, nextPoll, true);
        });
        if (claim == null || !claim.shouldCall()) return claim == null ? getInternal(executionId) : claim.execution();

        RemoteA2aResult response;
        try {
            response = outsideTransaction.execute(tx -> connectors.getA2aTask(actor.tenantId(), workspaceId, claim.operation().connectorId(),
                    claim.operation().messageId() + ":poll:" + claim.pollNumber(), claim.operation().remoteTaskId(),
                    Instant.now(clock).plusSeconds(5)));
        } catch (RuntimeException failure) {
            response = new RemoteA2aResult(RemoteA2aOutcome.UNKNOWN, null, null, null, null, "A2A_POLL_UNAVAILABLE");
        }
        var result = response == null
                ? new RemoteA2aResult(RemoteA2aOutcome.UNKNOWN, null, null, null, null, "A2A_POLL_UNAVAILABLE") : response;
        return remoteTransactions.execute(tx -> completeRemoteResponse(getInternal(executionId), result, actor, false));
    }

    @Override
    public ExecutionSnapshot cancelRemote(ActorContext actor, UUID workspaceId, UUID executionId) {
        // Task 域先记录取消请求；Execution 只向已保存的 remoteTaskId 发送固定 CancelTask。
        var current = getInternal(executionId);
        if (current == null || !actor.tenantId().equals(current.tenantId()) || !workspaceId.equals(current.workspaceId()))
            throw EafException.notFound();
        var task = tasks.get(actor, workspaceId, current.taskId());
        var authorized = tasks.cancel(actor, workspaceId, current.taskId(), task.version());
        if (authorized.status() != io.eaf.task.api.TaskStatus.CANCELLING_REMOTE)
            return current;

        var op = getRemoteOperation(executionId);
        if (op == null || op.remoteTaskId() == null || !op.actorId().equals(actor.actorId()))
            return current;
        var claim = remoteTransactions.execute(tx -> {
            var latest = getInternal(executionId);
            var locked = getRemoteOperation(executionId, true);
            if (latest == null || locked == null || !"AWAITING_REMOTE".equals(latest.status()))
                return new RemoteCancelClaim(latest, locked, false);
            if ("CANCELLED".equals(locked.state()))
                return new RemoteCancelClaim(latest, locked, false);
            var retryExpiredCancel = "CANCEL_PENDING".equals(locked.state())
                    && locked.nextPollAt() != null && !Instant.now(clock).isBefore(locked.nextPollAt());
            if (!List.of("ACCEPTED", "WORKING", "CANCEL_UNKNOWN").contains(locked.state()) && !retryExpiredCancel)
                return new RemoteCancelClaim(latest, locked, false);
            var changed = jdbc.update("update execution.remote_a2a_operation set state = 'CANCEL_PENDING', next_poll_at = now() + interval '30 seconds', row_version = row_version + 1, updated_at = now() where execution_id = ? and row_version = ? and state = ?",
                    latest.id(), locked.rowVersion(), locked.state());
            if (changed != 1) return new RemoteCancelClaim(getInternal(executionId), getRemoteOperation(executionId), false);
            var claimed = getRemoteOperation(executionId);
            audit.append(new AuditFact("remote-a2a-cancel-intent:" + latest.id() + ":" + claimed.rowVersion(),
                    actor.tenantId(), workspaceId, actor.actorId(), latest.taskId(), "REMOTE_AGENT_CANCEL_INTENT",
                    "CANCEL_PENDING", remoteEvidence(latest.id(), claimed.messageId(), claimed.remoteTaskId(),
                    claimed.remoteContextId(), claimed.pollsUsed(), null), claimed.traceId()));
            return new RemoteCancelClaim(latest, claimed, true);
        });
        if (claim == null || !claim.shouldCall()) return claim == null ? getInternal(executionId) : claim.execution();

        RemoteA2aResult response;
        try {
            response = outsideTransaction.execute(tx -> connectors.cancelA2aTask(actor.tenantId(), workspaceId,
                    claim.operation().connectorId(), claim.operation().messageId() + ":cancel",
                    claim.operation().remoteTaskId(), Instant.now(clock).plusSeconds(5)));
        } catch (RuntimeException failure) {
            response = new RemoteA2aResult(RemoteA2aOutcome.UNKNOWN, null, null, null, null, "A2A_CANCEL_UNAVAILABLE");
        }
        var result = response == null
                ? new RemoteA2aResult(RemoteA2aOutcome.UNKNOWN, null, null, null, null, "A2A_CANCEL_UNAVAILABLE") : response;
        return remoteTransactions.execute(tx -> completeRemoteCancellation(getInternal(executionId), result));
    }

    @Override
    public void wakeDueRemoteTasks() {
        // 只做持久到期扫描与 Task 入队；并发调度器通过 next_poll_at 短租约避免重复唤醒。
        var due = jdbc.query("select o.execution_id, e.tenant_id, e.workspace_id, e.task_id, e.attempt, e.operation_id, o.polls_used, o.poll_limit, o.poll_deadline_at "
                        + "from execution.remote_a2a_operation o join execution.execution e on e.id = o.execution_id "
                        + "where e.status = 'AWAITING_REMOTE' and o.state in ('ACCEPTED','WORKING') and o.next_poll_at <= now() "
                        + "order by o.next_poll_at limit 50",
                (rs, row) -> new DueRemoteTask(rs.getObject("execution_id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("task_id", UUID.class), rs.getInt("attempt"),
                        rs.getObject("operation_id", UUID.class), rs.getInt("polls_used"), rs.getInt("poll_limit"),
                        rs.getTimestamp("poll_deadline_at").toInstant()));
        for (var item : due) {
            var claimed = jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() + interval '30 seconds', row_version = row_version + 1, updated_at = now() where execution_id = ? and state in ('ACCEPTED','WORKING') and next_poll_at <= now()",
                    item.executionId());
            if (claimed == 0) continue;
            if (item.pollsUsed() >= item.pollLimit() || !Instant.now(clock).isBefore(item.pollDeadline())) {
                // 上限到达后仍唤醒原 Task 一次，让 Runtime 将 UNKNOWN 投影为 WAITING_VERIFICATION。
                var wake = tasks.wakeRemote(item.tenantId(), item.workspaceId(), item.taskId(), item.attempt(), item.operationId());
                if (wake == io.eaf.task.api.RemoteTaskWakeStatus.CAPACITY_DEFERRED) {
                    deferRemoteWake(item.executionId());
                    continue;
                }
                remoteTransactions.execute(tx -> finishRemoteUnknown(getInternal(item.executionId()), getRemoteOperation(item.executionId()),
                        "REMOTE_POLL_LIMIT_EXCEEDED"));
                continue;
            }
            var wake = tasks.wakeRemote(item.tenantId(), item.workspaceId(), item.taskId(), item.attempt(), item.operationId());
            if (wake == io.eaf.task.api.RemoteTaskWakeStatus.CAPACITY_DEFERRED) {
                deferRemoteWake(item.executionId());
                continue;
            }
            if (wake == io.eaf.task.api.RemoteTaskWakeStatus.EXPIRED) {
                remoteTransactions.execute(tx -> finishRemoteUnknown(getInternal(item.executionId()), getRemoteOperation(item.executionId()),
                        "REMOTE_TASK_DEADLINE_EXCEEDED"));
                continue;
            }
            // Task 已终态时不再轮询或推进；远端结果无法改变本地已提交的终态事实。
            if (wake == io.eaf.task.api.RemoteTaskWakeStatus.TERMINAL) {
                remoteTransactions.execute(tx -> finishRemoteUnknown(getInternal(item.executionId()), getRemoteOperation(item.executionId()),
                        "REMOTE_TASK_TERMINAL"));
                continue;
            }
            if (wake == io.eaf.task.api.RemoteTaskWakeStatus.NOT_WAITING)
                jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() + interval '5 seconds', updated_at = now() where execution_id = ? and state in ('ACCEPTED','WORKING')",
                        item.executionId());
        }
    }

    private void deferRemoteWake(UUID executionId) {
        jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() + interval '1 second', updated_at = now() where execution_id = ? and state in ('ACCEPTED','WORKING')",
                executionId);
    }

    private ExecutionSnapshot completeRemoteResponse(ExecutionSnapshot current, RemoteA2aResult response,
                                                      ActorContext actor, boolean send) {
        // 外部响应回到数据库后先校验原 Task、委托、Policy、Capability、Connector 与远端 Task 关联。
        if (current == null) throw EafException.notFound();
        var operation = getRemoteOperation(current.id(), true);
        if (operation == null) return finishRemoteUnknown(current, null, "REMOTE_OPERATION_UNAVAILABLE");
        if (List.of("CANCEL_PENDING", "CANCEL_UNKNOWN", "CANCELLED").contains(operation.state())) {
            var lateHash = response == null ? Hashing.sha256("missing") : Hashing.sha256(String.valueOf(response.taskJson()));
            audit.append(new AuditFact("remote-a2a-late-result-ignored:" + current.id() + ":" + operation.pollsUsed(),
                    current.tenantId(), current.workspaceId(), operation.actorId(), current.taskId(),
                    "REMOTE_AGENT_LATE_RESULT_IGNORED", operation.state(), remoteEvidence(current.id(), operation.messageId(),
                    operation.remoteTaskId(), operation.remoteContextId(), operation.pollsUsed(), lateHash), operation.traceId()));
            return getInternal(current.id());
        }
        if (!List.of("ACCEPTED", "WORKING", "SEND_PENDING").contains(operation.state())) return getInternal(current.id());
        if (response == null) response = new RemoteA2aResult(RemoteA2aOutcome.UNKNOWN, null, null, null, null, "A2A_RESPONSE_UNKNOWN");
        if (send && response.outcome() == RemoteA2aOutcome.REJECTED)
            return finishRemoteFailure(current, operation, response.errorCode(), "远端 reviewer 明确拒绝了请求。", "FAILED");
        if (response.outcome() == RemoteA2aOutcome.UNKNOWN) {
            if (send) return finishRemoteUnknown(current, operation,
                    response.errorCode() == null ? "A2A_SEND_UNKNOWN" : response.errorCode(), null, null, "SEND_UNKNOWN");
            if (operation.pollsUsed() >= operation.pollLimit() || !Instant.now(clock).isBefore(operation.pollDeadline()))
                return finishRemoteUnknown(current, operation, "REMOTE_POLL_LIMIT_EXCEEDED");
            updateRemoteWait(current, operation, "WORKING", operation.remoteTaskId(), operation.remoteContextId(),
                    "REMOTE_POLL_UNAVAILABLE", false);
            return getInternal(current.id());
        }
        if (response.outcome() == RemoteA2aOutcome.REJECTED)
            return finishRemoteUnknown(current, operation, response.errorCode() == null ? "A2A_GET_REJECTED" : response.errorCode());

        RemoteTaskPayload task;
        try {
            task = parseRemoteTask(response.taskJson());
            if (!send && (!operation.remoteTaskId().equals(task.taskId()) || !operation.remoteContextId().equals(task.contextId())))
                return finishRemoteUnknown(current, operation, "REMOTE_TASK_ASSOCIATION_MISMATCH");
        } catch (RuntimeException malformed) {
            return send ? finishRemoteUnknown(current, operation, "A2A_SEND_RESPONSE_INVALID", null, null, "SEND_UNKNOWN")
                    : finishRemoteFailure(current, operation, "REMOTE_RESULT_INVALID", "远端 Task 关联或格式无效，结果已拒绝。", "FAILED");
        }

        var authority = remoteAuthority(actor, current, operation);
        if (authority != null) return finishRemoteUnknown(current, operation, authority, task.taskId(), task.contextId());
        if ("TASK_STATE_SUBMITTED".equals(task.state()) || "TASK_STATE_WORKING".equals(task.state())) {
            if (send) {
                var next = "TASK_STATE_SUBMITTED".equals(task.state()) ? "ACCEPTED" : "WORKING";
                return updateRemoteWait(current, operation, next, task.taskId(), task.contextId(), null, true);
            }
            var next = "TASK_STATE_SUBMITTED".equals(task.state()) ? "ACCEPTED" : "WORKING";
            return updateRemoteWait(current, operation, next, operation.remoteTaskId(), operation.remoteContextId(), null, false);
        }
        if ("TASK_STATE_FAILED".equals(task.state()) || "TASK_STATE_CANCELED".equals(task.state()))
            return finishRemoteFailure(current, operation, "REMOTE_TASK_FAILED", "远端 reviewer Task 未能完成。", "FAILED", task.taskId(), task.contextId());
        if (!"TASK_STATE_COMPLETED".equals(task.state()))
            return finishRemoteFailure(current, operation, "REMOTE_TASK_STATE_UNSUPPORTED", "远端 reviewer 返回了不支持的状态。", "FAILED", task.taskId(), task.contextId());

        try {
            var resultJson = validateRemoteAdvice(json.readTree(task.json()), operation.peerSkillId());
            var done = finish(current, current.status(), "SUCCEEDED", resultJson, null, null, current.policyVersion(),
                    remoteEvidence(current.id(), operation.messageId(), task.taskId(), task.contextId(), operation.pollsUsed(), Hashing.sha256(resultJson)));
            if (!"SUCCEEDED".equals(done.status())) return done;
            jdbc.update("update execution.remote_a2a_operation set state = 'COMPLETED', remote_task_id = ?, remote_context_id = ?, result_hash = ?, next_poll_at = null, row_version = row_version + 1, updated_at = now() where execution_id = ? and row_version = ?",
                    task.taskId(), task.contextId(), Hashing.sha256(resultJson), current.id(), operation.rowVersion());
            tasks.recordToolExecution(current.taskId(), current.attempt());
        audit.append(new AuditFact("remote-a2a-complete:" + current.id(), actor.tenantId(), current.workspaceId(), operation.actorId(),
                    current.taskId(), "REMOTE_AGENT_ADVICE_ACCEPTED", "SUCCEEDED", remoteEvidence(current.id(), operation.messageId(),
                    task.taskId(), task.contextId(), operation.pollsUsed(), Hashing.sha256(resultJson)), operation.traceId()));
            return getInternal(current.id());
        } catch (RuntimeException | java.io.IOException invalid) {
            return finishRemoteFailure(current, operation, "REMOTE_RESULT_INVALID", "远端建议不符合固定只读 Schema，结果已拒绝。", "FAILED", task.taskId(), task.contextId());
        }
    }

    private boolean isRemoteReview(ToolDefinition tool) {
        return remoteReviewKind(tool) != null;
    }

    private String remoteReviewKind(ToolDefinition tool) {
        if (tool == null || !"READ".equals(tool.effect())) return null;
        if ("agent.risk.review".equals(tool.name()) && "agent:risk-review".equals(tool.permissionAction())
                && "A2A_REVIEW_PEER:risk-review@1.0.0".equals(tool.bindingRef())) return "USER";
        if ("agent.risk.review.evaluation".equals(tool.name())
                && "agent:risk-review-evaluation".equals(tool.permissionAction())
                && "A2A_EVALUATION_REVIEW_PEER:risk-review-evaluation@1.0.0".equals(tool.bindingRef())) return "EVALUATION";
        return null;
    }

    private RemoteArguments normalizeRemote(String raw) {
        try {
            var node = json.readTree(raw);
            if (node == null || !node.isObject() || node.size() != 2 || !node.path("customerId").isTextual()
                    || !node.path("riskSummary").isTextual()) throw new IllegalArgumentException();
            var customerId = node.path("customerId").asText();
            var riskSummary = node.path("riskSummary").asText();
            if (customerId.isBlank() || customerId.length() > 160 || riskSummary.isBlank() || riskSummary.length() > 2_000)
                throw new IllegalArgumentException();
            var normalized = json.createObjectNode().put("customerId", customerId).put("riskSummary", riskSummary);
            return new RemoteArguments(customerId, riskSummary, write(normalized));
        } catch (Exception invalid) {
            throw EafException.invalid("远端风险复核参数必须仅包含有效 customerId 和 riskSummary。");
        }
    }

    private RemoteBinding requireRemoteBinding(ActorContext actor, UUID workspaceId, UUID agentId,
                                               String agentVersion, String reviewKind) {
        var evaluation = "EVALUATION".equals(reviewKind);
        var registrationKey = evaluation ? "risk-review-evaluation" : "risk-review";
        var peerSkill = evaluation ? "agent.risk.review.evaluation" : "agent.risk.review";
        var permission = evaluation ? "agent:risk-review-evaluation" : "agent:risk-review";
        var provider = evaluation ? "A2A_EVALUATION_REVIEW_PEER" : "A2A_REVIEW_PEER";
        // USER reviewer 的远端登记固定在 1.0.0；评测 reviewer 使用单独登记的 1.2.0。不要让新增评测版本改写旧授权。
        var capabilityVersion = evaluation ? "1.2.0" : "1.0.0";
        var resourceScope = evaluation ? "SYNTHETIC_EVALUATION_ONLY" : "OWNER_GRANTED_CUSTOMER";
        var registration = remoteAgents.requireActive(actor, workspaceId, registrationKey);
        if (!registration.tenantId().equals(actor.tenantId()) || !registration.workspaceId().equals(workspaceId)
                || !registration.ownerId().equals(actor.principalIdOrActorId())
                || !registrationKey.equals(registration.agentKey()) || !"1.0.0".equals(registration.agentVersion())
                || !peerSkill.equals(registration.peerSkillId())
                || !Set.copyOf(registration.allowedInputFields()).equals(Set.of("customerId", "riskSummary"))
                || !Set.copyOf(registration.allowedOutputFields()).equals(Set.of("riskLevel", "rationale", "citations"))
                || !Set.copyOf(registration.delegationActions()).equals(Set.of(permission))
                || !resourceScope.equals(registration.resourceScope())
                || !capabilityVersion.equals(registration.capabilityVersion()))
            throw EafException.forbidden("固定 reviewer 登记与调用身份或 Schema 不匹配。");
        if (actor.delegated() && (actor.type() != ActorType.AGENT || !actor.can(permission)))
            throw EafException.forbidden("当前委托未包含固定 reviewer 动作。");
        var capability = capabilities.requirePublished(actor, workspaceId, registration.capabilityId(), registration.capabilityVersion());
        if (!capability.id().equals(registration.capabilityId()) || !capability.version().equals(registration.capabilityVersion())
                || !capability.ownerId().equals(registration.ownerId()) || !capability.agentId().equals(agentId)
                || !capability.agentVersion().equals(agentVersion) || !"PUBLISHED".equals(capability.status()))
            throw EafException.forbidden("固定 reviewer 引用的 Capability 已撤回或版本不匹配。");
        var connector = connectors.requireActive(actor.tenantId(), workspaceId, provider);
        if (!connector.id().equals(registration.connectorId()) || !connector.allowedUses().contains("a2a.send")
                || !connector.allowedUses().contains("a2a.get") || !connector.allowedUses().contains("a2a.cancel")
                || !connector.permissions().equals(Set.of(permission)))
            throw EafException.forbidden("固定 reviewer Connector 未满足登记的最小权限。");
        return new RemoteBinding(registration, capability, connector);
    }

    private String remoteRequestHash(ExecutionCommand command, RemoteArguments args, UUID rootTaskId, RemoteBinding binding) {
        var actor = command.actor();
        return Hashing.sha256(String.join("\u001f", args.json(), actor.tenantId().toString(), command.workspaceId().toString(),
                actor.actorId().toString(), actor.principalIdOrActorId().toString(), String.valueOf(actor.delegationId()),
                String.valueOf(actor.authorizationHash()), command.taskId().toString(), String.valueOf(rootTaskId),
                String.valueOf(command.attempt()), String.valueOf(command.agentId()), String.valueOf(command.agentVersion()),
                command.toolName(), command.toolVersion(), binding.registration().id().toString(), binding.capability().id().toString(),
                binding.capability().version(), binding.capability().contentHash(), binding.connector().id().toString(),
                binding.connector().baseUrl(), binding.connector().credentialRef(), binding.connector().audience(),
                String.join(",", binding.connector().allowedUses().stream().sorted().toList()),
                String.join(",", binding.connector().permissions().stream().sorted().toList()), binding.registration().peerSkillId()));
    }

    private String remoteAuthority(ActorContext actor, ExecutionSnapshot execution, RemoteOperation operation) {
        try {
            var check = tasks.checkExecution(actor, execution.workspaceId(), execution.taskId(), execution.attempt());
            if (!check.allowed()) return check.code();
            var reviewKind = remoteReviewKind(tools.requirePublished(execution.tenantId(), execution.workspaceId(),
                    execution.toolName(), execution.toolVersion()));
            if ("USER".equals(reviewKind) && !"USER".equals(check.source())) return "REMOTE_REVIEW_SOURCE_DENIED";
            if ("EVALUATION".equals(reviewKind)) {
                if (!"EVALUATION".equals(check.source())) return "EVALUATION_REVIEW_SOURCE_DENIED";
                try {
                    // 每次发送、轮询和取消前都重新核验评测 Task 仍属于固定 reviewer 步骤。
                    tasks.requireEvaluationReviewerTask(actor, execution.workspaceId(), execution.taskId());
                } catch (EafException denied) {
                    return "EVALUATION_REVIEW_SOURCE_DENIED";
                }
            }
            if (reviewKind == null) return "REMOTE_TOOL_CHANGED";
            var evidence = tasks.evidence(execution.tenantId(), execution.workspaceId(), execution.taskId());
            if (!"TOOL_EXECUTION".equals(evidence.snapshot().runKind())) return "REMOTE_REVIEW_FIXED_TOOL_REQUIRED";
            var args = normalizeRemote(execution.argumentsJson());
            var tool = tools.requirePublished(execution.tenantId(), execution.workspaceId(), execution.toolName(), execution.toolVersion());
            if (!isRemoteReview(tool)) return "REMOTE_TOOL_CHANGED";
            var command = new ExecutionCommand(actor, execution.workspaceId(), execution.taskId(), execution.attempt(),
                    evidence.snapshot().agentId(), evidence.snapshot().agentVersion(), tool.name(), tool.version(), args.json(),
                    operation.operationKey(), operation.traceId());
            var decision = evaluate(command, tool, args.customerId(), check.source());
            if (!decision.allowed()) return "POLICY_DENIED";
            var binding = requireRemoteBinding(actor, execution.workspaceId(), evidence.snapshot().agentId(), evidence.snapshot().agentVersion(), reviewKind);
            if (!operation.actorId().equals(actor.actorId()) || !operation.principalId().equals(actor.principalIdOrActorId())
                    || !java.util.Objects.equals(operation.delegationId(), actor.delegationId())
                    || !java.util.Objects.equals(operation.authorizationHash(), actor.authorizationHash())
                    || !operation.registrationId().equals(binding.registration().id())
                    || !operation.capabilityId().equals(binding.capability().id())
                    || !operation.capabilityVersion().equals(binding.capability().version())
                    || !operation.connectorId().equals(binding.connector().id())
                    || !operation.rootTaskId().equals(evidence.snapshot().rootTaskId())
                    || !operation.requestHash().equals(remoteRequestHash(command, args, evidence.snapshot().rootTaskId(), binding)))
                return "REMOTE_AUTHORITY_CHANGED";
            return null;
        } catch (EafException denied) {
            return denied.code();
        }
    }

    private RemoteTaskPayload parseRemoteTask(String taskJson) {
        try {
            var task = json.readTree(taskJson);
            if (task == null || !task.isObject() || hasUnknownFields(task, Set.of("id", "contextId", "status", "artifacts", "history", "metadata")))
                throw new IllegalArgumentException();
            var taskId = boundedText(task.path("id"), 200);
            var contextId = boundedText(task.path("contextId"), 200);
            var state = boundedText(task.path("status").path("state"), 40);
            // 合法 A2A peer 可以回送消息历史；EAF 不消费它，只保留有界响应中的 Task 外壳。
            if (task.has("history") && !task.path("history").isArray()) throw new IllegalArgumentException();
            return new RemoteTaskPayload(taskId, contextId, state, write(task));
        } catch (Exception invalid) {
            throw new IllegalArgumentException("A2A Task 响应无效。", invalid);
        }
    }

    private String validateRemoteAdvice(JsonNode task, String peerSkillId) {
        // 远端只提供 advisory DataPart；严格字段白名单阻止 approval、ToolCall 或未知字段变成执行授权。
        var artifactName = switch (peerSkillId) {
            case "agent.risk.review" -> "risk-review";
            case "agent.risk.review.evaluation" -> "risk-review-evaluation";
            default -> throw new IllegalArgumentException("unknown reviewer skill");
        };
        if (!task.path("artifacts").isArray() || task.path("artifacts").size() != 1)
            throw new IllegalArgumentException("review artifact count");
        var artifact = task.path("artifacts").get(0);
        if (!artifact.isObject() || hasUnknownFields(artifact, Set.of("artifactId", "name", "description", "parts", "metadata"))
                || !artifactName.equals(artifact.path("name").asText()) || !artifact.path("parts").isArray()
                || artifact.path("parts").size() != 1) throw new IllegalArgumentException("review artifact");
        var part = artifact.path("parts").get(0);
        if (!part.isObject() || hasUnknownFields(part, Set.of("kind", "data")) || !"data".equals(part.path("kind").asText())
                || !part.path("data").isObject()) throw new IllegalArgumentException("structured data part");
        var advice = part.path("data");
        if (hasUnknownFields(advice, Set.of("riskLevel", "rationale", "citations")))
            throw new IllegalArgumentException("advice fields");
        var level = boundedText(advice.path("riskLevel"), 16);
        if (!Set.of("LOW", "MEDIUM", "HIGH", "UNKNOWN").contains(level)) throw new IllegalArgumentException("risk level");
        var rationale = boundedText(advice.path("rationale"), 1_000);
        if (!advice.path("citations").isArray() || advice.path("citations").size() > 8)
            throw new IllegalArgumentException("citations");
        var result = json.createObjectNode().put("riskLevel", level).put("rationale", rationale);
        var citations = result.putArray("citations");
        for (var citation : advice.path("citations")) citations.add(boundedText(citation, 160));
        return write(result);
    }

    private ExecutionSnapshot updateRemoteWait(ExecutionSnapshot current, RemoteOperation operation, String state,
                                               String remoteTaskId, String remoteContextId, String errorCode,
                                               boolean firstResponse) {
        var nextAt = Timestamp.from(Instant.now(clock).plusSeconds(2));
        var metadata = remoteEvidence(current.id(), operation.messageId(), remoteTaskId, remoteContextId,
                operation.pollsUsed(), null);
        var changed = jdbc.update("update execution.execution set status = 'AWAITING_REMOTE', result_json = null, verification_json = ?::jsonb, error_code = ?, error_detail = null, lease_until = null, ended_at = null, row_version = row_version + 1 where id = ? and row_version = ? and status = ?",
                metadata, errorCode, current.id(), current.version(), current.status());
        if (changed != 1) return getInternal(current.id());
        jdbc.update("update execution.remote_a2a_operation set state = ?, remote_task_id = ?, remote_context_id = ?, next_poll_at = ?, row_version = row_version + 1, updated_at = now() where execution_id = ? and row_version = ?",
                state, remoteTaskId, remoteContextId, nextAt, current.id(), operation.rowVersion());
        tasks.markExternalEffect(current.taskId(), current.attempt(), current.operationId(), "REMOTE_PENDING");
        var waiting = getInternal(current.id());
        recordOutbox(waiting);
        var factKey = firstResponse ? "remote-a2a-accepted:" + current.id()
                : "remote-a2a-poll-result:" + current.id() + ":" + operation.pollsUsed();
        var action = firstResponse ? "REMOTE_AGENT_TASK_ACCEPTED" : "REMOTE_AGENT_POLL_RECORDED";
        audit.append(new AuditFact(factKey, current.tenantId(), current.workspaceId(), operation.actorId(),
                current.taskId(), action, state, metadata, operation.traceId()));
        return waiting;
    }

    private ExecutionSnapshot completeRemoteCancellation(ExecutionSnapshot current, RemoteA2aResult response) {
        // 只有同一 remoteTaskId 的明确 CANCELED 回执可以结束本地 Task；其他响应保留未确认状态。
        if (current == null) return null;
        var operation = getRemoteOperation(current.id(), true);
        if (operation == null || !"CANCEL_PENDING".equals(operation.state())) return getInternal(current.id());
        var confirmed = false;
        if (response != null && response.outcome() == RemoteA2aOutcome.TASK) {
            try {
                var task = parseRemoteTask(response.taskJson());
                confirmed = operation.remoteTaskId().equals(task.taskId())
                        && operation.remoteContextId().equals(task.contextId())
                        && "TASK_STATE_CANCELED".equals(task.state());
            } catch (RuntimeException ignored) {
                confirmed = false;
            }
        }
        if (confirmed) {
            var metadata = remoteEvidence(current.id(), operation.messageId(), operation.remoteTaskId(),
                    operation.remoteContextId(), operation.pollsUsed(), null);
            var done = finish(current, "AWAITING_REMOTE", "CANCELLED", null, null, null,
                    current.policyVersion(), metadata);
            if (!"CANCELLED".equals(done.status())) return done;
            jdbc.update("update execution.remote_a2a_operation set state = 'CANCELLED', next_poll_at = null, row_version = row_version + 1, updated_at = now() where execution_id = ? and row_version = ? and state = 'CANCEL_PENDING'",
                    current.id(), operation.rowVersion());
            tasks.confirmRemoteCancellation(current.tenantId(), current.workspaceId(), current.taskId(),
                    current.attempt(), current.operationId());
            audit.append(new AuditFact("remote-a2a-cancel-confirmed:" + current.id(), current.tenantId(),
                    current.workspaceId(), operation.actorId(), current.taskId(), "REMOTE_AGENT_CANCEL_CONFIRMED",
                    "CANCELLED", metadata, operation.traceId()));
            return getInternal(current.id());
        }

        var changed = jdbc.update("update execution.remote_a2a_operation set state = 'CANCEL_UNKNOWN', next_poll_at = null, row_version = row_version + 1, updated_at = now() where execution_id = ? and row_version = ? and state = 'CANCEL_PENDING'",
                current.id(), operation.rowVersion());
        if (changed == 1) {
            tasks.markExternalEffect(current.taskId(), current.attempt(), current.operationId(), "REMOTE_CANCEL_UNKNOWN");
            audit.append(new AuditFact("remote-a2a-cancel-unknown:" + current.id() + ":" + operation.rowVersion(),
                    current.tenantId(), current.workspaceId(), operation.actorId(), current.taskId(),
                    "REMOTE_AGENT_CANCEL_UNCONFIRMED", response == null || response.errorCode() == null
                    ? "CANCEL_UNKNOWN" : response.errorCode(), remoteEvidence(current.id(), operation.messageId(),
                    operation.remoteTaskId(), operation.remoteContextId(), operation.pollsUsed(), null), operation.traceId()));
        }
        return getInternal(current.id());
    }

    private ExecutionSnapshot finishRemoteUnknown(ExecutionSnapshot current, RemoteOperation operation, String code) {
        return finishRemoteUnknown(current, operation, code, operation == null ? null : operation.remoteTaskId(),
                operation == null ? null : operation.remoteContextId());
    }

    private ExecutionSnapshot finishRemoteUnknown(ExecutionSnapshot current, RemoteOperation operation, String code,
                                                  String remoteTaskId, String remoteContextId) {
        return finishRemoteUnknown(current, operation, code, remoteTaskId, remoteContextId, "UNKNOWN");
    }

    private ExecutionSnapshot finishRemoteUnknown(ExecutionSnapshot current, RemoteOperation operation, String code,
                                                  String remoteTaskId, String remoteContextId, String operationState) {
        if (current == null) return null;
        var messageId = operation == null ? current.operationId().toString() : operation.messageId();
        var polls = operation == null ? 0 : operation.pollsUsed();
        var metadata = remoteEvidence(current.id(), messageId, remoteTaskId, remoteContextId, polls, null);
        jdbc.update("update execution.remote_a2a_operation set state = ?, remote_task_id = coalesce(?, remote_task_id), remote_context_id = coalesce(?, remote_context_id), next_poll_at = null, row_version = row_version + 1, updated_at = now() where execution_id = ? and state not in ('COMPLETED','FAILED','UNKNOWN')",
                operationState, remoteTaskId, remoteContextId, current.id());
        return finish(current, current.status(), "UNKNOWN", null, code, "远端结果或成本无法确认，已停止自动推进且不会重发请求。",
                current.policyVersion(), metadata);
    }

    private ExecutionSnapshot finishRemoteFailure(ExecutionSnapshot current, RemoteOperation operation, String code,
                                                  String detail, String operationState) {
        return finishRemoteFailure(current, operation, code, detail, operationState,
                operation == null ? null : operation.remoteTaskId(), operation == null ? null : operation.remoteContextId());
    }

    private ExecutionSnapshot finishRemoteFailure(ExecutionSnapshot current, RemoteOperation operation, String code,
                                                  String detail, String operationState, String remoteTaskId,
                                                  String remoteContextId) {
        var metadata = remoteEvidence(current.id(), operation.messageId(), remoteTaskId, remoteContextId,
                operation.pollsUsed(), null);
        var done = finish(current, current.status(), "FAILED", null, code, detail, current.policyVersion(), metadata);
        if ("FAILED".equals(done.status())) {
            jdbc.update("update execution.remote_a2a_operation set state = ?, remote_task_id = coalesce(?, remote_task_id), remote_context_id = coalesce(?, remote_context_id), next_poll_at = null, row_version = row_version + 1, updated_at = now() where execution_id = ? and row_version = ?",
                    operationState, remoteTaskId, remoteContextId, current.id(), operation.rowVersion());
        audit.append(new AuditFact("remote-a2a-failed:" + current.id(), current.tenantId(), current.workspaceId(), operation.actorId(),
                    current.taskId(), "REMOTE_AGENT_TASK_FAILED", code, metadata, operation.traceId()));
        }
        return getInternal(current.id());
    }

    private RemoteOperation getRemoteOperation(UUID executionId) { return getRemoteOperation(executionId, false); }

    private RemoteOperation getRemoteOperation(UUID executionId, boolean lock) {
        var sql = "select tenant_id, workspace_id, task_id, attempt, actor_id, principal_id, delegation_id, authorization_hash, root_task_id, operation_key, message_id, request_hash, registration_id, capability_id, capability_version, connector_id, peer_skill_id, state, remote_task_id, remote_context_id, polls_used, poll_limit, next_poll_at, poll_deadline_at, trace_id, row_version from execution.remote_a2a_operation where execution_id = ?" + (lock ? " for update" : "");
        return jdbc.query(sql, rs -> rs.next() ? new RemoteOperation(rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                rs.getObject("task_id", UUID.class), rs.getInt("attempt"), rs.getObject("actor_id", UUID.class), rs.getObject("principal_id", UUID.class),
                rs.getObject("delegation_id", UUID.class), rs.getString("authorization_hash"), rs.getObject("root_task_id", UUID.class),
                rs.getString("operation_key"), rs.getString("message_id"), rs.getString("request_hash"), rs.getObject("registration_id", UUID.class),
                rs.getObject("capability_id", UUID.class), rs.getString("capability_version"), rs.getObject("connector_id", UUID.class),
                rs.getString("peer_skill_id"), rs.getString("state"), rs.getString("remote_task_id"), rs.getString("remote_context_id"),
                rs.getInt("polls_used"), rs.getInt("poll_limit"), rs.getTimestamp("next_poll_at") == null ? null : rs.getTimestamp("next_poll_at").toInstant(),
                rs.getTimestamp("poll_deadline_at").toInstant(), rs.getString("trace_id"), rs.getLong("row_version")) : null, executionId);
    }

    private String remoteEvidence(UUID executionId, String messageId, String remoteTaskId, String remoteContextId,
                                  int pollsUsed, String resultHash) {
        var execution = getInternal(executionId);
        var operation = getRemoteOperation(executionId);
        var task = tasks.evidence(execution.tenantId(), execution.workspaceId(), execution.taskId()).snapshot();
        // 审计保存关联标识与摘要，不复制工具参数、peer 正文或凭证。
        var node = json.createObjectNode().put("executionId", executionId.toString())
                .put("operationId", execution.operationId().toString()).put("taskId", task.id().toString())
                .put("rootTaskId", task.rootTaskId().toString()).put("attempt", execution.attempt())
                .put("entryProtocol", task.entryProtocol()).put("actorId", operation.actorId().toString())
                .put("principalId", operation.principalId().toString())
                .put("delegateId", operation.actorId().equals(operation.principalId()) ? null : operation.actorId().toString())
                .put("delegationId", operation.delegationId() == null ? null : operation.delegationId().toString())
                .put("traceId", operation.traceId()).put("messageId", messageId)
                .put("pollsUsed", pollsUsed).put("remoteCostStatus", "UNKNOWN");
        if (task.parentTaskId() != null) node.put("parentTaskId", task.parentTaskId().toString());
        var savedRemoteTaskId = remoteTaskId == null ? operation.remoteTaskId() : remoteTaskId;
        var savedRemoteContextId = remoteContextId == null ? operation.remoteContextId() : remoteContextId;
        if (savedRemoteTaskId != null) node.put("remoteTaskId", savedRemoteTaskId);
        if (savedRemoteContextId != null) node.put("remoteContextId", savedRemoteContextId);
        if (resultHash != null) node.put("resultHash", resultHash);
        return write(node);
    }

    private String boundedText(JsonNode node, int maxLength) {
        if (node == null || !node.isTextual() || node.asText().isBlank() || node.asText().length() > maxLength)
            throw new IllegalArgumentException("text field");
        return node.asText();
    }

    private boolean hasUnknownFields(JsonNode node, Set<String> allowed) {
        var fields = node.fieldNames();
        while (fields.hasNext()) if (!allowed.contains(fields.next())) return true;
        return false;
    }

    private Instant taskDeadline(UUID tenantId, UUID workspaceId, UUID taskId) {
        return tasks.deadlineAt(tenantId, workspaceId, taskId);
    }

    private static Instant min(Instant a, Instant b) { return a.isBefore(b) ? a : b; }

    private ExecutionSnapshot submitRead(ExecutionCommand c, ToolDefinition tool, NormalizedArguments args, PolicyDecision decision) {
        var id = insertReceived(c, args.json(), decision.policyVersion());
        var current = move(getInternal(id), "RECEIVED", "VALIDATING", null);
        if (!"VALIDATING".equals(current.status())) return current;
        var beforeCall = tasks.checkExecution(c.actor(), c.workspaceId(), c.taskId(), c.attempt());
        var latest = evaluate(c, tool, args.customerId(), beforeCall.source());
        if (!beforeCall.allowed() || !latest.allowed())
            return finish(current, "VALIDATING", "DENIED", null, !beforeCall.allowed() ? beforeCall.code() : "POLICY_DENIED",
                    !beforeCall.allowed() ? beforeCall.detail() : latest.reason(), decision.policyVersion());
        current = move(current, "VALIDATING", "READY", null);
        if (!"READY".equals(current.status())) return current;
        current = move(current, "READY", "EXECUTING", null);
        if (!"EXECUTING".equals(current.status())) return current;
        try {
            var readAt = Instant.now(clock).plusSeconds(10);
            String resultJson;
            if (args.p27() != null) resultJson = p27ReadResult(c, args.p27(), args.json(), readAt);
            else {
                CustomerRecord result = "EVALUATION".equals(beforeCall.source())
                        ? connectors.readCustomerForEvaluation(c.actor().tenantId(), c.workspaceId(), args.customerId(), readAt)
                        : connectors.readCustomer(c.actor().tenantId(), c.workspaceId(), tool.bindingRef(), args.customerId(), readAt);
                resultJson = customerJson(result);
            }
            current = move(current, "EXECUTING", "VERIFYING", resultJson);
            if (!"VERIFYING".equals(current.status())) return current;
            var done = finish(current, "VERIFYING", "SUCCEEDED", resultJson, null, null, decision.policyVersion());
            if (!"SUCCEEDED".equals(done.status())) return done;
            tasks.recordToolExecution(c.taskId(), c.attempt());
            audit.append(new AuditFact("execution-succeeded:" + id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(),
                    c.taskId(), "TOOL_EXECUTION_SUCCEEDED", "SUCCEEDED", resultJson, c.traceId()));
            return done;
        } catch (EafException e) {
            return finish(current, current.status(), "FAILED", null, e.code(), e.getMessage(), decision.policyVersion());
        }
    }

    private String p27ReadResult(ExecutionCommand command, P27BusinessTaskSource source, String argumentsJson,
            Instant deadline) {
        var bindingVersion = p27Connections.bindingVersion(command.actor(), command.workspaceId(), source.bindingRef());
        var input = read(argumentsJson);
        var output = json.createObjectNode().put("bindingVersion", bindingVersion);
        switch (source.kind()) {
            case "OA_LIST" -> {
                Integer limit = input.has("limit") ? input.path("limit").asInt() : 20;
                var status = input.path("status").asText(null);
                var cursor = input.path("cursor").asText(null);
                var page = p27Connections.listTodos(command.actor(), command.workspaceId(), source.bindingRef(),
                        bindingVersion, status == null || status.isBlank() ? null : status,
                        cursor == null || cursor.isBlank() ? null : cursor, limit, deadline);
                output.set("page", json.valueToTree(page));
            }
            case "OA_ITEM" -> {
                var todoId = input.path("todoId").asText(null);
                var todo = p27Connections.getTodo(command.actor(), command.workspaceId(), source.bindingRef(),
                        bindingVersion, todoId, deadline);
                output.put("found", todo.isPresent());
                if (todo.isPresent()) output.set("todo", json.valueToTree(todo.get())); else output.putNull("todo");
            }
            case "SERVICE_STATE" -> {
                var state = p27Connections.readCurrentState(command.actor(), command.workspaceId(), source.bindingRef(),
                        bindingVersion, source.requestId(), deadline);
                if (!source.registrationOperationId().equals(state.registrationOperationId()))
                    throw EafException.conflict("SERVICE_REQUEST_REGISTRATION_MISMATCH", "当前服务台状态与原登记操作不匹配。");
                output.set("state", json.valueToTree(state));
            }
            default -> throw EafException.forbidden("P27 只读 Tool 来源无效。");
        }
        return write(output);
    }

    private JsonNode read(String value) {
        try { return json.readTree(value); }
        catch (Exception invalid) { throw EafException.invalid("P27 Tool 参数无效。"); }
    }

    /** 写入先读取受授权客户事实生成预览；此处绝不调用 POST。 */
    private ExecutionSnapshot submitWrite(ExecutionCommand c, ToolDefinition tool, NormalizedArguments args, PolicyDecision decision) {
        var connector = connectors.requireActiveForTool(c.actor().tenantId(), c.workspaceId(), tool.bindingRef());
        if (args.serviceRequest() != null && !serviceRequestSourcesCurrent(c.actor(), c.workspaceId(), args.serviceRequest()))
            return persistDenied(c, decision.policyVersion(), "SERVICE_REQUEST_EVIDENCE_UNAVAILABLE",
                    "服务请求知识出处已撤回或当前身份失去读取权限。", args);
        if (args.p27() != null) return submitP27Write(c, tool, args, decision, connector);
        CustomerRecord customer = null;
        if (args.serviceRequest() == null) {
            var readBinding = previewReadBinding(tool.bindingRef());
            customer = connectors.readCustomer(c.actor().tenantId(), c.workspaceId(), readBinding, args.customerId(),
                    Instant.now(clock).plusSeconds(10));
        }
        var connectorVersion = connectors.bindingVersion(connector);
        var operationId = UUID.randomUUID();
        var expiresAt = Instant.now(clock).plusSeconds(30 * 60);
        var ownerId = customer == null ? null : "owner:" + customer.customerId();
        ObjectNode preview = json.createObjectNode();
        preview.put("operationId", operationId.toString());
        preview.put("tool", tool.name() + "@" + tool.version());
        if (args.serviceRequest() != null) {
            var payload = args.serviceRequest();
            preview.put("submissionId", payload.submissionId().toString());
            preview.put("requesterId", payload.requesterId().toString());
            preview.put("category", payload.category());
            preview.put("title", payload.title());
            preview.put("summary", payload.summary());
            preview.put("handlingSuggestion", payload.handlingSuggestion());
            preview.put("sourceTaskId", payload.sourceTaskId().toString());
            preview.put("sourceResultHash", payload.sourceResultHash());
        } else {
            preview.put("customerId", args.customerId());
            preview.put("summary", args.summary());
        }
        if (args.outcome() == null && args.serviceRequest() == null) {
            preview.put("ownerId", ownerId);
        } else if (args.outcome() != null) {
            var payload = args.outcome();
            preview.put("externalId", payload.externalId());
            preview.put("followupId", payload.followupId().toString());
            preview.put("resultId", payload.resultId().toString());
            preview.put("resultNo", payload.resultNo());
            preview.put("recordedBy", payload.recordedBy().toString());
            preview.put("outcomeCode", payload.outcomeCode());
            if (payload.nextAction() == null) preview.putNull("nextAction"); else preview.put("nextAction", payload.nextAction());
            if (payload.nextContactAt() == null) preview.putNull("nextContactAt"); else preview.put("nextContactAt", payload.nextContactAt().toString());
            preview.put("disposition", payload.disposition());
        }
        preview.put("bindingRef", tool.bindingRef());
        preview.put("connectorBindingVersion", connectorVersion);
        if (customer != null) preview.put("sourceId", customer.sourceId());
        preview.put("policyVersion", decision.policyVersion());
        preview.put("expiresAt", expiresAt.toString());
        String previewJson = write(preview);
        var id = UUID.randomUUID();
        jdbc.update("insert into execution.execution(id, tenant_id, workspace_id, actor_id, task_id, attempt, agent_id, agent_version, tool_name, tool_version, arguments_json, request_hash, idempotency_key, status, policy_version, operation_id, connector_id, connector_version, preview_json, preview_hash, row_version, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, 'AWAITING_APPROVAL', ?, ?, ?, ?, ?::jsonb, ?, 1, ?)",
                id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.taskId(), c.attempt(), c.agentId(), c.agentVersion(),
                c.toolName(), c.toolVersion(), args.json(), Hashing.sha256(args.json()), c.idempotencyKey(), decision.policyVersion(),
                operationId, connector.id(), connectorVersion, previewJson, Hashing.sha256(previewJson), Timestamp.from(Instant.now(clock)));
        ApprovalBinding binding = new ApprovalBinding(c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.agentId(),
                c.agentVersion(), id, operationId.toString(), c.toolName(), c.toolVersion(), connector.id(), connectorVersion,
                args.json(), Hashing.sha256(args.json()), decision.policyVersion(), previewJson, Hashing.sha256(previewJson), expiresAt);
        // Execution 幂等键按任务隔离；审批表的幂等键也必须带 taskId，避免不同任务的相同参数互相占用。
        ApprovalSnapshot approval = approvals.request(new ApprovalCreateCommand(c.actor(), c.workspaceId(), c.taskId(), binding,
                c.idempotencyKey() + ":" + c.taskId()));
        jdbc.update("update execution.execution set approval_id = ? where id = ?", approval.id(), id);
        audit.append(new AuditFact("execution-preview:" + id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(),
                c.taskId(), "WRITE_PREVIEW_CREATED", "AWAITING_APPROVAL", previewJson, c.traceId()));
        var pending = getInternal(id);
        recordOutbox(pending);
        return pending;
    }

    private ExecutionSnapshot submitP27Write(ExecutionCommand c, ToolDefinition tool, NormalizedArguments args,
            PolicyDecision decision, ConnectorDefinition connector) {
        var source = args.p27();
        if (!"RESULT_SYNC".equals(source.kind()) || !"service.request.result.record".equals(tool.name()))
            return persistDenied(c, decision.policyVersion(), "P27_WRITE_SOURCE_INVALID", "服务台写入缺少固定人工结果来源。", args);
        try {
            var mapping = p27Connections.requireEmployeeMapping(c.actor(), c.workspaceId(), source.bindingRef());
            if (!mapping.bindingVersion().equals(source.bindingVersion())
                    || !mapping.externalSubjectId().equals(source.externalSubjectId()))
                return persistDenied(c, decision.policyVersion(), "P27_SOURCE_ACCESS_REVOKED",
                        "当前员工映射或服务台请求读取权限已变化。", args);
        } catch (EafException denied) {
            return persistDenied(c, decision.policyVersion(), denied.code(), "当前业务绑定不允许创建写入审批。", args);
        }
        var connectorVersion = connectors.bindingVersion(connector);
        var operationId = UUID.randomUUID();
        var expiresAt = Instant.now(clock).plusSeconds(30 * 60);
        ObjectNode preview = json.createObjectNode().put("operationId", operationId.toString())
                .put("tool", tool.name() + "@" + tool.version()).put("requestId", source.requestId())
                .put("registrationOperationId", source.registrationOperationId())
                .put("workItemId", source.workItemId().toString()).put("workItemVersion", source.workItemVersion())
                .put("sourceResultHash", source.sourceResultHash()).put("externalSubjectId", source.externalSubjectId())
                .put("completedBy", source.completedBy().toString()).put("completedAt", source.completedAt().toString())
                .put("outcome", source.outcome()).put("summary", source.summary())
                .put("expectedExternalVersion", source.expectedExternalVersion())
                .put("bindingRef", tool.bindingRef()).put("connectorBindingVersion", connectorVersion)
                .put("employeeBindingVersion", source.bindingVersion())
                .put("sourceId", "EAF-SERVICE-DESK-V1").put("policyVersion", decision.policyVersion())
                .put("expiresAt", expiresAt.toString());
        if (source.nextAction() == null) preview.putNull("nextAction"); else preview.put("nextAction", source.nextAction());
        var previewJson = write(preview);
        var id = UUID.randomUUID();
        jdbc.update("insert into execution.execution(id, tenant_id, workspace_id, actor_id, task_id, attempt, agent_id, agent_version, tool_name, tool_version, arguments_json, request_hash, idempotency_key, status, policy_version, operation_id, connector_id, connector_version, preview_json, preview_hash, row_version, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, 'AWAITING_APPROVAL', ?, ?, ?, ?, ?::jsonb, ?, 1, ?) ",
                id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.taskId(), c.attempt(), c.agentId(), c.agentVersion(),
                c.toolName(), c.toolVersion(), args.json(), Hashing.sha256(args.json()), c.idempotencyKey(), decision.policyVersion(),
                operationId, connector.id(), connectorVersion, previewJson, Hashing.sha256(previewJson), Timestamp.from(Instant.now(clock)));
        var binding = new ApprovalBinding(c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.agentId(),
                c.agentVersion(), id, operationId.toString(), c.toolName(), c.toolVersion(), connector.id(), connectorVersion,
                args.json(), Hashing.sha256(args.json()), decision.policyVersion(), previewJson, Hashing.sha256(previewJson), expiresAt);
        var approval = approvals.request(new ApprovalCreateCommand(c.actor(), c.workspaceId(), c.taskId(), binding,
                c.idempotencyKey() + ":" + c.taskId()));
        jdbc.update("update execution.execution set approval_id = ? where id = ?", approval.id(), id);
        audit.append(new AuditFact("p27-result-sync-preview:" + id, c.actor().tenantId(), c.workspaceId(),
                c.actor().actorId(), c.taskId(), "SERVICE_REQUEST_RESULT_PREVIEW_CREATED", "AWAITING_APPROVAL", previewJson, c.traceId()));
        var pending = getInternal(id);
        recordOutbox(pending);
        return pending;
    }

    @Override
    public ExecutionSnapshot resume(ActorContext actor, UUID workspaceId, UUID executionId) {
        var claim = transactions.execute(transaction -> {
            var current = visible(actor, workspaceId, executionId);
            if (!"AWAITING_APPROVAL".equals(current.status())) return new ResumeAttempt(current, false);
            if (current.approvalId() == null) throw EafException.conflict("APPROVAL_MISSING", "Execution 缺少审批绑定。");
            ApprovalSnapshot approval = approvals.get(actor, workspaceId, current.approvalId());
            if ("PENDING".equals(approval.state())) return new ResumeAttempt(current, false);
            if (!"APPROVED".equals(approval.state()))
                return new ResumeAttempt(finish(current, "AWAITING_APPROVAL", "DENIED", null, "APPROVAL_DENIED", "审批未批准该精确写入。", current.policyVersion()), false);
            NormalizedArguments args = normalizeStored(actor, workspaceId, current);
            var authority = jdbc.query("select agent_id, agent_version, request_hash from execution.execution where id = ?",
                    rs -> rs.next() ? new ExecutionAuthority(rs.getObject("agent_id", UUID.class), rs.getString("agent_version"), rs.getString("request_hash")) : null,
                    current.id());
            var bindingProblem = approvalBindingProblem(actor, current, approval, args, authority);
            if (bindingProblem != null)
                return new ResumeAttempt(finish(current, "AWAITING_APPROVAL", "DENIED", null, bindingProblem,
                        "审批绑定、参数或有效期已变化，拒绝提交外部写入。", current.policyVersion()), false);
            // Task 租约检查与 EXECUTING 意图同事务提交，之后释放行锁再调用 CRM。
            var check = tasks.checkExecution(actor, workspaceId, current.taskId(), current.attempt());
            if (!check.allowed())
                return new ResumeAttempt(finish(current, "AWAITING_APPROVAL", "DENIED", null, check.code(), check.detail(), current.policyVersion()), false);
            if (args.serviceRequest() != null && !serviceRequestSourcesCurrent(actor, workspaceId, args.serviceRequest()))
                return new ResumeAttempt(finish(current, "AWAITING_APPROVAL", "DENIED", null,
                        "SERVICE_REQUEST_EVIDENCE_UNAVAILABLE", "批准前服务请求知识出处已撤回或失去读取权限。", current.policyVersion()), false);
            ToolDefinition tool;
            ConnectorDefinition connector;
            PolicyDecision decision;
            try {
                tool = tools.requirePublished(current.tenantId(), workspaceId, current.toolName(), current.toolVersion());
                connector = connectors.requireActiveForTool(current.tenantId(), workspaceId, tool.bindingRef());
                if (args.p27() != null) {
                    var mapping = p27Connections.requireEmployeeMapping(actor, workspaceId, args.p27().bindingRef());
                    if (!mapping.bindingVersion().equals(args.p27().bindingVersion())
                            || !mapping.externalSubjectId().equals(args.p27().externalSubjectId()))
                        return new ResumeAttempt(finish(current, "AWAITING_APPROVAL", "DENIED", null,
                                "EMPLOYEE_MAPPING_CHANGED", "审批后员工映射已变化，拒绝使用旧批准。", current.policyVersion()), false);
                }
                decision = args.serviceRequest() != null
                        ? evaluateServiceRequest(actor, workspaceId, authority.agentId(), authority.agentVersion(), tool, check.source())
                        : policy.evaluate(new PolicyRequest(actor, workspaceId, authority.agentId(), authority.agentVersion(),
                                current.toolName(), current.toolVersion(), tool.effect(), args.customerId(), check.source()));
            } catch (EafException e) {
                return new ResumeAttempt(finish(current, "AWAITING_APPROVAL", "DENIED", null, e.code(),
                        "批准后重新检查工具、Connector 或 Policy 未通过。", current.policyVersion()), false);
            }
            if (!decision.allowed() || !current.policyVersion().equals(decision.policyVersion())
                    || !approval.binding().connectorId().equals(connector.id())
                    || !approval.binding().connectorVersion().equals(connectors.bindingVersion(connector))
                    || !current.connectorVersion().equals(connectors.bindingVersion(connector))
                    || !tool.bindingRef().equals(previewBindingRef(current)))
                return new ResumeAttempt(finish(current, "AWAITING_APPROVAL", "DENIED", null,
                        decision.allowed() ? "APPROVAL_AUTHORITY_CHANGED" : "POLICY_DENIED",
                        "批准后权限、Policy 或 Connector 版本已变化，拒绝提交外部写入。", decision.policyVersion()), false);
            // 停止只拦截尚未提交的写入意图；既有 UNKNOWN 的核验走独立 verify 路径。
            if (!operationalControl.businessOutboundOpen(actor.tenantId(), workspaceId))
                return new ResumeAttempt(current, false);
            var now = Instant.now(clock);
            var changed = jdbc.update("update execution.execution set status = 'EXECUTING', started_at = coalesce(started_at, ?), lease_until = ?, row_version = row_version + 1 where id = ? and row_version = ? and status = 'AWAITING_APPROVAL'",
                    Timestamp.from(now), Timestamp.from(now.plusSeconds(30)), current.id(), current.version());
            if (changed == 1) tasks.markExternalEffect(current.taskId(), current.attempt(), current.operationId(), "IN_PROGRESS");
            return new ResumeAttempt(getInternal(current.id()), changed == 1);
        });
        var current = claim.snapshot();
        if (!claim.claimed()) return current;

        ExternalWriteResult result;
        NormalizedArguments args;
        try {
            args = normalizeStored(actor, workspaceId, current);
            if (args.p27() != null)
                return resumeP27Write(actor, workspaceId, current, args);
            var node = json.readTree(current.previewJson());
            if (args.serviceRequest() != null) {
                var payload = args.serviceRequest();
                var request = new ServiceRequestPayload(current.operationId().toString(), payload.requesterId().toString(),
                        payload.category(), payload.title(), payload.summary(), payload.handlingSuggestion(),
                        payload.sourceTaskId(), payload.sourceResultHash());
                var writeResult = outsideTransaction.execute(transaction -> connectors.registerServiceRequest(
                        actor.tenantId(), workspaceId, node.path("bindingRef").asText(), current.connectorVersion(),
                        request, Instant.now(clock).plusSeconds(10)));
                var serviceResult = writeResult == null
                        ? ServiceRequestWriteResult.unknown("SERVICE_REQUEST_WRITE_UNKNOWN", "服务台写入回执缺失，外部状态待核验。")
                        : writeResult;
                var readback = "REJECTED".equals(serviceResult.state()) ? Optional.<ServiceRequestReceipt>empty()
                        : outsideTransaction.execute(transaction -> connectors.findServiceRequest(actor.tenantId(),
                                workspaceId, node.path("bindingRef").asText(), current.connectorVersion(),
                                current.operationId().toString(), Instant.now(clock).plusSeconds(10)));
                return transactions.execute(transaction -> resolveServiceRequestExternal(actor, workspaceId, current,
                        args, serviceResult, readback));
            }
            if (args.outcome() != null) {
                var payload = args.outcome();
                var request = new FollowupOutcomeRecord(current.operationId().toString(), payload.externalId(),
                        payload.customerId(), payload.followupId(), payload.resultId(), payload.resultNo(),
                        payload.recordedBy(), payload.outcomeCode(), payload.summary(), payload.nextAction(),
                        payload.nextContactAt(), payload.disposition(), null, null);
                var writeResult = outsideTransaction.execute(transaction -> connectors.recordFollowupOutcome(
                        actor.tenantId(), workspaceId, node.path("bindingRef").asText(), current.connectorVersion(),
                        current.operationId().toString(), request, Instant.now(clock).plusSeconds(10)));
                var outcomeResult = writeResult == null
                        ? ExternalOutcomeWriteResult.unknown("CRM_OUTCOME_UNKNOWN", "CRM 结果写入回执缺失，外部事实待核验。")
                        : writeResult;
                if ("REJECTED".equals(outcomeResult.state())) {
                    return transactions.execute(transaction -> {
                        var done = finish(current, "EXECUTING", "FAILED", null, outcomeResult.errorCode(),
                                outcomeResult.detail(), current.policyVersion());
                        if ("FAILED".equals(done.status())) customerFollowups.closeSyncAdmission(actor, workspaceId, payload.syncAttemptId(), "FAILED_SAFE");
                        return done;
                    });
                }
                var readback = outsideTransaction.execute(transaction -> connectors.findFollowupOutcome(
                        actor.tenantId(), workspaceId, node.path("bindingRef").asText(), current.connectorVersion(),
                        current.operationId().toString(), Instant.now(clock).plusSeconds(10)));
                return transactions.execute(transaction -> resolveOutcomeExternal(actor, workspaceId, current,
                        args, outcomeResult, readback));
            }
            result = connectors.createFollowup(actor.tenantId(), workspaceId, node.path("bindingRef").asText(),
                    current.connectorVersion(), current.operationId().toString(), args.customerId(),
                    args.summary(), node.path("ownerId").asText(), Instant.now(clock).plusSeconds(10));
        } catch (Exception e) {
            return transactions.execute(transaction -> finishUnknown(current, "EXECUTING", "CRM_WRITE_UNKNOWN", "写入调用异常，外部事实必须通过核验确认。", null));
        }
        // CRM 已返回后通过新的短事务 CAS；若过期扫描先赢，只保留回执证据。
        return resolveExternal(actor, workspaceId, current, args, result);
    }

    @Override
    public ExecutionSnapshot verify(ActorContext actor, UUID workspaceId, UUID executionId) {
        requireP27OrExecutionVerify(actor, workspaceId, executionId);
        var current = visible(actor, workspaceId, executionId);
        if (!"UNKNOWN".equals(current.status()) && !"VERIFICATION_FAILED".equals(current.status())) return current;
        // 先以短事务抢占核验状态，再在事务外查询 CRM，避免网络调用持有数据库事务或行锁。
        var verifying = transactions.execute(transaction -> {
            var fresh = visible(actor, workspaceId, executionId);
            if (!"UNKNOWN".equals(fresh.status()) && !"VERIFICATION_FAILED".equals(fresh.status())) return fresh;
            return move(fresh, fresh.status(), "VERIFYING", null);
        });
        if (!"VERIFYING".equals(verifying.status())) return verifying;
        try {
            var args = normalizeStored(actor, workspaceId, verifying);
            if (args.p27() != null) {
                var receipt = outsideTransaction.execute(transaction -> p27Connections.findHandlingResult(actor,
                        workspaceId, args.p27().bindingRef(), args.p27().bindingVersion(),
                        verifying.operationId().toString(), Instant.now(clock).plusSeconds(10)));
                if (receipt == null || receipt.isEmpty()) return transactions.execute(transaction -> finishUnknown(verifying,
                        "VERIFYING", "SERVICE_REQUEST_RESULT_NOT_FOUND",
                        "原 operationId 暂无匹配回执；不会重新发送服务台写入。", "{\"found\":false}"));
                return transactions.execute(transaction -> finishP27Verified(actor, workspaceId, verifying,
                        args.p27(), receipt.get(), true));
            }
            var target = approvedWriteTarget(actor, workspaceId, verifying, args);
            if (args.serviceRequest() != null) {
                var record = outsideTransaction.execute(transaction -> connectors.findServiceRequest(actor.tenantId(), workspaceId,
                        target.tool().bindingRef(), verifying.connectorVersion(), verifying.operationId().toString(),
                        Instant.now(clock).plusSeconds(10)));
                if (record.isEmpty()) return transactions.execute(transaction -> finishUnknown(verifying, "VERIFYING",
                        "SERVICE_REQUEST_NOT_FOUND", "原 operationId 暂未找到服务台回执；不会因此重新提交登记。", "{\"found\":false}"));
                return transactions.execute(transaction -> finishServiceRequestVerified(actor, workspaceId, verifying,
                        args, record.get(), true));
            }
            if (args.outcome() != null) {
                var record = outsideTransaction.execute(transaction -> connectors.findFollowupOutcome(actor.tenantId(), workspaceId,
                        target.tool().bindingRef(), verifying.connectorVersion(), verifying.operationId().toString(),
                        Instant.now(clock).plusSeconds(10)));
                if (record.isEmpty()) return transactions.execute(transaction -> finishUnknown(verifying, "VERIFYING",
                        "CRM_OUTCOME_NOT_FOUND", "原 operationId 暂未找到结果回执；不会因此重新提交写入。", "{\"found\":false}"));
                return transactions.execute(transaction -> finishOutcomeVerified(actor, workspaceId, verifying,
                        args, record.get(), true));
            }
            var record = outsideTransaction.execute(transaction -> connectors.findFollowup(actor.tenantId(), workspaceId,
                    target.tool().bindingRef(), verifying.connectorVersion(), verifying.operationId().toString(),
                    Instant.now(clock).plusSeconds(10)));
            if (record.isEmpty()) return transactions.execute(transaction -> finishUnknown(verifying, "VERIFYING",
                    "CRM_RECORD_NOT_FOUND", "核验窗口内未找到外部跟进记录；不会因此重新提交写入。", "{\"found\":false}"));
            // UNKNOWN 经核验确认成功时补记唯一一次真实工具执行；成功状态 CAS 使重复核验不会重复计数。
            return transactions.execute(transaction -> finishVerified(actor, workspaceId, verifying, args, record.get(), true));
        } catch (EafException e) {
            return transactions.execute(transaction -> finishUnknown(verifying, "VERIFYING", e.code(),
                    "当前权限、资产或 CRM 目标不允许回读；外部事实仍待人工处置。", null));
        } catch (Exception e) {
            return transactions.execute(transaction -> finishUnknown(verifying, "VERIFYING",
                    "CRM_VERIFY_UNAVAILABLE", "外部事实暂时无法核验。", null));
        }
    }

    @Override
    public ExecutionVerificationReceipt verifyOperational(ActorContext actor, UUID workspaceId, UUID executionId,
                                                          String requestKey, String reason) {
        var normalizedReason = reason == null ? null : reason.strip();
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("UNKNOWN 核验运维命令只允许本人 HUMAN 操作者执行。");
        if (requestKey == null || requestKey.isBlank() || requestKey.length() > 200
                || normalizedReason == null || normalizedReason.isBlank() || normalizedReason.length() > 500)
            throw EafException.invalid("Execution 核验需要不超过 200 字符的请求键和 1 至 500 字符的原因。");
        requireP27OrExecutionVerify(actor, workspaceId, executionId);

        var keyHash = Hashing.sha256(String.join("\u001f", actor.tenantId().toString(), workspaceId.toString(), requestKey));
        var requestHash = Hashing.sha256(String.join("\u001f", actor.actorId().toString(), executionId.toString(), normalizedReason));
        var command = transactions.execute(tx -> {
            var prior = operationalVerifyCommand(actor, workspaceId, keyHash);
            if (prior != null) return requireMatchingVerifyCommand(actor, executionId, requestHash, prior);

            // 仅 UNKNOWN 和 VERIFICATION_FAILED 可新建处置命令；资源锁使接受点与并发完成按版本串行。
            var target = jdbc.query("select operation_id, status, row_version from execution.execution "
                            + "where id = ? and tenant_id = ? and workspace_id = ? for update",
                    rs -> rs.next() ? new VerifyTarget(rs.getObject("operation_id", UUID.class),
                            rs.getString("status"), rs.getLong("row_version")) : null,
                    executionId, actor.tenantId(), workspaceId);
            if (target == null) throw EafException.notFound();
            if (!"UNKNOWN".equals(target.status()) && !"VERIFICATION_FAILED".equals(target.status()))
                throw EafException.conflict("EXECUTION_NOT_VERIFIABLE", "只有 UNKNOWN 或核验失败的 Execution 可提交运维核验命令。");

            var commandId = UUID.randomUUID();
            var now = Instant.now(clock);
            jdbc.update("insert into execution.operations_command(command_id, tenant_id, workspace_id, execution_id, operation_id, "
                            + "actor_id, request_key_hash, request_hash, reason, state, created_at) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?) on conflict (tenant_id, workspace_id, request_key_hash) do nothing",
                    commandId, actor.tenantId(), workspaceId, executionId, target.operationId(), actor.actorId(), keyHash,
                    requestHash, normalizedReason, Timestamp.from(now));
            var accepted = operationalVerifyCommand(actor, workspaceId, keyHash);
            if (accepted == null) throw EafException.conflict("EXECUTION_COMMAND_CONFLICT", "核验请求键已被并发命令占用。");
            var matched = requireMatchingVerifyCommand(actor, executionId, requestHash, accepted);
            matched = withReplayFlag(matched, !commandId.equals(matched.commandId()));
            if (commandId.equals(matched.commandId()))
                audit.append(new AuditFact("execution-verify-command:" + commandId, actor.tenantId(), workspaceId,
                        actor.actorId(), null, "EXECUTION_VERIFY_REQUESTED", "PENDING",
                        "{\"executionId\":\"" + executionId + "\",\"operationId\":\"" + target.operationId()
                                + "\",\"reasonHash\":\"" + Hashing.sha256(normalizedReason) + "\"}", null));
            return matched;
        });
        if (command.completed()) return verifyReceipt(command, true);

        // 核验仍复用原 operationId。若进程在 VERIFYING 中断，命令留在 PENDING，租约恢复后可安全续做只读回读。
        var result = verify(actor, workspaceId, executionId);
        return transactions.execute(tx -> {
            if ("VERIFYING".equals(result.status()))
                return new ExecutionVerificationReceipt(command.commandId(), executionId, command.operationId(),
                        result.status(), result.version(), false, command.replayed());
            var now = Instant.now(clock);
            var changed = jdbc.update("update execution.operations_command set state = 'COMPLETED', result_status = ?, "
                            + "result_version = ?, completed_at = ? where command_id = ? and state = 'PENDING'",
                    result.status(), result.version(), Timestamp.from(now), command.commandId());
            if (changed == 1)
                audit.append(new AuditFact("execution-verify-command-result:" + command.commandId(), actor.tenantId(),
                        workspaceId, actor.actorId(), null, "EXECUTION_VERIFY_COMMAND_COMPLETED", result.status(),
                        "{\"executionId\":\"" + executionId + "\",\"operationId\":\"" + command.operationId()
                                + "\",\"version\":" + result.version() + "}", null));
            var completed = operationalVerifyCommand(actor, workspaceId, keyHash);
            return verifyReceipt(completed, command.replayed());
        });
    }

    private void requireP27OrExecutionVerify(ActorContext actor, UUID workspaceId, UUID executionId) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("Execution 核验只允许本人 HUMAN 操作者执行。");
        var row = jdbc.query("select task_id, attempt, tool_name, tool_version, arguments_json::text arguments_json "
                        + "from execution.execution where id = ? and tenant_id = ? and workspace_id = ? and actor_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("task_id", UUID.class), rs.getInt("attempt"),
                        rs.getString("tool_name"), rs.getString("tool_version"), rs.getString("arguments_json")} : null,
                executionId, actor.tenantId(), workspaceId, actor.actorId());
        if (row != null && "service.request.result.record".equals(row[2]) && "1.0.0".equals(row[3])) {
            workspaces.require(actor, workspaceId, "service-request:result:sync");
            p27Sources.requireToolSource(actor, workspaceId, (UUID) row[0], (Integer) row[1], (String) row[2],
                    (String) row[3], (String) row[4]);
        } else workspaces.require(actor, workspaceId, "execution:verify");
    }

    private VerifyCommand operationalVerifyCommand(ActorContext actor, UUID workspaceId, String keyHash) {
        return jdbc.query("select command_id, execution_id, operation_id, actor_id, request_hash, state, result_status, "
                        + "result_version, created_at from execution.operations_command "
                        + "where tenant_id = ? and workspace_id = ? and request_key_hash = ?",
                rs -> rs.next() ? new VerifyCommand(rs.getObject("command_id", UUID.class),
                        rs.getObject("execution_id", UUID.class), rs.getObject("operation_id", UUID.class),
                        rs.getObject("actor_id", UUID.class), rs.getString("request_hash"),
                        "COMPLETED".equals(rs.getString("state")), rs.getString("result_status"),
                        (Long) rs.getObject("result_version"), rs.getTimestamp("created_at").toInstant(), true) : null,
                actor.tenantId(), workspaceId, keyHash);
    }

    private VerifyCommand requireMatchingVerifyCommand(ActorContext actor, UUID executionId, String requestHash,
                                                       VerifyCommand prior) {
        if (!prior.executionId().equals(executionId) || !actor.actorId().equals(prior.actorId())
                || !requestHash.equals(prior.requestHash()))
            throw EafException.conflict("EXECUTION_COMMAND_CONFLICT", "同一核验请求键不能绑定不同目标、操作者或原因。");
        return prior;
    }

    private ExecutionVerificationReceipt verifyReceipt(VerifyCommand command, boolean replayed) {
        return new ExecutionVerificationReceipt(command.commandId(), command.executionId(), command.operationId(),
                command.resultStatus(), command.resultVersion() == null ? 0 : command.resultVersion(),
                command.completed(), replayed);
    }

    private VerifyCommand withReplayFlag(VerifyCommand command, boolean replayed) {
        return new VerifyCommand(command.commandId(), command.executionId(), command.operationId(), command.actorId(),
                command.requestHash(), command.completed(), command.resultStatus(), command.resultVersion(),
                command.createdAt(), replayed);
    }

    @Override
    public Optional<ExecutionSnapshot> pending(UUID tenantId, UUID workspaceId, UUID taskId, int attempt) {
        return jdbc.query(select() + " where tenant_id = ? and workspace_id = ? and task_id = ? and attempt = ? and status in ('AWAITING_APPROVAL','AWAITING_REMOTE','UNKNOWN','VERIFICATION_FAILED','SUCCEEDED') order by created_at desc limit 1",
                rs -> rs.next() ? Optional.of(map(rs)) : Optional.empty(), tenantId, workspaceId, taskId, attempt);
    }

    @Override
    @Transactional
    public void cancelAwaitingApproval(UUID tenantId, UUID workspaceId, UUID taskId) {
        // Task 已先取消并持有自己的锁；这里仅关闭未提交的审批执行，不触碰提交中或待核验的写入。
        var current = jdbc.query(select() + " where tenant_id = ? and workspace_id = ? and task_id = ? and status = 'AWAITING_APPROVAL' order by created_at desc limit 1 for update",
                rs -> rs.next() ? map(rs) : null, tenantId, workspaceId, taskId);
        if (current == null) return;
        if (current.approvalId() != null)
            approvals.cancelPending(tenantId, workspaceId, taskId, current.id());
        var changed = jdbc.update("update execution.execution set status = 'CANCELLED', error_code = 'TASK_CANCELLED', error_detail = 'Task 已取消，审批后不能再提交写入。', ended_at = ?, lease_until = null, row_version = row_version + 1 where id = ? and row_version = ? and status = 'AWAITING_APPROVAL'",
                Timestamp.from(Instant.now(clock)), current.id(), current.version());
        if (changed == 1) recordOutbox(getInternal(current.id()));
    }

    @Override
    @Transactional
    public void recoverOnStartup() {
        // 周期扫描只推进当前租约已过期的操作；rowVersion 同时使晚到 Worker 失去写回资格。
        var expired = jdbc.query("select id, status, row_version from execution.execution where status in ('EXECUTING','VERIFYING') and lease_until is not null and lease_until <= now()",
                (rs, row) -> new Object[]{rs.getObject("id", UUID.class), rs.getString("status"), rs.getLong("row_version")});
        for (var row : expired) {
            var id = (UUID) row[0];
            var status = (String) row[1];
            var version = (Long) row[2];
            var changed = jdbc.update("update execution.execution set status = 'UNKNOWN', error_code = 'INTERRUPTED_BEFORE_RESULT', error_detail = '执行租约过期，外部事实必须核验。', verification_json = coalesce(verification_json, '{\"interrupted\":true}'::jsonb), ended_at = ?, lease_until = null, row_version = row_version + 1 where id = ? and status = ? and row_version = ? and lease_until <= now()",
                    Timestamp.from(Instant.now(clock)), id, status, version);
            if (changed == 1) {
                var recovered = getInternal(id);
                tasks.markExternalEffect(recovered.taskId(), recovered.attempt(), recovered.operationId(), "UNKNOWN");
                var remote = getRemoteOperation(id, true);
                if (remote != null && "SEND_PENDING".equals(remote.state())) {
                    // Send 可能已到达 peer；中断后固定为 SEND_UNKNOWN，禁止自动重发。
                    var sendUnknown = jdbc.update("update execution.remote_a2a_operation set state = 'SEND_UNKNOWN', next_poll_at = null, row_version = row_version + 1, updated_at = now() where execution_id = ? and row_version = ? and state = 'SEND_PENDING'",
                            id, remote.rowVersion());
                    if (sendUnknown == 1)
                        audit.append(new AuditFact("remote-a2a-send-recovered-unknown:" + id, recovered.tenantId(),
                                recovered.workspaceId(), remote.actorId(), recovered.taskId(),
                                "REMOTE_AGENT_SEND_UNKNOWN", "SEND_UNKNOWN", remoteEvidence(id, remote.messageId(),
                                null, null, remote.pollsUsed(), null), remote.traceId()));
                }
                recordOutbox(recovered);
            }
        }
    }

    private ExecutionSnapshot resolveExternal(ActorContext actor, UUID workspaceId, ExecutionSnapshot current,
                                              NormalizedArguments args, ExternalWriteResult result) {
        if ("REJECTED".equals(result.state()))
            return transactions.execute(tx -> finish(current, "EXECUTING", "FAILED", null,
                    result.errorCode(), result.detail(), current.policyVersion()));
        if (!"ACCEPTED".equals(result.state()))
            return transactions.execute(tx -> finishUnknown(current, "EXECUTING", result.errorCode(), result.detail(), null));

        var verifying = transactions.execute(tx -> move(current, "EXECUTING", "VERIFYING", null));
        if (!"VERIFYING".equals(verifying.status())) {
            if (result.record() != null) preserveLateEvidence(verifying.id(), write(result.record()));
            return getInternal(verifying.id());
        }
        if (result.record() != null)
            return transactions.execute(tx -> finishVerified(actor, workspaceId, verifying, args, result.record(), true));

        FollowupRecord record;
        try {
            record = outsideTransaction.execute(tx -> {
                var target = approvedWriteTarget(actor, workspaceId, verifying, args);
                return connectors.findFollowup(actor.tenantId(), workspaceId, target.tool().bindingRef(),
                        verifying.connectorVersion(), verifying.operationId().toString(),
                        Instant.now(clock).plusSeconds(10)).orElse(null);
            });
        } catch (Exception unavailable) {
            return transactions.execute(tx -> finishUnknown(verifying, "VERIFYING", "CRM_VERIFY_UNAVAILABLE",
                    "写入结果待核验。", null));
        }
        if (record == null)
            return transactions.execute(tx -> finishUnknown(verifying, "VERIFYING", "CRM_RECORD_NOT_FOUND",
                    "CRM 接收后未返回可核验记录。", "{\"found\":false}"));
        return transactions.execute(tx -> finishVerified(actor, workspaceId, verifying, args, record, true));
    }

    private ExecutionSnapshot resolveServiceRequestExternal(ActorContext actor, UUID workspaceId,
            ExecutionSnapshot current, NormalizedArguments args, ServiceRequestWriteResult writeResult,
            Optional<ServiceRequestReceipt> readback) {
        if ("REJECTED".equals(writeResult.state()))
            return finish(current, "EXECUTING", "FAILED", null, writeResult.errorCode(), writeResult.detail(), current.policyVersion());
        if (readback == null || readback.isEmpty())
            return finishUnknown(current, "EXECUTING", writeResult.errorCode() == null
                    ? "SERVICE_REQUEST_NOT_FOUND" : writeResult.errorCode(),
                    "服务台写入结果尚未由原 operationId 回读确认；不会盲目重发。", "{\"found\":false}");
        var evidence = write(readback.get());
        var verifying = move(current, "EXECUTING", "VERIFYING", null);
        if (!"VERIFYING".equals(verifying.status())) {
            preserveLateEvidence(verifying.id(), evidence);
            return getInternal(verifying.id());
        }
        return finishServiceRequestVerified(actor, workspaceId, verifying, args, readback.get(), true);
    }

    private ExecutionSnapshot finishServiceRequestVerified(ActorContext actor, UUID workspaceId,
            ExecutionSnapshot current, NormalizedArguments args, ServiceRequestReceipt receipt, boolean countExecution) {
        var payload = args.serviceRequest();
        var evidence = receipt == null ? "null" : write(receipt);
        boolean matches = payload != null && receipt != null && current.operationId().toString().equals(receipt.operationId())
                && receipt.requestId() != null && !receipt.requestId().isBlank() && receipt.requestId().length() <= 160
                && "REGISTERED".equals(receipt.status()) && payload.requesterId().toString().equals(receipt.requesterId())
                && payload.category().equals(receipt.category()) && payload.title().equals(receipt.title())
                && payload.summary().equals(receipt.summary())
                && payload.handlingSuggestion().equals(receipt.handlingSuggestion())
                && payload.sourceTaskId().equals(receipt.sourceTaskId())
                && payload.sourceResultHash().equals(receipt.sourceResultHash());
        if (!matches) return finish(current, "VERIFYING", "VERIFICATION_FAILED", evidence,
                "SERVICE_REQUEST_MISMATCH", "服务台回执与已批准的服务请求载荷不一致。", current.policyVersion(), evidence);
        var result = json.createObjectNode().put("requestId", receipt.requestId()).put("operationId", receipt.operationId())
                .put("status", receipt.status()).put("requesterId", receipt.requesterId()).put("category", receipt.category())
                .put("title", receipt.title()).put("summary", receipt.summary())
                .put("handlingSuggestion", receipt.handlingSuggestion()).put("sourceTaskId", receipt.sourceTaskId().toString())
                .put("sourceResultHash", receipt.sourceResultHash());
        if (receipt.acceptedAt() != null) result.put("acceptedAt", receipt.acceptedAt().toString());
        var done = finish(current, "VERIFYING", "SUCCEEDED", write(result), null, null, current.policyVersion(), evidence);
        if (!"SUCCEEDED".equals(done.status())) {
            preserveLateEvidence(current.id(), evidence);
            return getInternal(current.id());
        }
        if (countExecution) tasks.recordToolExecution(current.taskId(), current.attempt());
        audit.append(new AuditFact("execution-service-request-verified:" + current.id(), actor.tenantId(), workspaceId,
                actor.actorId(), current.taskId(), "SERVICE_REQUEST_REGISTERED", "SUCCEEDED", evidence, null));
        return done;
    }

    private ExecutionSnapshot resumeP27Write(ActorContext actor, UUID workspaceId, ExecutionSnapshot current,
            NormalizedArguments args) {
        var source = args.p27();
        var deadline = Instant.now(clock).plusSeconds(10);
        try {
            var allowed = outsideTransaction.execute(tx -> p27Connections.canReadServiceRequest(actor, workspaceId,
                    source.bindingRef(), source.bindingVersion(), source.requestId(), deadline));
            if (!Boolean.TRUE.equals(allowed))
                return transactions.execute(tx -> finish(current, "EXECUTING", "FAILED", null,
                        "SERVICE_REQUEST_ACCESS_REVOKED", "当前员工已失去该服务台请求读取权限，未发送结果写入。", current.policyVersion()));
        } catch (Exception denied) {
            return transactions.execute(tx -> finish(current, "EXECUTING", "FAILED", null,
                    "SERVICE_REQUEST_ACCESS_UNAVAILABLE", "写入前无法确认当前服务台对象权限，未发送结果写入。", current.policyVersion()));
        }
        var payload = p27Payload(current.operationId(), source);
        P27BusinessConnectionIntegrationPort.ServiceRequestResultWriteResult writeResult;
        try {
            writeResult = outsideTransaction.execute(tx -> p27Connections.recordHandlingResult(actor, workspaceId,
                    source.bindingRef(), source.bindingVersion(), payload, deadline));
        } catch (Exception uncertain) {
            writeResult = P27BusinessConnectionIntegrationPort.ServiceRequestResultWriteResult.unknown(
                    "SERVICE_REQUEST_RESULT_UNKNOWN", "服务台写入调用结果不明，需按原 operationId 核验。");
        }
        if (writeResult == null) writeResult = P27BusinessConnectionIntegrationPort.ServiceRequestResultWriteResult.unknown(
                "SERVICE_REQUEST_RESULT_UNKNOWN", "服务台写入回执缺失，需按原 operationId 核验。");
        final var settledWriteResult = writeResult;
        if ("REJECTED".equals(settledWriteResult.state()))
            return transactions.execute(tx -> finish(current, "EXECUTING", "FAILED", null, settledWriteResult.errorCode(),
                    settledWriteResult.detail(), current.policyVersion()));
        Optional<P27BusinessConnectionIntegrationPort.ServiceRequestHandlingReceipt> readback;
        try {
            readback = outsideTransaction.execute(tx -> p27Connections.findHandlingResult(actor, workspaceId,
                    source.bindingRef(), source.bindingVersion(), current.operationId().toString(), Instant.now(clock).plusSeconds(10)));
        } catch (Exception unavailable) { readback = Optional.empty(); }
        if (readback == null || readback.isEmpty())
            return transactions.execute(tx -> finishUnknown(current, "EXECUTING",
                    settledWriteResult.errorCode() == null ? "SERVICE_REQUEST_RESULT_NOT_FOUND" : settledWriteResult.errorCode(),
                    "没有取得原 operationId 的匹配回执；后续只核验该操作，不重新发送。", "{\"found\":false}"));
        var receipt = readback.get();
        var evidence = write(receipt);
        var verifying = move(current, "EXECUTING", "VERIFYING", null);
        if (!"VERIFYING".equals(verifying.status())) {
            preserveLateEvidence(verifying.id(), evidence);
            return getInternal(verifying.id());
        }
        return transactions.execute(tx -> finishP27Verified(actor, workspaceId, verifying, source, receipt, true));
    }

    private P27BusinessConnectionIntegrationPort.ServiceRequestHandlingPayload p27Payload(UUID operationId,
            P27BusinessTaskSource source) {
        return new P27BusinessConnectionIntegrationPort.ServiceRequestHandlingPayload(operationId.toString(),
                source.requestId(), source.registrationOperationId(), source.workItemId().toString(),
                source.workItemVersion(), source.sourceResultHash(), source.externalSubjectId(),
                source.completedBy().toString(), source.completedAt(), source.outcome(), source.summary(),
                source.nextAction(), source.expectedExternalVersion());
    }

    private ExecutionSnapshot finishP27Verified(ActorContext actor, UUID workspaceId, ExecutionSnapshot current,
            P27BusinessTaskSource source, P27BusinessConnectionIntegrationPort.ServiceRequestHandlingReceipt receipt,
            boolean countExecution) {
        var evidence = receipt == null ? "null" : write(receipt);
        var expectedStatus = "COMPLETED".equals(source.outcome()) ? "RESOLVED" : "IN_PROGRESS";
        var matches = receipt != null && current.operationId().toString().equals(receipt.operationId())
                && source.requestId().equals(receipt.requestId())
                && source.registrationOperationId().equals(receipt.registrationOperationId())
                && source.workItemId().toString().equals(receipt.workItemId())
                && source.workItemVersion() == receipt.workItemVersion()
                && source.sourceResultHash().equals(receipt.sourceResultHash())
                && source.externalSubjectId().equals(receipt.externalSubjectId())
                && source.completedBy().toString().equals(receipt.completedBy())
                && source.completedAt().equals(receipt.completedAt())
                && source.outcome().equals(receipt.outcome()) && source.summary().equals(receipt.summary())
                && java.util.Objects.equals(source.nextAction(), receipt.nextAction())
                && "RECORDED".equals(receipt.recordState())
                && source.expectedExternalVersion().equals(receipt.previousExternalVersion())
                && receipt.resultingExternalVersion() != null
                && expectedStatus.equals(receipt.resultingStatus()) && receipt.acceptedAt() != null;
        if (!matches) return finish(current, "VERIFYING", "VERIFICATION_FAILED", evidence,
                "SERVICE_REQUEST_RESULT_MISMATCH", "服务台回执与已审批的人工来源、版本或 operationId 不一致。",
                current.policyVersion(), evidence);
        var result = json.createObjectNode().put("operationId", receipt.operationId())
                .put("requestId", receipt.requestId()).put("registrationOperationId", receipt.registrationOperationId())
                .put("resultId", receipt.resultId()).put("workItemId", receipt.workItemId())
                .put("workItemVersion", receipt.workItemVersion()).put("sourceResultHash", receipt.sourceResultHash())
                .put("externalSubjectId", receipt.externalSubjectId()).put("completedBy", receipt.completedBy())
                .put("completedAt", receipt.completedAt().toString()).put("outcome", receipt.outcome())
                .put("summary", receipt.summary()).put("recordState", receipt.recordState())
                .put("previousExternalVersion", receipt.previousExternalVersion())
                .put("resultingExternalVersion", receipt.resultingExternalVersion())
                .put("resultingStatus", receipt.resultingStatus()).put("acceptedAt", receipt.acceptedAt().toString());
        if (receipt.nextAction() == null) result.putNull("nextAction"); else result.put("nextAction", receipt.nextAction());
        var done = finish(current, "VERIFYING", "SUCCEEDED", write(result), null, null, current.policyVersion(), evidence);
        if (!"SUCCEEDED".equals(done.status())) {
            preserveLateEvidence(current.id(), evidence);
            return getInternal(current.id());
        }
        if (countExecution) tasks.recordToolExecution(current.taskId(), current.attempt());
        audit.append(new AuditFact("p27-result-sync-verified:" + current.id(), actor.tenantId(), workspaceId,
                actor.actorId(), current.taskId(), "SERVICE_REQUEST_RESULT_VERIFIED", "SUCCEEDED", evidence, null));
        return done;
    }

    /** 结果 POST 只建立外部意图；只有同 operationId 的 CRM 回读完整匹配后才能报告成功。 */
    private ExecutionSnapshot resolveOutcomeExternal(ActorContext actor, UUID workspaceId,
            ExecutionSnapshot current, NormalizedArguments args, ExternalOutcomeWriteResult writeResult,
            Optional<FollowupOutcomeRecord> readback) {
        var payload = args.outcome();
        if ("REJECTED".equals(writeResult.state())) {
            var done = finish(current, "EXECUTING", "FAILED", null, writeResult.errorCode(),
                    writeResult.detail(), current.policyVersion());
            if ("FAILED".equals(done.status())) customerFollowups.closeSyncAdmission(actor, workspaceId, payload.syncAttemptId(), "FAILED_SAFE");
            return done;
        }
        if (readback == null || readback.isEmpty())
            return finishUnknown(current, "EXECUTING", writeResult.errorCode() == null ? "CRM_OUTCOME_NOT_FOUND" : writeResult.errorCode(),
                    "结果写入的外部效果尚未由 CRM 原 operationId 回读确认。", "{\"found\":false}");
        var verifying = move(current, "EXECUTING", "VERIFYING", null);
        if (!"VERIFYING".equals(verifying.status())) {
            preserveLateEvidence(verifying.id(), write(readback.get()));
            return getInternal(verifying.id());
        }
        return finishOutcomeVerified(actor, workspaceId, verifying, args, readback.get(), true);
    }

    private ExecutionSnapshot finishOutcomeVerified(ActorContext actor, UUID workspaceId,
            ExecutionSnapshot current, NormalizedArguments args, FollowupOutcomeRecord record, boolean countExecution) {
        var payload = args.outcome();
        var evidence = write(record);
        boolean matches = payload != null && current.operationId().toString().equals(record.operationId())
                && payload.externalId().equals(record.externalId()) && payload.customerId().equals(record.customerId())
                && payload.followupId().equals(record.followupId()) && payload.resultId().equals(record.resultId())
                && payload.resultNo() == record.resultNo() && payload.recordedBy().equals(record.recordedBy())
                && payload.outcomeCode().equals(record.outcomeCode()) && payload.summary().equals(record.summary())
                && java.util.Objects.equals(payload.nextAction(), record.nextAction())
                && java.util.Objects.equals(payload.nextContactAt(), record.nextContactAt())
                && payload.disposition().equals(record.disposition()) && "RECORDED".equals(record.status())
                && record.acceptedAt() != null;
        if (!matches)
            return finish(current, "VERIFYING", "VERIFICATION_FAILED", evidence, "CRM_OUTCOME_MISMATCH",
                    "CRM 结果回执与已审批的结果正文或来源不一致。", current.policyVersion(), evidence);
        var result = outcomeTaskJson(record);
        var done = finish(current, "VERIFYING", "SUCCEEDED", result, null, null, current.policyVersion(), evidence);
        if (!"SUCCEEDED".equals(done.status())) {
            preserveLateEvidence(current.id(), evidence);
            return getInternal(current.id());
        }
        if (countExecution) tasks.recordToolExecution(current.taskId(), current.attempt());
        customerFollowups.closeSyncAdmission(actor, workspaceId, payload.syncAttemptId(), "SUCCEEDED");
        audit.append(new AuditFact("execution-outcome-verified:" + current.id(), actor.tenantId(), workspaceId,
                actor.actorId(), current.taskId(), "FOLLOWUP_OUTCOME_VERIFIED", "SUCCEEDED", evidence, null));
        return done;
    }

    private String outcomeTaskJson(FollowupOutcomeRecord record) {
        var node = json.createObjectNode().put("operationId", record.operationId())
                .put("externalId", record.externalId()).put("customerId", record.customerId())
                .put("followupId", record.followupId().toString()).put("resultId", record.resultId().toString())
                .put("resultNo", record.resultNo()).put("recordedBy", record.recordedBy().toString())
                .put("outcomeCode", record.outcomeCode()).put("summary", record.summary())
                .put("disposition", record.disposition()).put("status", record.status())
                .put("acceptedAt", record.acceptedAt().toString());
        node.put("nextAction", record.nextAction() == null ? "" : record.nextAction());
        node.put("nextContactAt", record.nextContactAt() == null ? "" : record.nextContactAt().toString());
        return write(node);
    }

    private ApprovedWriteTarget approvedWriteTarget(ActorContext actor, UUID workspaceId,
                                                     ExecutionSnapshot current, NormalizedArguments args) {
        var authority = jdbc.query("select agent_id, agent_version from execution.execution where id = ?",
                rs -> rs.next() ? new AgentAuthority(rs.getObject("agent_id", UUID.class), rs.getString("agent_version")) : null,
                current.id());
        if (authority == null) throw EafException.conflict("APPROVAL_BINDING_CHANGED", "执行主体快照缺失。");
        var tool = tools.requirePublished(current.tenantId(), workspaceId, current.toolName(), current.toolVersion());
        var previewBinding = previewBindingRef(current);
        if (!"WRITE".equals(tool.effect()) || !tool.bindingRef().equals(previewBinding))
            throw EafException.conflict("APPROVAL_BINDING_CHANGED", "已批准工具绑定与当前版本不一致。");
        var task = tasks.evidence(current.tenantId(), workspaceId, current.taskId()).snapshot();
        var decision = args.serviceRequest() != null
                ? evaluateServiceRequest(actor, workspaceId, authority.agentId(), authority.agentVersion(), tool, task.source())
                : policy.evaluate(new PolicyRequest(actor, workspaceId, authority.agentId(), authority.agentVersion(),
                        current.toolName(), current.toolVersion(), tool.effect(), args.customerId(), task.source()));
        if (!decision.allowed() || !current.policyVersion().equals(decision.policyVersion()))
            throw EafException.forbidden("当前 Policy 不允许核验该业务写入结果。");
        var connector = connectors.requireActiveForTool(current.tenantId(), workspaceId, tool.bindingRef());
        var bindingVersion = connectors.bindingVersion(connector);
        if (!connector.id().equals(current.connectorId()) || !bindingVersion.equals(current.connectorVersion())
                || !bindingVersion.equals(readPreviewConnectorVersion(current)))
            throw EafException.conflict("CONNECTOR_BINDING_CHANGED", "外部目标或凭据用途与获批快照不一致。");
        return new ApprovedWriteTarget(tool, connector);
    }

    private String previewReadBinding(String writeBindingRef) {
        // 写入预览只允许查询同一固定 CRM 契约夹具，不接受模型提供的读取 Connector。
        return switch (writeBindingRef == null ? "" : writeBindingRef) {
            case "test-crm.followup-create" -> "test-crm.customer-read";
            case "p7-crm-write-contract.followup-create" -> "p7-crm-write-contract.customer-read";
            case "p7-crm-write-contract.followup-result" -> "p7-crm-write-contract.customer-read";
            default -> throw EafException.conflict("CONNECTOR_UNAVAILABLE", "写入 Tool 没有固定的客户预览绑定。");
        };
    }

    private String previewBindingRef(ExecutionSnapshot current) {
        try { return json.readTree(current.previewJson()).path("bindingRef").asText(""); }
        catch (Exception invalid) { return ""; }
    }

    private String readPreviewConnectorVersion(ExecutionSnapshot current) {
        try { return json.readTree(current.previewJson()).path("connectorBindingVersion").asText(""); }
        catch (Exception invalid) { return ""; }
    }

    private ExecutionSnapshot finishVerified(ActorContext actor, UUID workspaceId, ExecutionSnapshot current,
                                             NormalizedArguments args, FollowupRecord record, boolean countExecution) {
        var evidence = write(record);
        // 不匹配记录同时保留原始结果和核验凭据，供人工调查但绝不升级为成功。
        if (!current.operationId().toString().equals(record.operationId()) || !args.customerId().equals(record.customerId())
                || !args.summary().equals(record.summary()) || !expectedOwner(current).equals(record.ownerId()))
            return finish(current, "VERIFYING", "VERIFICATION_FAILED", evidence, "CRM_RESULT_MISMATCH", "CRM 返回记录与写入预览不一致。", current.policyVersion(), evidence);
        var done = finish(current, "VERIFYING", "SUCCEEDED", evidence, null, null, current.policyVersion());
        if (!"SUCCEEDED".equals(done.status())) {
            preserveLateEvidence(current.id(), evidence);
            return getInternal(current.id());
        }
        if (countExecution) tasks.recordToolExecution(current.taskId(), current.attempt());
        if ("crm.followup.create".equals(current.toolName())) {
            customerFollowups.recordCreationReceiptForTask(actor, workspaceId, current.taskId(), current.operationId(),
                    record.externalId());
        }
        audit.append(new AuditFact("execution-verified:" + current.id(), actor.tenantId(), workspaceId, actor.actorId(),
                current.taskId(), "WRITE_VERIFIED", "SUCCEEDED", evidence, null));
        return done;
    }

    private String expectedOwner(ExecutionSnapshot current) {
        try { return json.readTree(current.previewJson()).path("ownerId").asText(); }
        catch (Exception e) { return ""; }
    }

    private ExecutionSnapshot finishUnknown(ExecutionSnapshot current, String expectedStatus, String code, String detail, String evidence) {
        return finish(current, expectedStatus, "UNKNOWN", null, code, detail, current.policyVersion(), evidence);
    }

    private void preserveLateEvidence(UUID executionId, String evidence) {
        // 迟到的外部回执只补充 UNKNOWN 证据，不恢复执行权或推进成功状态。
        jdbc.update("update execution.execution set verification_json = ?::jsonb where id = ? and status = 'UNKNOWN'", evidence, executionId);
    }

    @Override
    public ExecutionSnapshot get(ActorContext actor, UUID workspaceId, UUID executionId) {
        workspaces.require(actor, workspaceId, "execution:read");
        return visible(actor, workspaceId, executionId);
    }

    @Override
    public Optional<ExecutionSnapshot> findForTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        workspaces.require(actor, workspaceId, "execution:read");
        if (taskId == null) throw EafException.invalid("Task ID 必填。");
        var id = jdbc.query("select id from execution.execution where task_id = ? and tenant_id = ? and workspace_id = "
                        + "? order by created_at desc, id desc limit 1",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null, taskId, actor.tenantId(), workspaceId);
        return id == null ? Optional.empty() : Optional.of(visible(actor, workspaceId, id));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<ExecutionSnapshot> findP27ResultSyncForTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        workspaces.require(actor, workspaceId, "service-request:result:sync");
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated() || taskId == null)
            throw EafException.forbidden("P27 同步执行状态只对本人 HUMAN 申请人开放。");
        var row = jdbc.query("select id, attempt, arguments_json::text arguments_json from execution.execution "
                        + "where tenant_id = ? and workspace_id = ? and task_id = ? and actor_id = ? "
                        + "and tool_name = 'service.request.result.record' and tool_version = '1.0.0' "
                        + "order by created_at desc limit 1",
                rs -> rs.next() ? new Object[]{rs.getObject("id", UUID.class), rs.getInt("attempt"), rs.getString("arguments_json")} : null,
                actor.tenantId(), workspaceId, taskId, actor.actorId());
        if (row == null) return Optional.empty();
        var execution = getInternal((UUID) row[0]);
        p27Sources.requireToolSource(actor, workspaceId, taskId, (Integer) row[1], execution.toolName(),
                execution.toolVersion(), (String) row[2]);
        return Optional.of(execution);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<ExecutionSnapshot> findServiceRequestRegistration(ActorContext actor, UUID workspaceId,
            UUID taskId, int attempt, UUID submissionId) {
        // Task Owner 先核对本人登记 Task 的原始来源；此只读 API 不扩大普通 execution:read 权限。
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated() || attempt < 1)
            throw EafException.forbidden("只能核验本人直接发起的登记 Execution。");
        tasks.requireServiceRequestWritePayload(actor, workspaceId, taskId, attempt, submissionId);
        var id = jdbc.query("select id from execution.execution where tenant_id = ? and workspace_id = ? and task_id = ? "
                        + "and attempt = ? and actor_id = ? and tool_name = 'service.request.register' and tool_version = '1.0.0' "
                        + "order by created_at desc, id desc limit 1",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null,
                actor.tenantId(), workspaceId, taskId, attempt, actor.actorId());
        return Optional.ofNullable(id).map(this::getInternal);
    }

    @Override
    public ExecutionOperationsPage listOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                                  Instant createdAfter, ExecutionOperationsCursor cursor,
                                                  int pageSize) {
        workspaces.require(actor, workspaceId, "execution:read");
        if (pageSize < 1 || pageSize > 100 || (cursor != null && (cursor.createdAt() == null || cursor.executionId() == null))
                || statuses != null && !OPERATION_STATUSES.containsAll(statuses))
            throw EafException.invalid("Execution 运维列表分页或状态过滤无效。");
        var where = new StringBuilder(" where tenant_id = ? and workspace_id = ?");
        var filters = new java.util.ArrayList<Object>();
        filters.add(actor.tenantId());
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
        var totalSize = jdbc.queryForObject("select count(*) from execution.execution" + where, Long.class, filters.toArray());
        var pageWhere = new StringBuilder(where);
        var pageArgs = new java.util.ArrayList<>(filters);
        if (cursor != null) {
            pageWhere.append(" and (created_at, id) < (?, ?)");
            pageArgs.add(Timestamp.from(cursor.createdAt()));
            pageArgs.add(cursor.executionId());
        }
        pageArgs.add(pageSize + 1);
        // 仅读取支持值班关联与处置判断的列，Execution 参数和 Connector 回执不离开本域存储。
        var selected = jdbc.query("select id, task_id, attempt, tool_name, tool_version, status, error_code, operation_id, approval_id, row_version, created_at, ended_at "
                        + "from execution.execution" + pageWhere + " order by created_at desc, id desc limit ?",
                (rs, row) -> new ExecutionOperationsItem(rs.getObject("id", UUID.class),
                        rs.getObject("task_id", UUID.class), rs.getInt("attempt"), rs.getString("tool_name"),
                        rs.getString("tool_version"), rs.getString("status"), rs.getString("error_code"),
                        rs.getObject("operation_id", UUID.class), rs.getObject("approval_id", UUID.class),
                        rs.getLong("row_version"), rs.getTimestamp("created_at").toInstant(),
                        rs.getTimestamp("ended_at") == null ? null : rs.getTimestamp("ended_at").toInstant()), pageArgs.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        var next = last == null ? null : new ExecutionOperationsCursor(last.createdAt(), last.executionId());
        return new ExecutionOperationsPage(items, totalSize == null ? 0 : totalSize, next);
    }

    @Override
    public ExecutionOutboxPage listOutboxOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                                     Instant createdAfter, Instant cursorCreatedAt,
                                                     UUID cursorEventId, int pageSize) {
        var access = workspaces.require(actor, workspaceId, "execution:read");
        if (pageSize < 1 || pageSize > 100 || (cursorCreatedAt == null) != (cursorEventId == null)
                || statuses != null && !OUTBOX_STATUSES.containsAll(statuses))
            throw EafException.invalid("Execution Outbox 运维分页或状态过滤无效。");
        var where = new StringBuilder(" where tenant_id = ? and workspace_id = ?");
        var filters = new java.util.ArrayList<Object>();
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
        var totalSize = jdbc.queryForObject("select count(*) from execution.outbox" + where, Long.class, filters.toArray());
        var pageWhere = new StringBuilder(where);
        var pageArgs = new java.util.ArrayList<>(filters);
        if (cursorCreatedAt != null) {
            pageWhere.append(" and (created_at, event_id) < (?, ?)");
            pageArgs.add(Timestamp.from(cursorCreatedAt));
            pageArgs.add(cursorEventId);
        }
        pageArgs.add(pageSize + 1);
        // Payload 可能含内部关联和事件快照，运维接口只读 Execution 的安全状态列。
        var selected = jdbc.query("select event_id, execution_id, event_type, status, attempts, next_attempt_at, created_at "
                        + "from execution.outbox" + pageWhere + " order by created_at desc, event_id desc limit ?",
                (rs, row) -> new ExecutionOutboxItem(rs.getObject("event_id", UUID.class),
                        rs.getObject("execution_id", UUID.class), rs.getString("event_type"), rs.getString("status"),
                        rs.getInt("attempts"), rs.getTimestamp("next_attempt_at").toInstant(),
                        rs.getTimestamp("created_at").toInstant()), pageArgs.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        return new ExecutionOutboxPage(items, totalSize == null ? 0 : totalSize,
                last == null ? null : last.createdAt(), last == null ? null : last.eventId());
    }

    @Override
    public ExecutionOutboxReplayReceipt replayOutbox(ActorContext actor, UUID workspaceId, UUID eventId,
                                                     String requestKey, String reason) {
        var normalizedReason = reason == null ? null : reason.strip();
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("Execution Outbox 重投只允许本人 HUMAN 操作者执行。");
        if (eventId == null || requestKey == null || requestKey.isBlank() || requestKey.length() > 200
                || normalizedReason == null || normalizedReason.isBlank() || normalizedReason.length() > 500)
            throw EafException.invalid("Outbox 重投需要目标事件、请求键和 1 至 500 字符的原因。");
        workspaces.require(actor, workspaceId, "execution:outbox:replay");

        var keyHash = Hashing.sha256(String.join("\u001f", actor.tenantId().toString(), workspaceId.toString(), requestKey));
        var requestHash = Hashing.sha256(String.join("\u001f", actor.actorId().toString(), eventId.toString(), normalizedReason));
        return transactions.execute(tx -> {
            var prior = outboxReplayCommand(actor, workspaceId, keyHash);
            if (prior != null) return requireMatchingOutboxReplay(actor, eventId, requestHash, prior);
            var target = jdbc.query("select status, attempts from execution.outbox where event_id = ? and tenant_id = ? "
                            + "and workspace_id = ? for update",
                    rs -> rs.next() ? new OutboxReplayTarget(rs.getString("status"), rs.getInt("attempts")) : null,
                    eventId, actor.tenantId(), workspaceId);
            if (target == null) throw EafException.notFound();
            prior = outboxReplayCommand(actor, workspaceId, keyHash);
            if (prior != null) return requireMatchingOutboxReplay(actor, eventId, requestHash, prior);
            if (!"FAILED".equals(target.status()))
                throw EafException.conflict("OUTBOX_NOT_FAILED", "只有 FAILED 的 Execution Outbox 事件可由运维命令重投。");

            var commandId = UUID.randomUUID();
            var now = Instant.now(clock);
            var inserted = jdbc.update("insert into execution.outbox_replay_command(command_id, tenant_id, workspace_id, event_id, "
                            + "actor_id, request_key_hash, request_hash, reason, result_status, attempts, created_at) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?) "
                            + "on conflict (tenant_id, workspace_id, request_key_hash) do nothing",
                    commandId, actor.tenantId(), workspaceId, eventId, actor.actorId(), keyHash, requestHash,
                    normalizedReason, target.attempts(), Timestamp.from(now));
            if (inserted == 0) {
                prior = outboxReplayCommand(actor, workspaceId, keyHash);
                if (prior == null) throw EafException.conflict("EXECUTION_COMMAND_CONFLICT", "Outbox 请求键已被并发命令占用。");
                return requireMatchingOutboxReplay(actor, eventId, requestHash, prior);
            }

            jdbc.update("update execution.outbox set status = 'PENDING', next_attempt_at = now(), lease_until = null "
                    + "where event_id = ? and status = 'FAILED'", eventId);
            audit.append(new AuditFact("execution-outbox-replay:" + commandId, actor.tenantId(), workspaceId,
                    actor.actorId(), null, "EXECUTION_OUTBOX_REPLAY_REQUESTED", "PENDING",
                    "{\"eventId\":\"" + eventId + "\",\"attempts\":" + target.attempts()
                            + ",\"reasonHash\":\"" + Hashing.sha256(normalizedReason) + "\"}", null));
            return new ExecutionOutboxReplayReceipt(commandId, eventId, "PENDING", target.attempts(), now, false);
        });
    }

    private OutboxReplayCommand outboxReplayCommand(ActorContext actor, UUID workspaceId, String keyHash) {
        return jdbc.query("select command_id, event_id, actor_id, request_hash, result_status, attempts, created_at "
                        + "from execution.outbox_replay_command where tenant_id = ? and workspace_id = ? and request_key_hash = ?",
                rs -> rs.next() ? new OutboxReplayCommand(rs.getObject("command_id", UUID.class),
                        rs.getObject("event_id", UUID.class), rs.getObject("actor_id", UUID.class),
                        rs.getString("request_hash"), rs.getString("result_status"), rs.getInt("attempts"),
                        rs.getTimestamp("created_at").toInstant()) : null,
                actor.tenantId(), workspaceId, keyHash);
    }

    private ExecutionOutboxReplayReceipt requireMatchingOutboxReplay(ActorContext actor, UUID eventId,
                                                                    String requestHash, OutboxReplayCommand prior) {
        if (!prior.eventId().equals(eventId) || !actor.actorId().equals(prior.actorId())
                || !requestHash.equals(prior.requestHash()))
            throw EafException.conflict("EXECUTION_COMMAND_CONFLICT", "同一 Outbox 请求键不能绑定不同事件、操作者或原因。");
        return new ExecutionOutboxReplayReceipt(prior.commandId(), prior.eventId(), prior.status(),
                prior.attempts(), prior.createdAt(), true);
    }

    private ExecutionSnapshot visible(ActorContext actor, UUID workspaceId, UUID id) {
        var result = jdbc.query(select() + " where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? map(rs) : null, id, actor.tenantId(), workspaceId);
        if (result == null) throw EafException.notFound();
        return result;
    }

    private ExecutionSnapshot persistDenied(ExecutionCommand c, String policyVersion, String code, String detail) {
        var existing = existing(c);
        if (existing != null) return existing;
        var id = UUID.randomUUID();
        var args = c.argumentsJson() == null ? "{}" : c.argumentsJson();
        jdbc.update("insert into execution.execution(id, tenant_id, workspace_id, actor_id, task_id, attempt, agent_id, agent_version, tool_name, tool_version, arguments_json, request_hash, idempotency_key, status, policy_version, operation_id, error_code, error_detail, created_at, ended_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, 'DENIED', ?, ?, ?, ?, ?, ?)",
                id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.taskId(), c.attempt(), c.agentId(), c.agentVersion(),
                c.toolName(), c.toolVersion(), args, Hashing.sha256(args), c.idempotencyKey(), policyVersion, id, code, detail,
                Timestamp.from(Instant.now(clock)), Timestamp.from(Instant.now(clock)));
        return getInternal(id);
    }

    private ExecutionSnapshot persistDenied(ExecutionCommand c, String policyVersion, String code, String detail,
                                            NormalizedArguments args) {
        var denied = persistDenied(c, policyVersion, code, detail);
        if (args != null && args.outcome() != null && "DENIED".equals(denied.status()))
            customerFollowups.closeSyncAdmission(c.actor(), c.workspaceId(), args.outcome().syncAttemptId(), "FAILED_SAFE");
        return denied;
    }

    private UUID insertReceived(ExecutionCommand c, String args, String policyVersion) {
        var id = UUID.randomUUID();
        try {
            jdbc.update("insert into execution.execution(id, tenant_id, workspace_id, actor_id, task_id, attempt, agent_id, agent_version, tool_name, tool_version, arguments_json, request_hash, idempotency_key, status, policy_version, operation_id, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, 'RECEIVED', ?, ?, ?)",
                    id, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.taskId(), c.attempt(), c.agentId(), c.agentVersion(),
                    c.toolName(), c.toolVersion(), args, Hashing.sha256(args), c.idempotencyKey(), policyVersion, id, Timestamp.from(Instant.now(clock)));
            return id;
        } catch (DuplicateKeyException duplicate) {
            var same = existing(c);
            if (same != null && sameJson(args, same.argumentsJson())) return same.id();
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一 Execution 幂等键已被其他参数占用。");
        }
    }

    private ExecutionSnapshot move(ExecutionSnapshot current, String expectedStatus, String nextStatus, String result) {
        var now = Instant.now(clock);
        var leaseUntil = "EXECUTING".equals(nextStatus) || "VERIFYING".equals(nextStatus) ? Timestamp.from(now.plusSeconds(30)) : null;
        jdbc.update("update execution.execution set status = ?, result_json = coalesce(?::jsonb, result_json), started_at = case when ? in ('EXECUTING','VERIFYING') then coalesce(started_at, ?) else started_at end, lease_until = ?, row_version = row_version + 1 where id = ? and row_version = ? and status = ?",
                nextStatus, result, nextStatus, Timestamp.from(now), leaseUntil, current.id(), current.version(), expectedStatus);
        return getInternal(current.id());
    }

    private ExecutionSnapshot finish(ExecutionSnapshot current, String expectedStatus, String status, String result,
                                     String code, String detail, String policyVersion) {
        return finish(current, expectedStatus, status, result, code, detail, policyVersion, null);
    }

    private ExecutionSnapshot finish(ExecutionSnapshot current, String expectedStatus, String status, String result,
                                     String code, String detail, String policyVersion, String verification) {
        var ended = Timestamp.from(Instant.now(clock));
        var changed = jdbc.update("update execution.execution set status = ?, result_json = ?::jsonb, verification_json = coalesce(?::jsonb, verification_json), error_code = ?, error_detail = ?, policy_version = ?, ended_at = ?, lease_until = null, row_version = row_version + 1 where id = ? and row_version = ? and status = ?",
                status, result, verification, code, detail, policyVersion, ended, current.id(), current.version(), expectedStatus);
        var done = getInternal(current.id());
        if (changed == 1) {
            if (List.of("UNKNOWN", "SUCCEEDED", "FAILED", "VERIFICATION_FAILED").contains(status))
                tasks.markExternalEffect(current.taskId(), current.attempt(), current.operationId(), status);
            recordOutbox(done);
        }
        return done;
    }

    private void recordOutbox(ExecutionSnapshot execution) {
        if (execution == null) return;
        var eventType = switch (execution.status()) {
            case "SUCCEEDED" -> "eaf.execution.succeeded.v1";
            case "CANCELLED" -> "eaf.execution.cancelled.v1";
            case "UNKNOWN" -> "eaf.execution.uncertain.v1";
            case "VERIFICATION_FAILED" -> "eaf.execution.verification-failed.v1";
            case "AWAITING_APPROVAL" -> "eaf.execution.awaiting-approval.v1";
            case "AWAITING_REMOTE" -> "eaf.execution.awaiting-remote.v1";
            default -> "eaf.execution.failed.v1";
        };
        var task = tasks.evidence(execution.tenantId(), execution.workspaceId(), execution.taskId()).snapshot();
        var remote = getRemoteOperation(execution.id());
        var traceId = remote == null ? task.traceId() : remote.traceId();
        var payloadNode = json.createObjectNode().put("executionId", execution.id().toString())
                .put("operationId", execution.operationId().toString()).put("taskId", task.id().toString())
                .put("rootTaskId", task.rootTaskId().toString()).put("attempt", execution.attempt())
                .put("actorId", task.actorId().toString()).put("entryProtocol", task.entryProtocol())
                .put("traceId", traceId).put("status", execution.status())
                .put("tool", execution.toolName() + "@" + execution.toolVersion());
        if (task.parentTaskId() != null) payloadNode.put("parentTaskId", task.parentTaskId().toString());
        if (remote != null) {
            payloadNode.put("principalId", remote.principalId().toString())
                    .put("delegateId", remote.actorId().equals(remote.principalId()) ? null : remote.actorId().toString())
                    .put("delegationId", remote.delegationId() == null ? null : remote.delegationId().toString())
                    .put("messageId", remote.messageId()).put("remoteCostStatus", "UNKNOWN");
            if (remote.remoteTaskId() != null) payloadNode.put("remoteTaskId", remote.remoteTaskId());
            if (remote.remoteContextId() != null) payloadNode.put("remoteContextId", remote.remoteContextId());
        }
        var payload = write(payloadNode);
        jdbc.update("insert into execution.outbox(event_id, tenant_id, workspace_id, execution_id, event_type, aggregate_version, payload_json) values (?, ?, ?, ?, ?, ?, ?::jsonb) on conflict (execution_id, event_type) do nothing",
                UUID.nameUUIDFromBytes((execution.id() + ":" + eventType).getBytes(java.nio.charset.StandardCharsets.UTF_8)), execution.tenantId(), execution.workspaceId(), execution.id(), eventType, execution.version(), payload);
    }

    private ExecutionSnapshot existing(ExecutionCommand c) {
        return jdbc.query(select() + " where tenant_id = ? and workspace_id = ? and actor_id = ? and task_id = ? and attempt = ? and idempotency_key = ?",
                rs -> rs.next() ? map(rs) : null, c.actor().tenantId(), c.workspaceId(), c.actor().actorId(), c.taskId(), c.attempt(), c.idempotencyKey());
    }

    private ExecutionSnapshot getInternal(UUID id) {
        return jdbc.query(select() + " where id = ?", rs -> rs.next() ? map(rs) : null, id);
    }

    // 通过核对不可变审批绑定阻止批准后改参、超期或更换执行主体/目标。
    private String approvalBindingProblem(ActorContext actor, ExecutionSnapshot current,
                                          ApprovalSnapshot approval, NormalizedArguments args, ExecutionAuthority authority) {
        var binding = approval.binding();
        var argumentHash = Hashing.sha256(args.json());
        if (!approval.id().equals(current.approvalId()) || !current.tenantId().equals(binding.tenantId())
                || !current.workspaceId().equals(binding.workspaceId()) || !actor.actorId().equals(binding.requesterId())
                || !current.id().equals(binding.executionId()) || !current.operationId().toString().equals(binding.operationId())
                || !current.toolName().equals(binding.toolName()) || !current.toolVersion().equals(binding.toolVersion())
                || !current.connectorId().equals(binding.connectorId()) || !current.connectorVersion().equals(binding.connectorVersion())
                || authority == null || !authority.agentId().equals(binding.agentId())
                || !authority.agentVersion().equals(binding.agentVersion())
                || !argumentHash.equals(authority.requestHash()) || !argumentHash.equals(binding.argumentsHash())
                || !sameJson(args.json(), binding.argumentsJson()) || !sameJson(current.previewJson(), binding.previewJson())
                || !current.previewHash().equals(binding.previewHash())
                || !Instant.now(clock).isBefore(binding.expiresAt())) return "APPROVAL_BINDING_CHANGED";
        return null;
    }

    private String select() {
        return "select id, tenant_id, workspace_id, task_id, attempt, tool_name, tool_version, status, arguments_json::text, result_json::text, policy_version, error_code, error_detail, created_at, ended_at, operation_id, connector_id, connector_version, preview_json::text, preview_hash, approval_id, verification_json::text, row_version from execution.execution";
    }

    private ExecutionSnapshot map(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new ExecutionSnapshot(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                rs.getObject("task_id", UUID.class), rs.getInt("attempt"), rs.getString("tool_name"), rs.getString("tool_version"),
                rs.getString("status"), rs.getString("arguments_json"), rs.getString("result_json"), rs.getString("policy_version"),
                rs.getString("error_code"), rs.getString("error_detail"), rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("ended_at") == null ? null : rs.getTimestamp("ended_at").toInstant(), rs.getObject("operation_id", UUID.class),
                rs.getObject("connector_id", UUID.class), rs.getString("connector_version"), rs.getString("preview_json"),
                rs.getString("preview_hash"), rs.getObject("approval_id", UUID.class), rs.getString("verification_json"),
                rs.getLong("row_version"));
    }

    private PolicyDecision evaluate(ExecutionCommand c, ToolDefinition tool, String customerId, String taskSource) {
        return policy.evaluate(new PolicyRequest(c.actor(), c.workspaceId(), c.agentId(), c.agentVersion(),
                c.toolName(), c.toolVersion(), tool.effect(), customerId, taskSource));
    }

    private PolicyDecision evaluateServiceRequest(ActorContext actor, UUID workspaceId, UUID agentId,
            String agentVersion, ToolDefinition tool, String taskSource) {
        return policy.evaluateServiceRequest(new ServiceRequestPolicyRequest(actor, workspaceId, agentId, agentVersion,
                tool.name(), tool.version(), tool.effect(), taskSource));
    }

    private boolean serviceRequestSourcesCurrent(ActorContext actor, UUID workspaceId,
            io.eaf.task.api.ServiceRequestWritePayload payload) {
        if (payload == null || knowledge == null) return false;
        try {
            workspaces.require(actor, workspaceId, "context:read");
            var refs = json.readTree(payload.contextRefsJson());
            if (!refs.isArray() || refs.isEmpty() || refs.size() > 10) return false;
            for (var ref : refs) {
                if (!ref.isObject() || !"KNOWLEDGE".equals(ref.path("sourceType").asText())
                        || !ref.path("documentVersion").canConvertToInt() || ref.path("documentVersion").asInt() < 1
                        || !ref.path("contentHash").asText().matches("[0-9a-f]{64}")) return false;
                var documentId = UUID.fromString(ref.path("documentId").asText());
                var chunkId = UUID.fromString(ref.path("chunkId").asText());
                var buildId = UUID.fromString(ref.path("buildId").asText());
                if (!knowledge.isUsable(actor, workspaceId, documentId, ref.path("documentVersion").asInt(),
                        chunkId, buildId, ref.path("contentHash").asText())) return false;
            }
            return true;
        } catch (Exception unavailable) { return false; }
    }

    private NormalizedArguments normalizeForTask(ExecutionCommand command, ToolDefinition tool) {
        if (isP27Tool(tool.name())) {
            var source = p27Sources.requireToolSource(command.actor(), command.workspaceId(), command.taskId(),
                    command.attempt(), tool.name(), tool.version(), command.argumentsJson());
            return new NormalizedArguments(null, null, command.argumentsJson(), null, null, source);
        }
        if ("service.request.register".equals(tool.name())) {
            var submissionId = serviceRequestSubmissionId(command.argumentsJson());
            var payload = tasks.requireServiceRequestWritePayload(command.actor(), command.workspaceId(),
                    command.taskId(), command.attempt(), submissionId);
            return normalizedServiceRequest(payload);
        }
        if (!"crm.followup.result.record".equals(tool.name())) return normalize(command.argumentsJson(), tool.effect());
        var payload = resultPayloadFromWorkflowInput(command.actor(), command.workspaceId(), command.taskId(),
                command.attempt(), command.argumentsJson());
        return normalizedResult(payload);
    }

    private boolean isP27Tool(String name) {
        return Set.of("oa.todo.list", "oa.todo.get", "service.request.status.get",
                "service.request.result.record").contains(name);
    }

    private NormalizedArguments normalizeStored(ActorContext actor, UUID workspaceId, ExecutionSnapshot execution) {
        if (isP27Tool(execution.toolName())) {
            var source = p27Sources.requireToolSource(actor, workspaceId, execution.taskId(), execution.attempt(),
                    execution.toolName(), execution.toolVersion(), execution.argumentsJson());
            return new NormalizedArguments(null, null, execution.argumentsJson(), null, null, source);
        }
        if ("service.request.register".equals(execution.toolName())) {
            var submissionId = serviceRequestSubmissionId(execution.argumentsJson());
            var payload = tasks.requireServiceRequestWritePayload(actor, workspaceId, execution.taskId(),
                    execution.attempt(), submissionId);
            var normalized = normalizedServiceRequest(payload);
            if (!sameJson(normalized.json(), execution.argumentsJson()))
                throw EafException.conflict("SERVICE_REQUEST_CHANGED", "服务请求提交内容与已批准参数不一致。");
            return normalized;
        }
        if (!"crm.followup.result.record".equals(execution.toolName()))
            return normalize(execution.argumentsJson(), "WRITE");
        var ids = resultIds(execution.argumentsJson());
        var payload = customerFollowups.requireSyncWritePayload(actor, workspaceId, execution.taskId(),
                execution.attempt(), ids.followupId(), ids.resultId());
        var normalized = normalizedResult(payload);
        if (!sameJson(normalized.json(), execution.argumentsJson()))
            throw EafException.conflict("FOLLOWUP_RESULT_CHANGED", "待同步结果、处理人或 CRM 原跟进回执已变化，拒绝沿用旧审批参数。");
        return normalized;
    }

    private CustomerFollowupService.SyncWritePayload resultPayloadFromWorkflowInput(ActorContext actor,
            UUID workspaceId, UUID taskId, int attempt, String raw) {
        try {
            var node = json.readTree(raw);
            if (node == null || !node.isObject() || node.size() != 2 || !node.path("followupId").isTextual()
                    || !node.path("resultId").isTextual()) throw new IllegalArgumentException();
            var followupId = UUID.fromString(node.path("followupId").asText());
            var resultId = UUID.fromString(node.path("resultId").asText());
            return customerFollowups.requireSyncWritePayload(actor, workspaceId, taskId, attempt, followupId, resultId);
        } catch (EafException e) {
            throw e;
        } catch (Exception e) {
            throw EafException.invalid("结果登记工具只接受固定同步 Workflow 提供的两个业务标识。");
        }
    }

    private ResultIds resultIds(String raw) {
        try {
            var node = json.readTree(raw);
            if (node == null || !node.isObject() || !node.path("followupId").isTextual()
                    || !node.path("resultId").isTextual()) throw new IllegalArgumentException();
            return new ResultIds(UUID.fromString(node.path("followupId").asText()),
                    UUID.fromString(node.path("resultId").asText()));
        } catch (Exception e) {
            throw EafException.conflict("FOLLOWUP_RESULT_ARGUMENTS_INVALID", "保存的结果登记参数无法核对。");
        }
    }

    private NormalizedArguments normalizedResult(CustomerFollowupService.SyncWritePayload payload) {
        ObjectNode node = json.createObjectNode();
        node.put("followupId", payload.followupId().toString());
        node.put("resultId", payload.resultId().toString());
        node.put("syncAttemptId", payload.syncAttemptId().toString());
        node.put("customerId", payload.customerId());
        node.put("externalId", payload.externalId());
        node.put("resultNo", payload.resultNo());
        node.put("recordedBy", payload.recordedBy().toString());
        node.put("outcomeCode", payload.outcomeCode());
        node.put("summary", payload.summary());
        if (payload.nextAction() == null) node.putNull("nextAction"); else node.put("nextAction", payload.nextAction());
        if (payload.nextContactAt() == null) node.putNull("nextContactAt"); else node.put("nextContactAt", payload.nextContactAt().toString());
        node.put("disposition", payload.disposition());
        return new NormalizedArguments(payload.customerId(), payload.summary(), write(node), payload, null);
    }

    private NormalizedArguments normalizedServiceRequest(io.eaf.task.api.ServiceRequestWritePayload payload) {
        var node = json.createObjectNode().put("submissionId", payload.submissionId().toString())
                .put("requesterId", payload.requesterId().toString()).put("category", payload.category())
                .put("title", payload.title()).put("summary", payload.summary())
                .put("handlingSuggestion", payload.handlingSuggestion()).put("sourceTaskId", payload.sourceTaskId().toString())
                .put("sourceResultHash", payload.sourceResultHash());
        return new NormalizedArguments(null, payload.summary(), write(node), null, payload);
    }

    private UUID serviceRequestSubmissionId(String raw) {
        try {
            var node = json.readTree(raw);
            if (node == null || !node.isObject() || !node.path("submissionId").isTextual())
                throw new IllegalArgumentException();
            return UUID.fromString(node.path("submissionId").asText());
        } catch (Exception invalid) { throw EafException.invalid("服务请求登记 Tool 只接受有效 submissionId。"); }
    }

    private NormalizedArguments normalize(String raw, String effect) {
        try {
            JsonNode node = json.readTree(raw);
            if (node == null || !node.isObject()) throw new IllegalArgumentException();
            if ("READ".equals(effect) && (node.size() != 1 || !node.has("customerId") || !node.path("customerId").isTextual()))
                throw new IllegalArgumentException();
            if ("WRITE".equals(effect) && (node.size() != 2 || !node.has("customerId") || !node.has("summary")
                    || !node.path("customerId").isTextual() || !node.path("summary").isTextual())) throw new IllegalArgumentException();
            var customerId = node.path("customerId").asText();
            if (customerId.isBlank() || customerId.length() > 160) throw new IllegalArgumentException();
            var normalized = json.createObjectNode();
            normalized.put("customerId", customerId);
            if ("WRITE".equals(effect)) {
                var summary = node.path("summary").asText();
                if (summary.isBlank() || summary.length() > 2_000) throw new IllegalArgumentException();
                normalized.put("summary", summary);
                return new NormalizedArguments(customerId, summary, write(normalized), null, null);
            }
            return new NormalizedArguments(customerId, null, write(normalized), null, null);
        } catch (Exception e) {
            throw EafException.invalid("工具参数包含未知字段、缺少字段或字段值无效。");
        }
    }

    private void validateCommand(ExecutionCommand c) {
        if (c == null || c.actor() == null || c.taskId() == null || c.attempt() <= 0 || c.idempotencyKey() == null || c.idempotencyKey().isBlank())
            throw EafException.invalid("Execution 请求缺少必填字段。");
    }

    private String customerJson(CustomerRecord result) {
        try {
            ObjectNode node = json.createObjectNode();
            node.put("customerId", result.customerId()); node.put("renewalStatus", result.renewalStatus());
            node.put("lastContactDate", result.lastContactDate()); node.put("complaintSummary", result.complaintSummary());
            node.put("sourceId", result.sourceId()); node.put("readAt", result.readAt().toString());
            if (result.externalVersion() != null) node.put("externalVersion", result.externalVersion());
            return write(node);
        } catch (Exception e) { throw EafException.conflict("INVALID_TOOL_RESULT", "测试 CRM 返回字段无法保存。"); }
    }

    private String write(Object value) {
        try { return json.writeValueAsString(value); }
        catch (Exception e) { throw EafException.conflict("JSON_SERIALIZATION_FAILED", "执行证据无法序列化。"); }
    }

    private boolean sameJson(String left, String right) {
        try { return json.readTree(left).equals(json.readTree(right)); }
        catch (Exception e) { return java.util.Objects.equals(left, right); }
    }

    private record ResultIds(UUID followupId, UUID resultId) { }
    private record NormalizedArguments(String customerId, String summary, String json,
                                       CustomerFollowupService.SyncWritePayload outcome,
                                       io.eaf.task.api.ServiceRequestWritePayload serviceRequest,
                                       P27BusinessTaskSource p27) {
        private NormalizedArguments(String customerId, String summary, String json,
                                    CustomerFollowupService.SyncWritePayload outcome,
                                    io.eaf.task.api.ServiceRequestWritePayload serviceRequest) {
            this(customerId, summary, json, outcome, serviceRequest, null);
        }
    }
    private record RemoteArguments(String customerId, String riskSummary, String json) { }
    private record RemoteBinding(RemoteAgentRegistration registration, CapabilityDefinition capability,
                                 ConnectorDefinition connector) { }
    private record RemoteIntent(ExecutionSnapshot execution, boolean created) { }
    private record RemotePollClaim(ExecutionSnapshot execution, RemoteOperation operation, int pollNumber,
                                   boolean shouldCall) { }
    private record RemoteCancelClaim(ExecutionSnapshot execution, RemoteOperation operation, boolean shouldCall) { }
    private record RemoteTaskPayload(String taskId, String contextId, String state, String json) { }
    private record RemoteOperation(UUID tenantId, UUID workspaceId, UUID taskId, int attempt, UUID actorId,
                                   UUID principalId, UUID delegationId, String authorizationHash, UUID rootTaskId,
                                   String operationKey, String messageId, String requestHash, UUID registrationId,
                                   UUID capabilityId, String capabilityVersion, UUID connectorId, String peerSkillId,
                                   String state, String remoteTaskId, String remoteContextId, int pollsUsed,
                                   int pollLimit, Instant nextPollAt, Instant pollDeadline, String traceId, long rowVersion) { }
    private record DueRemoteTask(UUID executionId, UUID tenantId, UUID workspaceId, UUID taskId, int attempt,
                                 UUID operationId, int pollsUsed, int pollLimit, Instant pollDeadline) { }
    private record ExecutionAuthority(UUID agentId, String agentVersion, String requestHash) { }
    private record VerifyTarget(UUID operationId, String status, long version) { }
    private record VerifyCommand(UUID commandId, UUID executionId, UUID operationId, UUID actorId,
                                 String requestHash, boolean completed, String resultStatus,
                                 Long resultVersion, Instant createdAt, boolean replayed) { }
    private record OutboxReplayTarget(String status, int attempts) { }
    private record OutboxReplayCommand(UUID commandId, UUID eventId, UUID actorId, String requestHash,
                                       String status, int attempts, Instant createdAt) { }
    private record AgentAuthority(UUID agentId, String agentVersion) { }
    private record ApprovedWriteTarget(ToolDefinition tool, ConnectorDefinition connector) { }
    private record ResumeAttempt(ExecutionSnapshot snapshot, boolean claimed) { }
}
