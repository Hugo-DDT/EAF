package io.eaf.learning.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.eaf.context.api.ContextSourceRef;
import io.eaf.context.api.ContextItem;
import io.eaf.context.api.EnterpriseContext;
import io.eaf.context.api.TeamExperienceUsage;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.evaluation.api.CandidateEvaluationReport;
import io.eaf.evaluation.api.CandidateContextSnapshotCommand;
import io.eaf.evaluation.api.EvaluationService;
import io.eaf.evaluation.api.CandidateContextSnapshotCommand.TeamPreparationSnapshotBinding;
import io.eaf.learning.api.CandidateApproval;
import io.eaf.learning.api.CandidateIteration;
import io.eaf.learning.api.CandidateRelease;
import io.eaf.learning.api.CandidateWithdrawal;
import io.eaf.knowledge.api.CreateKnowledgeVersionCommand;
import io.eaf.knowledge.api.KnowledgeDocument;
import io.eaf.knowledge.api.KnowledgePublication;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.learning.api.CandidateProposalResult;
import io.eaf.learning.api.CandidateReview;
import io.eaf.learning.api.CandidateService;
import io.eaf.learning.api.CandidateSourceResult;
import io.eaf.learning.api.CandidateSubmission;
import io.eaf.learning.api.Feedback;
import io.eaf.learning.api.LearningCandidate;
import io.eaf.memory.api.MemoryDefinition;
import io.eaf.memory.api.MemoryCandidateReleaseCommand;
import io.eaf.memory.api.MemoryRelease;
import io.eaf.memory.api.MemoryService;
import io.eaf.memory.api.TeamExperienceService;
import io.eaf.evaluation.api.EvaluationService.TeamImprovementGenerationBinding;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.task.api.CreateQualityRunTaskCommand;
import io.eaf.usage.api.UsageRecorder;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

@Service
public class JdbcCandidateService implements CandidateService {
    private static final int MAX_RULE_TARGETS = 10;
    private static final Set<String> MEMORY_FIELDS = Set.of("logicalKey", "type", "scope", "content", "confidence",
            "expiresAt", "sourceRef", "evidenceRefs", "businessEntityType", "businessEntityId");
    private static final Pattern SECRET = Pattern.compile("(?i)(?:api[_-]?key|secret|password|token)\\s*[:=]\\s*\\S{8,}|-----BEGIN (?:RSA |EC )?PRIVATE KEY-----|AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{20,}");

    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final TaskService tasks;
    private final RuntimeQuery runtime;
    private final KnowledgeService knowledge;
    private final MemoryService memories;
    private final TeamExperienceService teamExperiences;
    private final EvaluationService evaluations;
    private final UsageRecorder usage;
    private final ObjectMapper json;
    private final Clock clock;
    private final TransactionTemplate transactions;
    private final boolean improvementEnabled;
    private final String modelMode;

    public JdbcCandidateService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces, TaskService tasks,
                                RuntimeQuery runtime,
                                KnowledgeService knowledge, MemoryService memories, TeamExperienceService teamExperiences,
                                EvaluationService evaluations, UsageRecorder usage,
                                ObjectMapper json, Clock clock, PlatformTransactionManager transactionManager,
                                @Value("${eaf.learning.improvement.enabled:false}") boolean improvementEnabled,
                                @Value("${eaf.model.mode:deterministic}") String modelMode) {
        this.jdbc = jdbc; this.workspaces = workspaces; this.tasks = tasks; this.runtime = runtime; this.knowledge = knowledge;
        this.memories = memories; this.teamExperiences = teamExperiences;
        this.evaluations = evaluations; this.usage = usage; this.json = json; this.clock = clock;
        this.transactions = new TransactionTemplate(transactionManager);
        this.improvementEnabled = improvementEnabled; this.modelMode = modelMode;
    }

    @Override
    @Transactional
    public CandidateProposalResult proposeFromFeedback(ActorContext actor, UUID workspaceId, UUID feedbackId) {
        requireActor(actor, workspaceId, "learning:propose");
        var feedback = requireFeedback(actor, workspaceId, feedbackId);
        var refs = new LinkedHashMap<String, ContextSourceRef>();
        for (var ref : feedback.source().contextSources() == null ? List.<ContextSourceRef>of() : feedback.source().contextSources()) {
            var type = sourceType(ref);
            var targetId = sourceTargetId(ref);
            if (type != null && targetId != null) refs.putIfAbsent(type + ":" + targetId, ref);
        }
        var candidates = new ArrayList<LearningCandidate>();
        var outcomes = new ArrayList<CandidateSourceResult>();
        int count = 0;
        for (var entry : refs.entrySet()) {
            var ref = entry.getValue();
            var type = sourceType(ref);
            var targetId = sourceTargetId(ref);
            if (!claimSource(actor, workspaceId, feedbackId, type, targetId)) {
                var existing = existingSourceOutcome(actor, workspaceId, feedbackId, type, targetId);
                outcomes.add(existing);
                if (existing.candidateId() != null) candidates.add(get(actor, workspaceId, existing.candidateId()));
                continue;
            }
            if (count++ >= MAX_RULE_TARGETS) {
                outcomes.add(skipSource(actor, workspaceId, feedbackId, type, targetId, "BATCH_LIMIT"));
                continue;
            }
            try {
                var draft = fromFeedback(actor, workspaceId, feedback, ref);
                var key = "feedback:" + feedbackId + ":" + type + ":" + targetId;
                var candidate = insertCandidate(actor, workspaceId, feedbackId, key, draft);
                jdbc.update("update learning.candidate_source set outcome = 'CREATED', candidate_id = ?, processed_at = ? where tenant_id = ? and workspace_id = ? and feedback_id = ? and target_type = ? and target_id = ?",
                        candidate.candidate().id(), Timestamp.from(Instant.now(clock)), actor.tenantId(), workspaceId, feedbackId, type, targetId);
                candidates.add(candidate.candidate());
                outcomes.add(new CandidateSourceResult(type, targetId, candidate.created() ? "CREATED" : "REPLAYED", null, candidate.candidate().id()));
            } catch (EafException unavailable) {
                outcomes.add(skipSource(actor, workspaceId, feedbackId, type, targetId, unavailable.code()));
            }
        }
        return new CandidateProposalResult(List.copyOf(candidates), List.copyOf(outcomes));
    }

    @Override
    @Transactional
    public CandidateService.ImprovementRunSubmission createImprovementRun(CandidateService.ImprovementRunCommand command) {
        if (command == null || command.actor() == null || command.actor().type() != ActorType.HUMAN
                || command.actor().delegated() || command.workspaceId() == null || command.cardId() == null
                || command.baseRevision() < 1 || command.expectedCardVersion() < 1
                || command.idempotencyKey() == null || command.idempotencyKey().isBlank()
                || command.idempotencyKey().length() > 200 || hasControl(command.idempotencyKey()))
            throw EafException.invalid("启动需要直接 HUMAN Owner、有效目标版本和 Idempotency-Key。");
        var actor = command.actor();
        for (var action : List.of("learning:propose", "learning:read", "task:create", "task:cancel",
                "evaluation:run", "memory:read", "memory:write", "memory:scope:team"))
            workspaces.require(actor, command.workspaceId(), action);
        if (!improvementEnabled)
            throw EafException.conflict("LEARNING_IMPROVEMENT_DISABLED", "当前关闭新改进运行接纳。");
        if (!"deterministic".equalsIgnoreCase(modelMode))
            throw EafException.conflict("LEARNING_IMPROVEMENT_MODEL_MODE_UNSUPPORTED", "当前只接纳 deterministic 运行，不启动未授权的 Provider 费用。");
        if (!"service-request-preparation".equals(command.datasetKey()) || !"1.0.0".equals(command.datasetVersion()))
            throw EafException.invalid("仅支持固定 service-request-preparation@1.0.0 数据集。");
        if (command.sourceFeedbackIds() == null || command.sourceFeedbackIds().isEmpty()
                || command.sourceFeedbackIds().size() > 5
                || new LinkedHashSet<>(command.sourceFeedbackIds()).size() != command.sourceFeedbackIds().size())
            throw EafException.invalid("来源反馈必须是 1 到 5 个唯一 ID。");
        var correction = command.sharedCorrection();
        if (blank(correction) || correction.length() > 4_000 || correction.chars().anyMatch(ch -> ch < 0x20 && ch != '\n' && ch != '\r' && ch != '\t' || ch == 0x7f)
                || isSensitive(correction))
            throw EafException.invalid("sharedCorrection 需要 1 到 4000 字符，并且不能含控制字符或疑似凭证。");
        var now = Instant.now(clock);
        // PostgreSQL timestamptz 按微秒保存；创建和后续绑定比较统一精度，避免纳秒舍入导致登记不匹配。
        var deadline = (command.deadlineAt() == null ? now.plus(Duration.ofHours(24)) : command.deadlineAt())
                .truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        if (!deadline.isAfter(now) || deadline.isAfter(now.plus(Duration.ofDays(7))))
            throw EafException.invalid("截止时间必须在当前时间之后且不超过 7 天。");

        var card = teamExperiences.get(actor, command.workspaceId(), command.cardId());
        if (!actor.actorId().equals(card.ownerId()) || card.version() != command.expectedCardVersion()
                || card.latestRevision() != command.baseRevision()
                || !Integer.valueOf(command.baseRevision()).equals(card.activeRevision())
                || card.active() == null || !"PUBLISHED".equals(card.active().status())
                || !card.active().expiresAt().isAfter(deadline))
            throw EafException.conflict("TEAM_EXPERIENCE_BASE_CHANGED", "目标必须是本人当前有效且截止时间内有效的已发布 TEAM 卡。");
        var base = card.active();
        var sourceRefs = new ArrayList<CandidateService.ImprovementSourceRef>();
        var promptRefs = json.createArrayNode();
        for (var feedbackId : command.sourceFeedbackIds()) {
            var feedback = requireFeedback(actor, command.workspaceId(), feedbackId);
            if (!actor.actorId().equals(feedback.actorId())) throw EafException.notFound();
            var task = tasks.get(actor, command.workspaceId(), feedback.taskId());
            var evidence = tasks.evidence(actor.tenantId(), command.workspaceId(), task.id());
            if (task.status() != TaskStatus.SUCCEEDED || task.attempt() != feedback.taskAttempt()
                    || evidence.qualityRunId() != null || !"USER".equals(task.source()))
                throw EafException.conflict("IMPROVEMENT_SOURCE_INVALID", "只接受本人成功的普通 USER Task 反馈及当前 attempt。");
            var used = runtime.teamExperienceUsage(actor, command.workspaceId(), task.id()).included().stream()
                    .filter(item -> item.cardId().equals(card.id()) && item.revision() == command.baseRevision()
                            && item.memoryVersion().equals(base.memoryVersion()) && item.contentHash().equals(base.contentHash()))
                    .findFirst().orElseThrow(() -> EafException.conflict("IMPROVEMENT_SOURCE_CARD_UNUSED",
                            "来源 Task 未实际带入所选 TEAM 卡的当前版本。"));
            var contextHash = Hashing.sha256(writeJson(feedback.source()));
            sourceRefs.add(new CandidateService.ImprovementSourceRef(feedback.id(), task.id(), task.attempt(),
                    contextHash, used.revision(), used.memoryVersion(), used.contentHash()));
            promptRefs.addObject().put("feedbackId", feedback.id().toString()).put("taskId", task.id().toString())
                    .put("attempt", task.attempt()).put("contextHash", contextHash)
                    .put("cardRevision", used.revision()).put("memoryVersion", used.memoryVersion())
                    .put("contentHash", used.contentHash());
        }
        var requestHash = Hashing.sha256(writeJson(new ImprovementRunRequestHash(card.id(), command.baseRevision(),
                command.expectedCardVersion(), sourceRefs, correction, command.datasetKey(), command.datasetVersion(), deadline)));
        var keyHash = Hashing.sha256(command.idempotencyKey());
        var existing = jdbc.query("select id, request_hash from learning.improvement_run where tenant_id = ? and workspace_id = ? "
                        + "and owner_id = ? and idempotency_key_hash = ? for update",
                rs -> rs.next() ? new ImprovementRunKey(rs.getObject("id", UUID.class), rs.getString("request_hash")) : null,
                actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (existing != null) {
            if (!requestHash.equals(existing.requestHash()))
                throw EafException.conflict("IMPROVEMENT_IDEMPOTENCY_CONFLICT", "启动键已绑定不同来源或输入。");
            return new CandidateService.ImprovementRunSubmission(loadImprovementRun(actor, command.workspaceId(), existing.id()), false);
        }

        var input = json.createObjectNode();
        input.putObject("card").put("cardId", card.id().toString())
                .put("revision", base.revision()).put("title", base.title())
                .put("appliesWhen", base.appliesWhen()).put("content", base.content())
                .put("memoryVersion", base.memoryVersion()).put("contentHash", base.contentHash());
        input.put("sharedCorrection", correction);
        input.set("sourceRefs", promptRefs);
        var inputText = writeJson(input);
        if (inputText.length() > 8_000) throw EafException.invalid("固定生成输入超过 Task 长度限制。");
        var inputHash = Hashing.sha256(inputText);
        var id = UUID.randomUUID();
        var sourceJson = writeJson(sourceRefs);
        jdbc.update("insert into learning.improvement_run(id, tenant_id, workspace_id, owner_id, card_id, base_revision, "
                        + "base_memory_version, expected_card_version, source_feedbacks, shared_correction, shared_correction_hash, "
                        + "generation_input_hash, model_mode, dataset_key, dataset_version, request_hash, idempotency_key_hash, "
                        + "status, deadline_at, row_version, next_poll_at, created_at, updated_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, 'DETERMINISTIC', ?, ?, ?, ?, 'CREATED', ?, 1, ?, ?, ?)",
                id, actor.tenantId(), command.workspaceId(), actor.actorId(), card.id(), base.revision(), base.memoryVersion(),
                card.version(), sourceJson, correction, Hashing.sha256(correction), inputHash, command.datasetKey(),
                command.datasetVersion(), requestHash, keyHash, Timestamp.from(deadline), Timestamp.from(now),
                Timestamp.from(now), Timestamp.from(now));
        var registration = evaluations.registerTeamImprovementGeneration(actor, command.workspaceId(), id, requestHash, inputHash, deadline);
        var task = tasks.createQualityRunTask(new CreateQualityRunTaskCommand(actor, command.workspaceId(),
                registration.registration().id(), registration.agentId(), registration.agentVersion(), inputText,
                "TEAM_EXPERIENCE_IMPROVEMENT", id.toString(), registration.taskKey(), "p23-" + id,
                "EVALUATION", null, registration.assetBinding(), deadline));
        jdbc.update("update learning.improvement_run set quality_run_id = ?, generation_task_id = ?, status = 'GENERATING', "
                        + "row_version = row_version + 1, updated_at = now() where id = ? and status = 'CREATED'",
                registration.registration().id(), task.id(), id);
        return new CandidateService.ImprovementRunSubmission(loadImprovementRun(actor, command.workspaceId(), id), true);
    }

    @Override
    @Transactional(readOnly = true)
    public CandidateService.ImprovementRun getImprovementRun(ActorContext actor, UUID workspaceId, UUID improvementRunId) {
        requireActor(actor, workspaceId, "learning:read");
        return loadImprovementRun(actor, workspaceId, improvementRunId);
    }

    @Override
    @Transactional
    public CandidateService.ImprovementRun stopImprovementRun(ActorContext actor, UUID workspaceId,
            UUID improvementRunId, long expectedVersion) {
        if (expectedVersion < 1 || actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.invalid("停止运行需要直接 HUMAN 身份和有效 expectedVersion。");
        requireActor(actor, workspaceId, "learning:read");
        requireActor(actor, workspaceId, "learning:propose");
        requireActor(actor, workspaceId, "task:cancel");
        var run = jdbc.query("select owner_id, quality_run_id, generation_task_id, dev_report_id, held_out_report_id, status, row_version, stop_requested "
                        + "from learning.improvement_run where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? new ImprovementRunStop(rs.getObject("owner_id", UUID.class),
                        rs.getObject("quality_run_id", UUID.class), rs.getObject("generation_task_id", UUID.class),
                        rs.getObject("dev_report_id", UUID.class), rs.getObject("held_out_report_id", UUID.class),
                        rs.getString("status"), rs.getLong("row_version"), rs.getBoolean("stop_requested")) : null,
                improvementRunId, actor.tenantId(), workspaceId);
        if (run == null || !actor.actorId().equals(run.ownerId())) throw EafException.notFound();
        if (run.rowVersion() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "运行版本已变化。");
        if (List.of("PUBLISHED", "NO_CHANGE", "NOT_SELECTED", "REJECTED", "STOPPED", "TIMED_OUT", "FAILED", "STALE").contains(run.status()))
            return loadImprovementRun(actor, workspaceId, improvementRunId);
        jdbc.update("update learning.improvement_run set stop_requested = true, stop_reason = coalesce(stop_reason, 'HUMAN_STOP'), "
                        + "status = 'STOPPING', row_version = row_version + 1, next_poll_at = now(), updated_at = now() "
                        + "where id = ? and row_version = ?", improvementRunId, expectedVersion);
        if (run.qualityRunId() != null)
            usage.stopSpendScope(actor.tenantId(), workspaceId, "EVALUATION", run.qualityRunId(), "HUMAN_STOP");
        var owner = internalHuman(actor.tenantId(), run.ownerId(), workspaceId);
        if (run.devReportId() != null) evaluations.stopTeamPreparationEvaluation(owner, workspaceId, run.devReportId(), "HUMAN_STOP");
        if (run.heldOutReportId() != null) evaluations.stopTeamPreparationEvaluation(owner, workspaceId, run.heldOutReportId(), "HUMAN_STOP");
        if (run.generationTaskId() == null) {
            jdbc.update("update learning.improvement_run set next_poll_at = now(), updated_at = now() where id = ? and status = 'STOPPING'",
                    improvementRunId);
        } else {
            try {
                var task = tasks.getTeamImprovementGenerationTask(actor, workspaceId, run.generationTaskId());
                if (!terminal(task.status())) tasks.cancelTeamImprovementGenerationTask(actor, workspaceId,
                        task.id(), task.version());
            } catch (EafException gone) {
                if (gone.status() != 404) throw gone;
            }
        }
        return loadImprovementRun(actor, workspaceId, improvementRunId);
    }

    /** 每 tick 只读取一个已登记生成 Task；不等待模型，Task 的租约仍由 Task/Runtime Owner 管理。 */
    @Scheduled(fixedDelayString = "${eaf.learning.improvement.poll-delay-ms:1000}")
    public void dispatchNextImprovementGeneration() {
        var selectedId = new UUID[1];
        try {
            transactions.execute(status -> {
                var run = jdbc.query("select id, tenant_id, workspace_id, owner_id, quality_run_id, generation_task_id, dev_report_id, held_out_report_id, "
                                + "status, stop_requested, deadline_at from learning.improvement_run "
                                + "where status in ('GENERATING','STOPPING') and next_poll_at <= now() "
                                + "order by next_poll_at, created_at for update skip locked limit 1",
                        rs -> rs.next() ? new GenerationPoll(rs.getObject("id", UUID.class),
                                rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                                rs.getObject("owner_id", UUID.class), rs.getObject("quality_run_id", UUID.class),
                                rs.getObject("generation_task_id", UUID.class), rs.getObject("dev_report_id", UUID.class),
                                rs.getObject("held_out_report_id", UUID.class), rs.getString("status"),
                                rs.getBoolean("stop_requested"), rs.getTimestamp("deadline_at").toInstant()) : null);
                if (run == null) return null;
                selectedId[0] = run.id();
                var now = Instant.now(clock);
                if (run.generationTaskId() == null) {
                    jdbc.update("update learning.improvement_run set status = case when stop_requested then 'STOPPED' else 'FAILED' end, "
                                    + "stop_reason = coalesce(stop_reason, 'GENERATION_TASK_MISSING'), row_version = row_version + 1, updated_at = now() where id = ?",
                            run.id());
                    return null;
                }
                var actor = new ActorContext(run.ownerId(), run.tenantId(), ActorType.HUMAN,
                        workspaces.actions(run.tenantId(), run.ownerId(), run.workspaceId()));
                if (run.stopRequested()) {
                    var task = tasks.getTeamImprovementGenerationTask(actor, run.workspaceId(), run.generationTaskId());
                    if (terminal(task.status()) && run.devReportId() == null && run.heldOutReportId() == null)
                        jdbc.update("update learning.improvement_run set status = 'STOPPED', row_version = row_version + 1, updated_at = now() where id = ? and status = 'STOPPING'",
                                run.id());
                    else jdbc.update("update learning.improvement_run set next_poll_at = now() + interval '3 seconds', updated_at = now() where id = ?",
                            run.id());
                    return null;
                }
                if (!run.deadlineAt().isAfter(now)) {
                    usage.stopSpendScope(run.tenantId(), run.workspaceId(), "EVALUATION", run.qualityRunId(), "DEADLINE_EXCEEDED");
                    jdbc.update("update learning.improvement_run set status = 'TIMED_OUT', stop_reason = 'DEADLINE_EXCEEDED', "
                                    + "stop_requested = true, row_version = row_version + 1, updated_at = now() where id = ? and status = 'GENERATING'",
                            run.id());
                    return null;
                }
                var task = tasks.getTeamImprovementGenerationTask(actor, run.workspaceId(), run.generationTaskId());
                if (!terminal(task.status())) {
                    jdbc.update("update learning.improvement_run set next_poll_at = now() + interval '3 seconds', updated_at = now() where id = ?",
                            run.id());
                    return null;
                }
                if (task.status() == TaskStatus.SUCCEEDED) {
                    proposeFromImprovementRun(actor, run.workspaceId(), run.id());
                } else {
                    var result = task.status() == TaskStatus.TIMED_OUT ? "TIMED_OUT" : "FAILED";
                    var reason = task.errorCode() == null ? result : task.errorCode();
                    if (run.qualityRunId() != null)
                        usage.stopSpendScope(run.tenantId(), run.workspaceId(), "EVALUATION", run.qualityRunId(), "TASK_FAILED");
                    jdbc.update("update learning.improvement_run set status = ?, stop_reason = ?, stop_requested = true, "
                                    + "row_version = row_version + 1, updated_at = now() where id = ? and status = 'GENERATING'",
                            result, reason, run.id());
                }
                return null;
            });
        } catch (EafException failure) {
            if (selectedId[0] == null) return;
            var status = failure.status() == 403 || failure.status() == 404 || failure.status() == 409 ? "STALE" : "FAILED";
            try {
                transactions.execute(tx -> {
                    jdbc.update("update learning.improvement_run set status = ?, stop_reason = ?, stop_requested = true, "
                                    + "row_version = row_version + 1, updated_at = now() where id = ? and status in ('GENERATING','STOPPING')",
                            status, failure.code(), selectedId[0]);
                    return null;
                });
            } catch (RuntimeException ignored) { }
        }
    }

    @Scheduled(fixedDelayString = "${eaf.learning.improvement-evaluation-poll-delay-ms:1000}")
    public void dispatchNextImprovementEvaluation() {
        try {
            transactions.executeWithoutResult(status -> {
                var run = jdbc.query("select id, tenant_id, workspace_id, owner_id, quality_run_id, generation_task_id, "
                                + "candidate_id, candidate_revision, dev_report_id, held_out_report_id, status, stop_requested, deadline_at "
                                + "from learning.improvement_run where status in ('DEV_EVALUATING','HELD_OUT_EVALUATING','AWAITING_EVIDENCE','STOPPING') "
                                + "and next_poll_at <= now() order by next_poll_at, created_at for update skip locked limit 1",
                        rs -> rs.next() ? new ImprovementEvaluationPoll(rs.getObject("id", UUID.class),
                                rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                                rs.getObject("owner_id", UUID.class), rs.getObject("quality_run_id", UUID.class),
                                rs.getObject("generation_task_id", UUID.class), rs.getObject("candidate_id", UUID.class),
                                rs.getObject("candidate_revision", Integer.class), rs.getObject("dev_report_id", UUID.class),
                                rs.getObject("held_out_report_id", UUID.class), rs.getString("status"),
                                rs.getBoolean("stop_requested"), rs.getTimestamp("deadline_at").toInstant()) : null);
                if (run == null) return;
                advanceImprovementEvaluation(run);
            });
        } catch (RuntimeException ignored) {
            // 下次 tick 仍按同一 run/report ID 恢复；不创建新的候选或预算范围。
        }
    }

    private void advanceImprovementEvaluation(ImprovementEvaluationPoll run) {
        var owner = internalHuman(run.tenantId(), run.ownerId(), run.workspaceId());
        var now = Instant.now(clock);
        if (run.stopRequested() || "STOPPING".equals(run.status()) || !run.deadlineAt().isAfter(now)) {
            var reason = run.deadlineAt().isAfter(now) ? "HUMAN_STOP" : "DEADLINE_EXCEEDED";
            stopImprovementEvaluationReports(owner, run, reason);
            if (run.generationTaskId() != null) {
                try {
                    var generation = tasks.getTeamImprovementGenerationTask(owner, run.workspaceId(), run.generationTaskId());
                    if (!terminal(generation.status())) { deferImprovementEvaluation(run.id()); return; }
                } catch (RuntimeException unavailable) { }
            }
            var finalStatus = "DEADLINE_EXCEEDED".equals(reason) ? "TIMED_OUT" : "STOPPED";
            jdbc.update("update learning.improvement_run set status = ?, stop_reason = coalesce(stop_reason, ?), "
                            + "row_version = row_version + 1, next_poll_at = now() + interval '1 day', updated_at = now() "
                            + "where id = ? and status = ?", finalStatus, reason, run.id(), run.status());
            return;
        }
        try {
            requireCurrentImprovementTarget(owner, run.workspaceId(), run.id());
            if ("DEV_EVALUATING".equals(run.status())) {
                if (run.devReportId() == null) throw EafException.conflict("DEV_REPORT_MISSING", "DEV 报告 ID 缺失。");
                var report = evaluations.getTeamPreparationRun(owner, run.workspaceId(), run.devReportId());
                if (!terminalPreparation(report.status())) { deferImprovementEvaluation(run.id()); return; }
                if (!"COMPLETED".equals(report.status()) || report.failedPairs() != 0
                        || report.completedPairs() != report.plannedPairs()) {
                    closeImprovementRun(run.id(), "NOT_SELECTED", "DEV_EVALUATION_FAILED"); return;
                }
                var snapshotId = teamCandidateSnapshotId(owner, run.workspaceId(), get(owner, run.workspaceId(), run.candidateId()));
                var heldOut = evaluations.startTeamPreparationEvaluation(owner, run.workspaceId(), run.candidateId(),
                        run.candidateRevision(), snapshotId, run.id(), run.qualityRunId(), "HELD_OUT", run.deadlineAt());
                jdbc.update("update learning.improvement_run set status = 'HELD_OUT_EVALUATING', held_out_report_id = ?, "
                                + "row_version = row_version + 1, next_poll_at = now(), updated_at = now() "
                                + "where id = ? and status = 'DEV_EVALUATING' and dev_report_id = ?",
                        heldOut.reportId(), run.id(), run.devReportId());
                return;
            }
            if ("HELD_OUT_EVALUATING".equals(run.status())) {
                if (run.heldOutReportId() == null) throw EafException.conflict("HELD_OUT_REPORT_MISSING", "保留集报告 ID 缺失。");
                var report = evaluations.getTeamPreparationRun(owner, run.workspaceId(), run.heldOutReportId());
                if (!terminalPreparation(report.status())) { deferImprovementEvaluation(run.id()); return; }
                if (!"COMPLETED".equals(report.status()) || report.failedPairs() != 0
                        || report.completedPairs() != report.plannedPairs()) {
                    closeImprovementRun(run.id(), "NOT_SELECTED", "HELD_OUT_EVALUATION_FAILED"); return;
                }
                jdbc.update("update learning.improvement_run set status = 'AWAITING_EVIDENCE', row_version = row_version + 1, "
                                + "next_poll_at = now(), updated_at = now() where id = ? and status = 'HELD_OUT_EVALUATING'",
                        run.id());
                return;
            }
            if ("AWAITING_EVIDENCE".equals(run.status())) {
                var evidence = evaluations.getTeamPreparationReleaseEvidence(owner, run.workspaceId(),
                        run.candidateId(), run.candidateRevision());
                if (!evidence.current()) { closeImprovementRun(run.id(), "STALE", "EVALUATION_SOURCE_UNAVAILABLE"); return; }
                if ("ELIGIBLE".equals(evidence.eligibility())) {
                    jdbc.update("update learning.improvement_run set status = 'READY', row_version = row_version + 1, "
                                    + "next_poll_at = now() + interval '1 day', updated_at = now() where id = ? and status = 'AWAITING_EVIDENCE'",
                            run.id());
                } else if ("NOT_ELIGIBLE".equals(evidence.eligibility())) {
                    closeImprovementRun(run.id(), "NOT_SELECTED", evidence.reasonCode());
                } else deferImprovementEvaluation(run.id());
            }
        } catch (EafException failure) {
            var terminal = failure.status() == 403 || failure.status() == 404 || failure.status() == 409 ? "STALE" : "FAILED";
            stopImprovementEvaluationReports(owner, run, failure.code());
            closeImprovementRun(run.id(), terminal, failure.code());
        }
    }

    private void requireCurrentImprovementTarget(ActorContext owner, UUID workspaceId, UUID improvementRunId) {
        var source = jdbc.query("select card_id, base_revision, base_memory_version, expected_card_version "
                        + "from learning.improvement_run where id = ? and tenant_id = ? and workspace_id = ? and owner_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("card_id", UUID.class), rs.getInt("base_revision"),
                        rs.getString("base_memory_version"), rs.getLong("expected_card_version")} : null,
                improvementRunId, owner.tenantId(), workspaceId, owner.actorId());
        if (source == null) throw EafException.notFound();
        var card = teamExperiences.get(owner, workspaceId, (UUID) source[0]);
        if (!owner.actorId().equals(card.ownerId()) || card.version() != (long) source[3]
                || card.latestRevision() != (int) source[1] || !Integer.valueOf((int) source[1]).equals(card.activeRevision())
                || card.active() == null || !"PUBLISHED".equals(card.active().status())
                || !((String) source[2]).equals(card.active().memoryVersion())
                || !card.active().expiresAt().isAfter(Instant.now(clock)))
            throw EafException.conflict("TEAM_EXPERIENCE_BASE_CHANGED", "评测期间 TEAM 基线已变化或到期。");
        var memory = memories.requireUsable(owner, workspaceId, card.id(), (String) source[2]);
        if (!"PUBLISHED".equals(memory.status()) || !"TEAM".equals(memory.scope()))
            throw EafException.conflict("TEAM_EXPERIENCE_BASE_CHANGED", "评测期间 TEAM 基线已失效。");
    }

    private void stopImprovementEvaluationReports(ActorContext owner, ImprovementEvaluationPoll run, String reason) {
        for (var reportId : java.util.Arrays.stream(new UUID[]{run.devReportId(), run.heldOutReportId()})
                .filter(java.util.Objects::nonNull).toList()) {
            try { evaluations.stopTeamPreparationEvaluation(owner, run.workspaceId(), reportId, reason); }
            catch (RuntimeException ignored) { }
        }
        if (run.qualityRunId() != null)
            usage.stopSpendScope(run.tenantId(), run.workspaceId(), "EVALUATION", run.qualityRunId(), reason);
    }

    private void deferImprovementEvaluation(UUID runId) {
        jdbc.update("update learning.improvement_run set next_poll_at = now() + interval '3 seconds', updated_at = now() "
                        + "where id = ? and status in ('DEV_EVALUATING','HELD_OUT_EVALUATING','AWAITING_EVIDENCE','STOPPING')", runId);
    }

    private void closeImprovementRun(UUID runId, String next, String reason) {
        jdbc.update("update learning.improvement_run set status = ?, stop_reason = ?, stop_requested = true, "
                        + "row_version = row_version + 1, next_poll_at = now() + interval '1 day', updated_at = now() "
                        + "where id = ? and status in ('DEV_EVALUATING','HELD_OUT_EVALUATING','AWAITING_EVIDENCE')",
                next, reason == null ? next : reason.substring(0, Math.min(80, reason.length())), runId);
    }

    private boolean terminalPreparation(String status) {
        return List.of("COMPLETED", "COMPLETED_WITH_ERRORS", "STOPPED", "FAILED", "TIMED_OUT").contains(status);
    }

    private ActorContext internalHuman(UUID tenantId, UUID actorId, UUID workspaceId) {
        return new ActorContext(actorId, tenantId, ActorType.HUMAN, workspaces.actions(tenantId, actorId, workspaceId));
    }

    @Override
    @Transactional
    public CandidateSubmission proposeFromImprovementRun(ActorContext actor, UUID workspaceId, UUID improvementRunId) {
        requireActor(actor, workspaceId, "learning:propose");
        if (actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("TEAM 候选只能由直接 HUMAN Owner 的改进运行提出。");
        var run = jdbc.query("select owner_id, card_id, base_revision, base_memory_version, expected_card_version, "
                        + "source_feedbacks::text, shared_correction, shared_correction_hash, generation_input_hash, "
                        + "quality_run_id, generation_task_id, status, candidate_id, candidate_revision, row_version "
                        + "from learning.improvement_run where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? new ImprovementCandidateRun(rs.getObject("owner_id", UUID.class),
                        rs.getObject("card_id", UUID.class), rs.getInt("base_revision"),
                        rs.getString("base_memory_version"), rs.getLong("expected_card_version"),
                        rs.getString("source_feedbacks"), rs.getString("shared_correction"),
                        rs.getString("shared_correction_hash"), rs.getString("generation_input_hash"),
                        rs.getObject("quality_run_id", UUID.class),
                        rs.getObject("generation_task_id", UUID.class), rs.getString("status"),
                        rs.getObject("candidate_id", UUID.class), rs.getObject("candidate_revision", Integer.class),
                        rs.getLong("row_version")) : null,
                improvementRunId, actor.tenantId(), workspaceId);
        if (run == null || !actor.actorId().equals(run.ownerId())) throw EafException.notFound();
        if (run.candidateId() != null)
            return new CandidateSubmission(get(actor, workspaceId, run.candidateId()), false);
        if (!"GENERATING".equals(run.status()) || run.qualityRunId() == null || run.generationTaskId() == null)
            throw EafException.conflict("IMPROVEMENT_RUN_NOT_GENERATING", "当前改进运行没有可绑定的生成 Task。");
        var generation = tasks.getTeamImprovementGenerationTask(actor, workspaceId, run.generationTaskId());
        var generationEvidence = tasks.evidence(actor.tenantId(), workspaceId, run.generationTaskId());
        if (generation.status() != TaskStatus.SUCCEEDED || !"EVALUATION".equals(generation.source())
                || !actor.actorId().equals(generation.actorId()) || generation.attempt() != 1
                || !run.qualityRunId().equals(generationEvidence.qualityRunId())
                || !Hashing.sha256(generation.inputText()).equals(run.generationInputHash()) || generation.resultJson() == null)
            throw EafException.conflict("IMPROVEMENT_GENERATION_UNVERIFIED", "生成结果未绑定本轮已完成的 EVALUATION Task。");
        final JsonNode proposedContent;
        try { proposedContent = json.readTree(generation.resultJson()); }
        catch (Exception invalid) { throw EafException.conflict("IMPROVEMENT_GENERATION_INVALID", "固定生成 Task 的结果不是有效 JSON。"); }
        var fields = new LinkedHashSet<String>();
        if (proposedContent != null && proposedContent.isObject()) proposedContent.fieldNames().forEachRemaining(fields::add);
        if (!fields.equals(Set.of("title", "appliesWhen", "content")))
            throw EafException.invalid("TEAM 候选只接受 title、appliesWhen 和 content。");
        var title = text(proposedContent, "title", 80);
        var appliesWhen = text(proposedContent, "appliesWhen", 300);
        var content = contentText(proposedContent, "content", 800);
        if (isSensitive(title + "\n" + appliesWhen + "\n" + content)
                || ("适用条件：" + appliesWhen + "\n建议：" + content).length() > 1_200)
            throw EafException.invalid("TEAM 候选疑似包含凭证或超过组合正文限制。");

        var card = teamExperiences.get(actor, workspaceId, run.cardId());
        if (!actor.actorId().equals(card.ownerId()) || card.version() != run.expectedCardVersion()
                || card.latestRevision() != run.baseRevision() || !Integer.valueOf(run.baseRevision()).equals(card.activeRevision())
                || card.active() == null || !run.baseMemoryVersion().equals(card.active().memoryVersion()))
            throw EafException.conflict("TEAM_EXPERIENCE_BASE_CHANGED", "生成期间 TEAM 卡基线已变化。");
        if (title.equals(card.active().title()) && appliesWhen.equals(card.active().appliesWhen())
                && content.equals(card.active().content())) {
            jdbc.update("update learning.improvement_run set status = 'NO_CHANGE', row_version = row_version + 1, "
                            + "next_poll_at = now(), updated_at = now() where id = ? and status = 'GENERATING' and row_version = ?",
                    improvementRunId, run.rowVersion());
            return new CandidateSubmission(null, false);
        }
        var sourceProof = card.active().source();
        if (sourceProof == null || sourceProof.workItemId() == null)
            throw EafException.conflict("TEAM_EXPERIENCE_SOURCE_MISSING", "目标 TEAM 卡缺少来源证明。");

        final JsonNode sourceFeedbacks;
        try { sourceFeedbacks = json.readTree(run.sourceFeedbacks()); }
        catch (Exception invalid) { throw new IllegalStateException("改进运行来源摘要无法读取。", invalid); }
        var evidenceRefs = new ArrayList<String>();
        evidenceRefs.add("improvement-run:" + improvementRunId);
        evidenceRefs.add("team-card:" + run.cardId() + "@" + run.baseRevision());
        evidenceRefs.add("work-item:" + sourceProof.workItemId());
        for (var source : sourceFeedbacks) {
            var feedbackId = UUID.fromString(source.path("feedbackId").asText());
            var feedback = requireFeedback(actor, workspaceId, feedbackId);
            if (!actor.actorId().equals(feedback.actorId())) throw EafException.notFound();
            var sourceTask = tasks.get(actor, workspaceId, feedback.taskId());
            if (!feedback.taskId().equals(UUID.fromString(source.path("taskId").asText()))
                    || feedback.taskAttempt() != source.path("attempt").asInt(-1)
                    || !Hashing.sha256(writeJson(feedback.source())).equals(source.path("contextHash").asText())
                    || sourceTask.status() != TaskStatus.SUCCEEDED || sourceTask.attempt() != feedback.taskAttempt()
                    || !"USER".equals(sourceTask.source())
                    || tasks.evidence(actor.tenantId(), workspaceId, sourceTask.id()).qualityRunId() != null)
                throw EafException.conflict("IMPROVEMENT_SOURCE_CHANGED", "所选反馈的普通 USER Task 或 attempt 已变化。");
            var usage = runtime.teamExperienceUsage(actor, workspaceId, sourceTask.id());
            if (usage.included().stream().noneMatch(item -> item.cardId().equals(run.cardId())
                    && item.revision() == run.baseRevision() && item.memoryVersion().equals(run.baseMemoryVersion())
                    && item.contentHash().equals(card.active().contentHash())))
                throw EafException.conflict("IMPROVEMENT_SOURCE_CARD_UNUSED", "所选反馈的来源 Task 未实际使用当前目标卡版本。");
            evidenceRefs.add("feedback:" + feedbackId);
            evidenceRefs.add("task:" + feedback.taskId() + "@" + feedback.taskAttempt());
        }
        var uniqueRefs = List.copyOf(new LinkedHashSet<>(evidenceRefs));
        var baseSnapshot = json.createObjectNode().put("cardId", run.cardId().toString())
                .put("baseRevision", run.baseRevision()).put("baseMemoryVersion", run.baseMemoryVersion())
                .put("expectedCardVersion", run.expectedCardVersion()).put("scenarioKey", card.scenarioKey())
                .put("expiresAt", card.active().expiresAt().toString());
        baseSnapshot.set("base", json.valueToTree(card.active()));
        baseSnapshot.set("sourceProof", json.valueToTree(sourceProof));
        baseSnapshot.put("sharedCorrection", run.sharedCorrection());
        baseSnapshot.put("sharedCorrectionHash", run.sharedCorrectionHash());
        var normalized = json.createObjectNode().put("title", title).put("appliesWhen", appliesWhen).put("content", content);
        var draft = new CandidateDraft("TEAM_EXPERIENCE_UPDATE", run.cardId(), run.baseMemoryVersion(),
                actor.actorId(), "TEAM", baseSnapshot, normalized, uniqueRefs, true);
        var sourceFeedbackId = UUID.fromString(sourceFeedbacks.get(0).path("feedbackId").asText());
        var submission = insertCandidate(actor, workspaceId, sourceFeedbackId,
                "p23-improvement:" + improvementRunId, draft);
        var candidate = submission.candidate();
        var evidenceHash = Hashing.sha256(writeJson(sourceFeedbacks));
        jdbc.update("insert into learning.team_candidate_binding(candidate_id, candidate_revision, improvement_run_id, card_id, "
                        + "base_revision, base_memory_version, expected_card_version, source_work_item_id, source_proof, source_feedbacks, evidence_hash, created_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?) on conflict (candidate_id, candidate_revision) do nothing",
                candidate.id(), candidate.revision(), improvementRunId, run.cardId(), run.baseRevision(),
                run.baseMemoryVersion(), run.expectedCardVersion(), sourceProof.workItemId(), writeJson(sourceProof),
                writeJson(sourceFeedbacks), evidenceHash, Timestamp.from(Instant.now(clock)));
        if (jdbc.update("update learning.improvement_run set candidate_id = ?, candidate_revision = ?, status = 'AWAITING_REVIEW', "
                        + "row_version = row_version + 1, next_poll_at = now(), updated_at = now() where id = ? and status = 'GENERATING' "
                        + "and row_version = ? and generation_task_id = ?",
                candidate.id(), candidate.revision(), improvementRunId, run.rowVersion(), run.generationTaskId()) != 1)
            throw EafException.conflict("IMPROVEMENT_RUN_CHANGED", "改进运行在候选绑定时已变化。");
        return submission;
    }

    @Override
    @Transactional
    public CandidateSubmission propose(ManualCandidateCommand command) {
        if (command == null) throw EafException.invalid("候选提议不能为空。");
        requireActor(command.actor(), command.workspaceId(), "learning:propose");
        if (blank(command.idempotencyKey()) || command.idempotencyKey().length() > 200 || hasControl(command.idempotencyKey()))
            throw EafException.invalid("候选提议需要有效 Idempotency-Key。");
        var sourceFeedback = command.sourceFeedbackId() == null ? null
                : requireFeedback(command.actor(), command.workspaceId(), command.sourceFeedbackId());
        var draft = prepare(command.actor(), command.workspaceId(), command.targetType(), command.targetId(),
                command.baseVersion(), command.proposedContent(), validateRefs(command.evidenceRefs()), false);
        if (sourceFeedback != null && !isLinked(sourceFeedback, draft.targetType(), draft.targetId()))
            throw EafException.invalid("候选目标必须来自所引用反馈的可信 Context 来源。");
        if (isSensitive(writeJson(draft.proposedContent()))) throw EafException.invalid("候选内容疑似包含凭证信息，不能进入学习流程。");
        return insertCandidate(command.actor(), command.workspaceId(), command.sourceFeedbackId(), command.idempotencyKey(), draft);
    }

    @Override
    @Transactional(readOnly = true)
    public LearningCandidate get(ActorContext actor, UUID workspaceId, UUID candidateId) {
        requireActor(actor, workspaceId, "learning:read");
        var candidate = load(actor.tenantId(), workspaceId, candidateId);
        checkTargetReadable(actor, workspaceId, candidate);
        return candidate;
    }

    @Override
    @Transactional
    public LearningCandidate revise(CandidateRevisionCommand command) {
        if (command == null || command.expectedVersion() <= 0) throw EafException.invalid("候选修订需要 expectedVersion。");
        requireActor(command.actor(), command.workspaceId(), "learning:propose");
        var current = get(command.actor(), command.workspaceId(), command.candidateId());
        if (!current.proposerId().equals(command.actor().actorId())) throw EafException.forbidden("只有原提议人可以修订此候选。");
        if ("PUBLISHED".equals(current.status())) throw EafException.conflict("CANDIDATE_IMMUTABLE", "已发布候选不能修改。");
        if ("PUBLISHING".equals(current.status())) throw EafException.conflict("CANDIDATE_PUBLISHING", "发布结果核清前不能修改候选修订。");
        if (current.rowVersion() != command.expectedVersion()) throw EafException.conflict("VERSION_CONFLICT", "候选版本已变化。");
        var feedback = current.sourceFeedbackId() == null ? null
                : requireFeedback(command.actor(), command.workspaceId(), current.sourceFeedbackId());
        var draft = prepare(command.actor(), command.workspaceId(), command.targetType(), command.targetId(),
                command.baseVersion(), command.proposedContent(), validateRefs(command.evidenceRefs()), true);
        if (feedback != null && !isLinked(feedback, draft.targetType(), draft.targetId()))
            throw EafException.invalid("候选目标必须来自所引用反馈的可信 Context 来源。");
        if (isSensitive(writeJson(draft.proposedContent()))) throw EafException.invalid("候选内容疑似包含凭证信息，不能进入学习流程。");
        var now = Timestamp.from(Instant.now(clock));
        var nextRevision = current.revision() + 1;
        var hash = revisionHash(draft);
        var changed = jdbc.update("update learning.candidate set current_revision = ?, status = 'PROPOSED', row_version = row_version + 1, updated_at = ? where id = ? and tenant_id = ? and workspace_id = ? and row_version = ? and status <> 'PUBLISHED'",
                nextRevision, now, command.candidateId(), command.actor().tenantId(), command.workspaceId(), command.expectedVersion());
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "候选修订竞争失败。");
        insertRevision(command.candidateId(), nextRevision, draft, hash, now);
        evaluations.invalidateCandidateContext(command.actor(), command.workspaceId(), current.id(), current.revision());
        return load(command.actor().tenantId(), command.workspaceId(), command.candidateId());
    }

    @Override
    @Transactional
    public LearningCandidate review(CandidateReviewCommand command) {
        if (command == null || command.expectedVersion() <= 0 || blank(command.reason()) || command.reason().length() > 1000
                || !List.of("ACCEPTED", "REJECTED").contains(command.decision()))
            throw EafException.invalid("审核需要有效决定、理由和 expectedVersion。");
        requireActor(command.actor(), command.workspaceId(), "learning:review");
        if (command.actor().type() != ActorType.HUMAN) throw EafException.forbidden("只有独立 HUMAN 可以审核学习候选。");
        var current = get(command.actor(), command.workspaceId(), command.candidateId());
        if (current.rowVersion() != command.expectedVersion() || !"PROPOSED".equals(current.status()))
            throw EafException.conflict("VERSION_CONFLICT", "候选不在当前可审核版本。");
        if ("ACCEPTED".equals(command.decision()) && current.proposerId().equals(command.actor().actorId()))
            throw EafException.forbidden("候选提议人不能接受自己的事实审核。");
        var refs = command.factEvidenceRefs() == null ? List.<String>of() : validateRefs(command.factEvidenceRefs());
        if ("ACCEPTED".equals(command.decision()) && (refs.isEmpty() || refs.stream().allMatch(this::feedbackOnlyRef)))
            throw EafException.invalid("接受候选必须记录独立事实依据，反馈自身不构成事实证明。");
        if (isSensitive(writeJson(current.proposedContent()))) throw EafException.invalid("含疑似凭证的候选不能通过事实审核。");
        var now = Timestamp.from(Instant.now(clock));
        var nextStatus = "ACCEPTED".equals(command.decision()) ? "IN_REVIEW" : "REJECTED";
        var changed = jdbc.update("update learning.candidate set status = ?, row_version = row_version + 1, updated_at = ? where id = ? and tenant_id = ? and workspace_id = ? and row_version = ? and status = 'PROPOSED'",
                nextStatus, now, command.candidateId(), command.actor().tenantId(), command.workspaceId(), command.expectedVersion());
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "候选审核竞争失败。");
        jdbc.update("insert into learning.candidate_review(id, candidate_id, candidate_revision, reviewer_id, decision, reason, fact_evidence_refs, created_at) values (?, ?, ?, ?, ?, ?, ?::jsonb, ?)",
                UUID.randomUUID(), command.candidateId(), current.revision(), command.actor().actorId(), command.decision(),
                command.reason().trim(), writeJson(refs), now);
        if ("ACCEPTED".equals(command.decision())) {
            if ("TEAM_EXPERIENCE_UPDATE".equals(current.targetType())) {
                var binding = jdbc.query("select b.improvement_run_id, i.quality_run_id, i.deadline_at, i.dataset_key, i.dataset_version "
                                + "from learning.team_candidate_binding b join learning.improvement_run i on i.id = b.improvement_run_id "
                                + "where b.candidate_id = ? and b.candidate_revision = ? and i.status = 'AWAITING_REVIEW'",
                        rs -> rs.next() ? new TeamReviewBinding(rs.getObject("improvement_run_id", UUID.class),
                                rs.getObject("quality_run_id", UUID.class), rs.getTimestamp("deadline_at").toInstant(),
                                rs.getString("dataset_key"), rs.getString("dataset_version")) : null,
                        current.id(), current.revision());
                if (binding == null || binding.qualityRunId() == null)
                    throw EafException.conflict("IMPROVEMENT_RUN_CHANGED", "候选已失去对应运行绑定。");
                var owner = internalHuman(current.tenantId(), current.ownerId(), command.workspaceId());
                evaluations.captureCandidateContext(teamCandidateContextCommand(owner, command.workspaceId(), current));
                var report = evaluations.startTeamPreparationEvaluation(owner, command.workspaceId(),
                        current.id(), current.revision(), teamCandidateSnapshotId(owner, command.workspaceId(), current),
                        binding.improvementRunId(), binding.qualityRunId(), "DEV", binding.deadlineAt());
                jdbc.update("update learning.improvement_run set status = 'DEV_EVALUATING', dev_report_id = ?, "
                                + "row_version = row_version + 1, next_poll_at = now(), updated_at = now() "
                                + "where id = ? and status = 'AWAITING_REVIEW'",
                        report.reportId(), binding.improvementRunId());
            } else evaluations.captureCandidateContext(candidateContextCommand(command.actor(), command.workspaceId(), current));
        } else if ("TEAM_EXPERIENCE_UPDATE".equals(current.targetType())) {
            jdbc.update("update learning.improvement_run set status = 'REJECTED', stop_reason = 'FACT_REVIEW_REJECTED', "
                            + "row_version = row_version + 1, updated_at = now() where candidate_id = ? and status = 'AWAITING_REVIEW'",
                    current.id());
        }
        return load(command.actor().tenantId(), command.workspaceId(), command.candidateId());
    }

    // 批准只锁定 Learning 修订与当前评测证据；目标资产仍须在后续发布时重新授权并检查基线。
    @Override
    @Transactional
    public CandidateApproval approve(CandidateApprovalCommand command) {
        if (command == null || command.expectedVersion() <= 0 || blank(command.reason())
                || command.reason().length() > 1000 || !List.of("APPROVED", "REJECTED").contains(command.decision()))
            throw EafException.invalid("候选批准需要有效决定、理由和 expectedVersion。");
        if (isSensitive(command.reason())) throw EafException.invalid("批准理由疑似包含凭证信息，不能保存。");
        requireActor(command.actor(), command.workspaceId(), "learning:approve");
        requireActor(command.actor(), command.workspaceId(), "learning:read");
        var current = get(command.actor(), command.workspaceId(), command.candidateId());
        if (current.rowVersion() != command.expectedVersion() || !"IN_REVIEW".equals(current.status()))
            throw EafException.conflict("VERSION_CONFLICT", "候选不在当前待批准修订。");
        if (current.proposerId().equals(command.actor().actorId()))
            throw EafException.forbidden("候选提议人不能批准自己的候选。");
        var independentlyReviewed = current.reviews().stream().anyMatch(review -> review.candidateRevision() == current.revision()
                && "ACCEPTED".equals(review.decision()) && !review.reviewerId().equals(current.proposerId()));
        if (!independentlyReviewed || current.evidenceGap())
            throw EafException.conflict("CANDIDATE_NOT_REVIEWED", "当前修订缺少独立事实审核或仍有证据缺口。");

        var approvalId = UUID.randomUUID();
        var now = Instant.now(clock);
        UUID reportId = null;
        String reportKind = "TEAM_EXPERIENCE_UPDATE".equals(current.targetType())
                ? "TEAM_PREPARATION_HELD_OUT" : "LEGACY_CANDIDATE_RISK";
        String reportJson = null, reportHash = null, reportConfigHash = null, datasetHash = null;
        String summaryJson = null;
        Instant validUntil = null;
        if ("APPROVED".equals(command.decision())) {
            if (command.evaluationReportId() == null)
                throw EafException.invalid("批准必须绑定通过的评测报告。");
            var summary = json.createObjectNode();
            if ("TEAM_EXPERIENCE_UPDATE".equals(current.targetType())) {
                var evidence = evaluations.getTeamPreparationReleaseEvidence(command.actor(), command.workspaceId(),
                        current.id(), current.revision());
                if (!command.evaluationReportId().equals(evidence.reportId())
                        || !"TEAM_PREPARATION_HELD_OUT".equals(evidence.reportKind())
                        || !current.id().equals(evidence.candidateId()) || evidence.candidateRevision() != current.revision()
                        || !current.targetId().equals(evidence.cardId())
                        || !current.contentHash().equals(evidence.candidateContentHash())
                        || !Hashing.sha256(writeJson(current.evidenceRefs())).equals(evidence.evidenceHash())
                        || !"ELIGIBLE".equals(evidence.eligibility()) || !evidence.current())
                    throw EafException.conflict("TEAM_EVALUATION_NOT_ELIGIBLE", "只有当前且经完整保留集人工复核的证据可以批准。");
                requireCurrentTarget(command.actor(), command.workspaceId(), current);
                reportId = evidence.reportId();
                reportKind = evidence.reportKind();
                reportHash = evidence.reportHash();
                reportConfigHash = evidence.configurationHash();
                datasetHash = evidence.datasetHash();
                summary.put("status", evidence.eligibility());
                summary.set("teamPreparationEvidence", json.valueToTree(evidence));
            } else {
                var report = evaluations.getCandidateEvaluation(command.actor(), command.workspaceId(), command.evaluationReportId());
                if (!"PASSED".equals(report.status()) || !report.candidateId().equals(current.id())
                        || report.candidateRevision() != current.revision()
                        || !report.targetType().equals(current.targetType())
                        || !java.util.Objects.equals(report.targetId(), current.targetId())
                        || !java.util.Objects.equals(report.baseVersion(), current.baseVersion())
                        || !report.candidateContentHash().equals(current.contentHash())
                        || report.completedSamples() != report.totalSamples() || report.totalSamples() == 0)
                    throw EafException.conflict("CANDIDATE_EVALUATION_MISMATCH", "评测报告未通过或未绑定当前候选修订。");
                if (!evaluations.isCandidateEvaluationCurrent(command.actor(), command.workspaceId(), report.id()))
                    throw EafException.conflict("CANDIDATE_EVALUATION_STALE", "评测快照或目标基线已失效，必须重新评测。");
                requireCurrentTarget(command.actor(), command.workspaceId(), current);
                reportId = report.id();
                reportJson = writeJson(report);
                reportHash = Hashing.sha256(reportJson);
                reportConfigHash = report.configurationHash();
                datasetHash = report.datasetHash();
                summary.put("status", report.status());
                summary.put("baselineCorrect", report.baselineCorrect());
                summary.put("candidateCorrect", report.candidateCorrect());
                summary.put("totalSamples", report.totalSamples());
                summary.put("targetImprovedCases", report.targetImprovedCases());
                summary.put("nonTargetRegressionCases", report.nonTargetRegressionCases());
                summary.put("safetyViolations", report.safetyViolations());
                summary.put("citationSupportedCalls", report.citationSupportedCalls());
                summary.put("citationScoredCalls", report.citationScoredCalls());
                summary.put("modelCalls", report.modelCalls());
                summary.put("knownUsageCalls", report.knownUsageCalls());
                summary.put("inputTokens", report.inputTokens());
                summary.put("outputTokens", report.outputTokens());
                summary.put("estimatedCost", report.estimatedCost());
                summary.put("costStatus", report.costStatus());
                summary.put("costCurrency", report.costCurrency());
            }
            summaryJson = writeJson(summary);
            validUntil = now.plus(Duration.ofMinutes(30));
        } else if (command.evaluationReportId() != null) {
            throw EafException.invalid("拒绝决定不需要评测报告。");
        }

        var nextStatus = command.decision();
        var changed = jdbc.update("update learning.candidate set status = ?, row_version = row_version + 1, updated_at = ? "
                        + "where id = ? and tenant_id = ? and workspace_id = ? and row_version = ? and status = 'IN_REVIEW'",
                nextStatus, Timestamp.from(now), current.id(), command.actor().tenantId(), command.workspaceId(), command.expectedVersion());
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "候选批准竞争失败。");
        var evidenceHash = Hashing.sha256(writeJson(current.evidenceRefs()));
        jdbc.update("insert into learning.candidate_approval(id, tenant_id, workspace_id, candidate_id, candidate_revision, approver_id, decision, target_type, target_id, base_version, owner_id, scope, candidate_content_hash, evidence_hash, evaluation_report_id, evaluation_report_kind, evaluation_report_hash, evaluation_configuration_hash, dataset_hash, evaluation_summary, authorization_action, valid_until, reason, created_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, 'learning:approve', ?, ?, ?)",
                approvalId, command.actor().tenantId(), command.workspaceId(), current.id(), current.revision(),
                command.actor().actorId(), command.decision(), current.targetType(), current.targetId(), current.baseVersion(),
                current.ownerId(), current.scope(), current.contentHash(), evidenceHash, reportId, reportKind,
                reportHash, reportConfigHash, datasetHash, summaryJson,
                validUntil == null ? null : Timestamp.from(validUntil), command.reason().trim(), Timestamp.from(now));
        return approvalById(command.actor().tenantId(), command.workspaceId(), approvalId);
    }

    @Override
    public CandidateRelease publish(CandidateReleaseCommand command) {
        if (command == null || command.expectedVersion() <= 0 || command.approvalId() == null)
            throw EafException.invalid("候选发布需要 expectedVersion 和批准记录。");
        var job = transactions.execute(status -> prepareReleaseIntent(command));
        if (job == null) throw new IllegalStateException("候选发布意图未保存。");
        if ("RELEASED".equals(job.status())) return job;

        // 目标写入前先查来源事实；目标已提交而 Learning 未补记时，此处只对账，不再创建新版本。
        var existing = findTargetReceipt(command.actor(), command.workspaceId(), load(command.actor().tenantId(),
                command.workspaceId(), command.candidateId()), job);
        if (existing != null) return finishRelease(job, existing);

        try {
            var candidate = load(command.actor().tenantId(), command.workspaceId(), command.candidateId());
            var approval = approvalById(command.actor().tenantId(), command.workspaceId(), job.approvalId());
            validateReleaseAuthorization(command.actor(), command.workspaceId(), candidate, approval, job);
            var receipt = publishTarget(command.actor(), command.workspaceId(), candidate);
            return finishRelease(job, receipt);
        } catch (EafException deniedOrConflict) {
            // 捕获目标返回的确定性冲突后再查一次，防止提交成功、回执读取失败时解除候选冻结。
            var committed = findTargetReceipt(command.actor(), command.workspaceId(),
                    load(command.actor().tenantId(), command.workspaceId(), command.candidateId()), job);
            if (committed != null) return finishRelease(job, committed);
            transactions.execute(status -> { failRelease(job, deniedOrConflict.code()); return null; });
            throw deniedOrConflict;
        }
    }

    @Override
    @Transactional(readOnly = true)
    public CandidateRelease getRelease(ActorContext actor, UUID workspaceId, UUID candidateId) {
        requireActor(actor, workspaceId, "learning:read");
        var candidate = load(actor.tenantId(), workspaceId, candidateId);
        return releaseByCandidate(actor.tenantId(), workspaceId, candidate.id(), candidate.revision())
                .orElseThrow(EafException::notFound);
    }

    @Override
    public CandidateWithdrawal withdrawRelease(CandidateWithdrawalCommand command) {
        if (command == null || command.actor() == null || command.candidateId() == null
                || command.expectedVersion() <= 0 || command.reasonRef() == null || command.reasonRef().isBlank())
            throw EafException.invalid("候选撤回需要 expectedVersion 和 reasonRef。");
        if (command.reasonRef().trim().length() > 500 || hasControl(command.reasonRef()))
            throw EafException.invalid("撤回 reasonRef 超过长度或包含控制字符。");
        var preparation = transactions.execute(status -> inspectWithdrawal(command));
        if (preparation == null) throw new IllegalStateException("撤回检查未返回结果。");
        var job = preparation.job();
        if (job == null) {
            var snapshot = withdrawalTargetSnapshot(command.actor(), command.workspaceId(), preparation.candidate(), preparation.release());
            job = transactions.execute(status -> saveWithdrawalIntent(command, preparation, snapshot));
            if (job == null) throw new IllegalStateException("撤回意图未保存。");
        }
        if ("WITHDRAWN".equals(job.status())) return job;
        var existing = findTargetWithdrawal(command.actor(), command.workspaceId(), preparation.candidate(), job);
        if (existing != null) return finishWithdrawal(command.actor(), command.workspaceId(), job, existing);
        try {
            var receipt = revokeTarget(command.actor(), command.workspaceId(), preparation.candidate(), job);
            return finishWithdrawal(command.actor(), command.workspaceId(), job, receipt);
        } catch (EafException deniedOrConflict) {
            var committed = findTargetWithdrawal(command.actor(), command.workspaceId(), preparation.candidate(), job);
            if (committed != null) return finishWithdrawal(command.actor(), command.workspaceId(), job, committed);
            var failedJob = job;
            transactions.execute(status -> { failWithdrawal(failedJob, deniedOrConflict.code()); return null; });
            throw deniedOrConflict;
        }
    }

    @Override
    @Transactional(readOnly = true)
    public CandidateWithdrawal getWithdrawal(ActorContext actor, UUID workspaceId, UUID candidateId) {
        requireActor(actor, workspaceId, "learning:read");
        var candidate = load(actor.tenantId(), workspaceId, candidateId);
        return withdrawalByCandidate(actor.tenantId(), workspaceId, candidate.id(), candidate.revision())
                .orElseThrow(EafException::notFound);
    }

    @Override
    @Transactional
    public CandidateService.CandidateIterationSubmission recordIteration(ActorContext actor, UUID workspaceId,
                                                                         UUID candidateId, UUID followupTaskId) {
        requireActor(actor, workspaceId, "learning:read");
        var candidate = get(actor, workspaceId, candidateId);
        if (candidate.sourceFeedbackId() == null)
            throw EafException.conflict("ITERATION_SOURCE_MISSING", "后续使用证据必须关联原始反馈。" );
        var release = releaseByCandidate(actor.tenantId(), workspaceId, candidate.id(), candidate.revision())
                .filter(item -> "RELEASED".equals(item.status()))
                .orElseThrow(() -> EafException.conflict("RELEASE_NOT_FOUND", "候选发布尚未完成。"));
        var approval = approvalById(actor.tenantId(), workspaceId, release.approvalId());
        //  批准时冻结的是类型化 ELIGIBLE 证据；旧候选仍要求原 PASSED 报告语义。
        var approvedEvidence = approval != null && approval.evaluationSummary() != null
                && ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType())
                ? "TEAM_PREPARATION_HELD_OUT".equals(approval.evaluationReportKind())
                    && "ELIGIBLE".equals(approval.evaluationSummary().path("status").asText())
                    && approval.evaluationReportId() != null
                    && approval.evaluationReportId().toString().equals(approval.evaluationSummary()
                    .path("teamPreparationEvidence").path("reportId").asText())
                    && candidate.id().toString().equals(approval.evaluationSummary()
                    .path("teamPreparationEvidence").path("candidateId").asText())
                    && candidate.revision() == approval.evaluationSummary().path("teamPreparationEvidence")
                    .path("candidateRevision").asInt()
                : "PASSED".equals(approval.evaluationSummary().path("status").asText()));
        if (approval == null || !"APPROVED".equals(approval.decision()) || approval.evaluationReportId() == null
                || !approvedEvidence)
            throw EafException.conflict("ITERATION_EVALUATION_MISSING", "发布缺少通过的离线评测证据。" );

        var sourceTaskId = jdbc.query("select task_id from learning.feedback where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getObject("task_id", UUID.class) : null,
                candidate.sourceFeedbackId(), actor.tenantId(), workspaceId);
        if (sourceTaskId == null) throw EafException.conflict("ITERATION_SOURCE_MISSING", "原始反馈来源已不可用。" );
        var sourceTask = tasks.get(actor, workspaceId, sourceTaskId);
        var followupTask = tasks.get(actor, workspaceId, followupTaskId);
        if (!"USER".equals(followupTask.source()) || !terminal(followupTask.status()))
            throw EafException.forbidden("后续使用证据只接受终态 USER Task。" );
        if (followupTask.id().equals(sourceTask.id()) || !differentInput(sourceTask, followupTask))
            throw EafException.invalid("后续 Task 必须使用不同于原反馈任务的新输入。" );
        if (followupTask.createdAt().isBefore(release.updatedAt()))
            throw EafException.conflict("ITERATION_TASK_PRECEDES_RELEASE", "后续 Task 必须在目标版本发布后创建。" );

        var sources = runtime.contextSources(actor, workspaceId, followupTask.id());
        var targetSource = sources.stream().filter(source -> matchesReleasedSource(actor, workspaceId, source, release))
                .findFirst().orElse(null);
        var reportedRisk = reportedRisk(followupTask);
        var citations = citations(followupTask);
        var usageStatus = followupTask.status() != io.eaf.task.api.TaskStatus.SUCCEEDED ? "TASK_NOT_SUCCEEDED"
                : targetSource == null ? "NOT_AVAILABLE"
                : citations.contains(targetSource.citationId()) ? "CITED" : "AVAILABLE_NOT_CITED";
        var inputHash = Hashing.sha256(normalizeInput(followupTask.inputText()));
        var resultHash = followupTask.resultJson() == null ? null : Hashing.sha256(followupTask.resultJson());
        var requestHash = Hashing.sha256(writeJson(new IterationRequest(candidate.id(), candidate.revision(),
                candidate.sourceFeedbackId(), sourceTask.id(), approval.evaluationReportId(), approval.id(), release.id(),
                release.targetReleaseId(), release.targetType(), release.targetId(), release.targetVersion(),
                followupTask.id(), followupTask.attempt(), followupTask.status().name(), usageStatus, targetSource,
                inputHash, resultHash, reportedRisk, followupTask.createdAt())));
        var prior = jdbc.query("select id, request_hash from learning.candidate_iteration where tenant_id = ? and workspace_id = ? "
                        + "and candidate_id = ? and candidate_revision = ? and followup_task_id = ? and followup_attempt = ?",
                rs -> rs.next() ? new IterationKey(rs.getObject("id", UUID.class), rs.getString("request_hash")) : null,
                actor.tenantId(), workspaceId, candidate.id(), candidate.revision(), followupTask.id(), followupTask.attempt());
        if (prior != null) {
            if (!requestHash.equals(prior.requestHash()))
                throw EafException.conflict("ITERATION_CONFLICT", "同一 Task 尝试的使用证据已绑定不同结果。" );
            return new CandidateService.CandidateIterationSubmission(iterationById(actor.tenantId(), workspaceId, prior.id()), false);
        }
        var now = Timestamp.from(Instant.now(clock));
        var id = UUID.randomUUID();
        jdbc.update("insert into learning.candidate_iteration(id, tenant_id, workspace_id, candidate_id, candidate_revision, "
                        + "source_feedback_id, source_task_id, evaluation_report_id, approval_id, release_id, target_release_id, "
                        + "target_type, target_id, target_version, followup_task_id, followup_attempt, task_status, usage_status, "
                        + "target_source, input_hash, result_hash, reported_risk_level, fact_outcome, request_hash, task_created_at, recorded_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, 'UNVERIFIED', ?, ?, ?)",
                id, actor.tenantId(), workspaceId, candidate.id(), candidate.revision(), candidate.sourceFeedbackId(),
                sourceTask.id(), approval.evaluationReportId(), approval.id(), release.id(), release.targetReleaseId(),
                release.targetType(), release.targetId(), release.targetVersion(), followupTask.id(), followupTask.attempt(),
                followupTask.status().name(), usageStatus, targetSource == null ? null : writeJson(targetSource), inputHash,
                resultHash, reportedRisk, requestHash, Timestamp.from(followupTask.createdAt()), now);
        return new CandidateService.CandidateIterationSubmission(iterationById(actor.tenantId(), workspaceId, id), true);
    }

    @Override
    @Transactional(readOnly = true)
    public List<CandidateIteration> iterations(ActorContext actor, UUID workspaceId, UUID candidateId) {
        requireActor(actor, workspaceId, "learning:read");
        get(actor, workspaceId, candidateId);
        return jdbc.query(iterationSelect() + " where i.tenant_id = ? and i.workspace_id = ? and i.candidate_id = ? "
                        + "order by i.recorded_at, i.id",
                (rs, row) -> mapIteration(rs), actor.tenantId(), workspaceId, candidateId);
    }

    private CandidateIteration iterationById(UUID tenantId, UUID workspaceId, UUID id) {
        return jdbc.query(iterationSelect() + " where i.tenant_id = ? and i.workspace_id = ? and i.id = ?",
                rs -> rs.next() ? mapIteration(rs) : null, tenantId, workspaceId, id);
    }

    private String iterationSelect() {
        return "select i.id, i.candidate_id, i.candidate_revision, i.source_feedback_id, i.source_task_id, "
                + "i.evaluation_report_id, a.evaluation_summary->>'status' as evaluation_status, i.approval_id, "
                + "i.release_id, i.target_release_id, r.status as release_status, w.id as withdrawal_id, "
                + "w.status as withdrawal_status, i.target_type, i.target_id, i.target_version, i.followup_task_id, "
                + "i.followup_attempt, i.task_status, i.usage_status, i.target_source::text, i.input_hash, i.result_hash, "
                + "i.reported_risk_level, i.fact_outcome, i.task_created_at, i.recorded_at "
                + "from learning.candidate_iteration i join learning.candidate_approval a on a.id = i.approval_id "
                + "join learning.candidate_release r on r.id = i.release_id "
                + "left join learning.candidate_withdrawal w on w.candidate_release_id = i.release_id";
    }

    private CandidateIteration mapIteration(ResultSet rs) throws SQLException {
        try {
            var sourceJson = rs.getString("target_source");
            return new CandidateIteration(rs.getObject("id", UUID.class), rs.getObject("candidate_id", UUID.class),
                    rs.getInt("candidate_revision"), rs.getObject("source_feedback_id", UUID.class),
                    rs.getObject("source_task_id", UUID.class), rs.getObject("evaluation_report_id", UUID.class),
                    rs.getString("evaluation_status"), rs.getObject("approval_id", UUID.class),
                    rs.getObject("release_id", UUID.class), rs.getObject("target_release_id", UUID.class),
                    rs.getString("release_status"), rs.getObject("withdrawal_id", UUID.class), rs.getString("withdrawal_status"),
                    rs.getString("target_type"), rs.getObject("target_id", UUID.class), rs.getString("target_version"),
                    rs.getObject("followup_task_id", UUID.class), rs.getInt("followup_attempt"), rs.getString("task_status"),
                    rs.getString("usage_status"), sourceJson == null ? null : json.readValue(sourceJson, ContextSourceRef.class),
                    rs.getString("input_hash"), rs.getString("result_hash"), rs.getString("reported_risk_level"),
                    rs.getString("fact_outcome"), rs.getTimestamp("task_created_at").toInstant(),
                    rs.getTimestamp("recorded_at").toInstant());
        } catch (Exception invalidJson) { throw new IllegalStateException("候选后续使用证据无法读取。", invalidJson); }
    }

    private boolean matchesReleasedSource(ActorContext actor, UUID workspaceId, ContextSourceRef source,
                                          CandidateRelease release) {
        if (source == null) return false;
        if ("KNOWLEDGE_UPDATE".equals(release.targetType())) {
            if (!"KNOWLEDGE_UPDATE".equals(sourceType(source)) || !release.targetId().equals(source.documentId())
                    || !Integer.toString(source.documentVersion()).equals(release.targetVersion())) return false;
            // Context 保存 Chunk 摘要，发布回执保存完整版本摘要；通过 Knowledge API 核对版本真相。
            try {
                var version = knowledge.getVersion(actor, workspaceId, source.documentId(), source.documentVersion());
                return "PUBLISHED".equals(version.status())
                        && release.targetContentHash().equals(version.contentHash());
            } catch (EafException unavailableOrDenied) {
                return false;
            }
        }
        if ("TEAM_EXPERIENCE_UPDATE".equals(release.targetType())) {
            if (!"MEMORY".equals(source.sourceType()) || !release.targetId().equals(source.memoryId())
                    || !release.targetVersion().equals(source.memoryVersion())
                    || !release.targetContentHash().equals(source.contentHash())) return false;
            try {
                var origin = teamExperiences.findReleaseByOrigin(actor, workspaceId, release.candidateId(),
                        release.candidateRevision()).orElse(null);
                if (origin == null || !"PUBLISH_CANDIDATE".equals(origin.action())
                        || !release.targetId().equals(origin.cardId()) || !release.targetVersion().equals(origin.memoryVersion()))
                    return false;
                var revision = teamExperiences.versions(actor, workspaceId, origin.cardId(), origin.revision() + 1, 10)
                        .stream().filter(item -> item.revision() == origin.revision()).findFirst().orElse(null);
                var memory = memories.get(actor, workspaceId, release.targetId(), release.targetVersion());
                return revision != null && "PUBLISHED".equals(revision.status())
                        && release.candidateId().equals(revision.originCandidateId())
                        && Integer.valueOf(release.candidateRevision()).equals(revision.originCandidateRevision())
                        && release.targetContentHash().equals(revision.contentHash())
                        && "PUBLISHED".equals(memory.status()) && "TEAM".equals(memory.scope())
                        && release.targetContentHash().equals(memory.contentHash());
            } catch (EafException unavailableOrDenied) {
                return false;
            }
        }
        return "MEMORY_UPSERT".equals(release.targetType()) && "MEMORY_UPSERT".equals(sourceType(source))
                && release.targetId().equals(source.memoryId()) && release.targetVersion().equals(source.memoryVersion())
                && java.util.Objects.equals(source.contentHash(), release.targetContentHash());
    }

    private Set<String> citations(TaskSnapshot task) {
        if (task.resultJson() == null) return Set.of();
        try {
            var node = json.readTree(task.resultJson()).path("citations");
            if (!node.isArray()) return Set.of();
            var result = new LinkedHashSet<String>();
            node.forEach(item -> { if (item.isTextual()) result.add(item.asText()); });
            return Set.copyOf(result);
        } catch (Exception ignored) { return Set.of(); }
    }

    private String reportedRisk(TaskSnapshot task) {
        if (task.resultJson() == null) return null;
        try {
            var value = json.readTree(task.resultJson()).path("riskLevel");
            return value.isTextual() ? value.asText() : null;
        } catch (Exception ignored) { return null; }
    }

    private boolean differentInput(TaskSnapshot source, TaskSnapshot followup) {
        return !normalizeInput(source.inputText()).equals(normalizeInput(followup.inputText()));
    }

    private String normalizeInput(String input) { return input == null ? "" : input.trim().replaceAll("\\s+", " "); }

    private record IterationKey(UUID id, String requestHash) { }
    private record IterationRequest(UUID candidateId, int candidateRevision, UUID sourceFeedbackId, UUID sourceTaskId,
                                    UUID evaluationReportId, UUID approvalId, UUID releaseId, UUID targetReleaseId,
                                    String targetType, UUID targetId, String targetVersion, UUID followupTaskId,
                                    int followupAttempt, String taskStatus, String usageStatus,
                                    ContextSourceRef targetSource, String inputHash, String resultHash,
                                    String reportedRiskLevel, Instant taskCreatedAt) { }

    private WithdrawalPreparation inspectWithdrawal(CandidateWithdrawalCommand command) {
        var actor = command.actor();
        if (actor.type() != ActorType.HUMAN) throw EafException.forbidden("只有 HUMAN 可以撤回学习发布。");
        requireActor(actor, command.workspaceId(), "learning:withdraw");
        requireActor(actor, command.workspaceId(), "learning:read");
        jdbc.query("select id from learning.candidate where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null,
                command.candidateId(), actor.tenantId(), command.workspaceId());
        var candidate = load(actor.tenantId(), command.workspaceId(), command.candidateId());
        if (!candidate.ownerId().equals(actor.actorId())) throw EafException.forbidden("只有候选 Owner 可以发起撤回。");
        if (candidate.rowVersion() != command.expectedVersion() || !"PUBLISHED".equals(candidate.status()))
            throw EafException.conflict("VERSION_CONFLICT", "只有当前已发布候选可以撤回。");
        var release = releaseByCandidate(actor.tenantId(), command.workspaceId(), candidate.id(), candidate.revision())
                .filter(item -> "RELEASED".equals(item.status()))
                .orElseThrow(() -> EafException.conflict("RELEASE_NOT_FOUND", "候选发布回执不存在或尚未完成。"));
        var requestHash = withdrawalRequestHash(candidate, release, command.reasonRef().trim(), actor.actorId());
        var prior = withdrawalByCandidate(actor.tenantId(), command.workspaceId(), candidate.id(), candidate.revision());
        if (prior.isPresent()) {
            var stored = jdbc.queryForObject("select request_hash from learning.candidate_withdrawal where id = ?",
                    String.class, prior.get().id());
            if (!requestHash.equals(stored))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "该候选修订已绑定不同撤回理由或命令。");
            if ("FAILED".equals(prior.get().status())) {
                jdbc.update("update learning.candidate_withdrawal set status = 'WITHDRAWING', failure_code = null, "
                                + "attempts = attempts + 1, updated_at = ? where id = ? and status = 'FAILED'",
                        Timestamp.from(Instant.now(clock)), prior.get().id());
                return new WithdrawalPreparation(candidate, release,
                        withdrawalById(actor.tenantId(), command.workspaceId(), prior.get().id()).orElseThrow());
            }
            return new WithdrawalPreparation(candidate, release, prior.get());
        }
        return new WithdrawalPreparation(candidate, release, null);
    }

    // 目标快照只读取正式资产；Learning 事务结束后才执行目标撤回命令。
    private WithdrawalTargetSnapshot withdrawalTargetSnapshot(ActorContext actor, UUID workspaceId,
                                                               LearningCandidate candidate, CandidateRelease release) {
        requireWithdrawalTargetAccess(actor, workspaceId, candidate);
        if ("KNOWLEDGE_UPDATE".equals(candidate.targetType())) {
            var origin = knowledge.findReleaseByOrigin(actor, workspaceId, candidate.id(), candidate.revision());
            if (!"PUBLISHED".equals(origin.action()) || !origin.eventId().equals(release.targetReleaseId())
                    || origin.assetVersion() != Integer.parseInt(release.targetVersion())
                    || !origin.contentHash().equals(release.targetContentHash()))
                throw EafException.conflict("RELEASE_MISMATCH", "Knowledge 发布历史与 Learning 收据不匹配。");
            var document = knowledge.get(actor, workspaceId, candidate.targetId());
            var version = knowledge.getVersion(actor, workspaceId, candidate.targetId(), origin.assetVersion());
            if (!"PUBLISHED".equals(version.status()))
                throw EafException.conflict("TARGET_ALREADY_WITHDRAWN", "Knowledge 版本已不可用。");
            return new WithdrawalTargetSnapshot(candidate.targetId(), release.targetVersion(), document.rowVersion());
        }
        if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType())) {
            var origin = findTeamTargetReceipt(actor, workspaceId, candidate);
            if (origin == null || !origin.releaseId().equals(release.targetReleaseId())
                    || !origin.targetVersion().equals(release.targetVersion())
                    || !origin.contentHash().equals(release.targetContentHash()))
                throw EafException.conflict("RELEASE_MISMATCH", "TEAM 候选发布回执与 Learning 收据不匹配。");
            var memory = memories.get(actor, workspaceId, candidate.targetId(), release.targetVersion());
            if (!"PUBLISHED".equals(memory.status()))
                throw EafException.conflict("TARGET_ALREADY_WITHDRAWN", "TEAM 候选版本已不可用。");
            return new WithdrawalTargetSnapshot(candidate.targetId(), release.targetVersion(), memory.rowVersion());
        }
        var origin = memories.findReleaseByOrigin(actor, workspaceId, candidate.id(), candidate.revision());
        if (!"PUBLISHED".equals(origin.action()) || !origin.releaseId().equals(release.targetReleaseId())
                || !origin.contentHash().equals(release.targetContentHash()))
            throw EafException.conflict("RELEASE_MISMATCH", "Memory 发布历史与 Learning 收据不匹配。");
        var memory = memories.get(actor, workspaceId, origin.memoryId(), origin.memoryVersion());
        if (!"PUBLISHED".equals(memory.status()))
            throw EafException.conflict("TARGET_ALREADY_WITHDRAWN", "Memory 版本已不可用。");
        return new WithdrawalTargetSnapshot(origin.memoryId(), origin.memoryVersion(), memory.rowVersion());
    }

    private CandidateWithdrawal saveWithdrawalIntent(CandidateWithdrawalCommand command,
                                                     WithdrawalPreparation preparation,
                                                     WithdrawalTargetSnapshot target) {
        var actor = command.actor();
        jdbc.query("select id from learning.candidate where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null,
                command.candidateId(), actor.tenantId(), command.workspaceId());
        var candidate = load(actor.tenantId(), command.workspaceId(), command.candidateId());
        if (candidate.rowVersion() != command.expectedVersion() || !"PUBLISHED".equals(candidate.status())
                || candidate.revision() != preparation.candidate().revision())
            throw EafException.conflict("VERSION_CONFLICT", "候选发布状态在创建撤回意图前已变化。");
        var hash = withdrawalRequestHash(candidate, preparation.release(), command.reasonRef().trim(), actor.actorId());
        var prior = withdrawalByCandidate(actor.tenantId(), command.workspaceId(), candidate.id(), candidate.revision());
        if (prior.isPresent()) {
            var stored = jdbc.queryForObject("select request_hash from learning.candidate_withdrawal where id = ?",
                    String.class, prior.get().id());
            if (!hash.equals(stored))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "该候选修订已绑定不同撤回理由或命令。");
            return prior.get();
        }
        var now = Timestamp.from(Instant.now(clock));
        var id = UUID.randomUUID();
        jdbc.update("insert into learning.candidate_withdrawal(id, tenant_id, workspace_id, candidate_id, "
                        + "candidate_revision, candidate_release_id, target_type, target_id, target_version, "
                        + "expected_target_row_version, request_hash, reason_ref, requested_by, status, attempts, created_at, updated_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WITHDRAWING', 1, ?, ?)",
                id, actor.tenantId(), command.workspaceId(), candidate.id(), candidate.revision(), preparation.release().id(),
                candidate.targetType(), target.targetId(), target.targetVersion(), target.expectedRowVersion(), hash,
                command.reasonRef().trim(), actor.actorId(), now, now);
        return withdrawalById(actor.tenantId(), command.workspaceId(), id).orElseThrow(EafException::notFound);
    }

    private TargetWithdrawalReceipt findTargetWithdrawal(ActorContext actor, UUID workspaceId,
                                                         LearningCandidate candidate, CandidateWithdrawal job) {
        try {
            if ("KNOWLEDGE_UPDATE".equals(job.targetType())) {
                var event = knowledge.findWithdrawalByOrigin(actor, workspaceId, candidate.id(), job.candidateRevision());
                if (!event.eventId().equals(job.targetWithdrawalId()) && job.targetWithdrawalId() != null)
                    throw EafException.conflict("WITHDRAWAL_MISMATCH", "Knowledge 撤回回执与本地作业不匹配。");
                if (!"REVOKED".equals(event.action()) || !job.targetId().equals(event.documentId())
                        || !job.targetVersion().equals(Integer.toString(event.assetVersion()))
                        || !event.contentHash().equals(jobTargetHash(job)))
                    throw EafException.conflict("WITHDRAWAL_MISMATCH", "Knowledge 撤回事实与候选版本不匹配。");
                return new TargetWithdrawalReceipt(event.eventId(), job.targetVersion(), event.contentHash());
            }
            if ("TEAM_EXPERIENCE_UPDATE".equals(job.targetType())) {
                var event = teamExperiences.findWithdrawalByOrigin(actor, workspaceId, candidate.id(), job.candidateRevision())
                        .orElse(null);
                if (event == null) return null;
                var memory = memories.get(actor, workspaceId, job.targetId(), job.targetVersion());
                var revision = teamExperiences.versions(actor, workspaceId, job.targetId(), event.revision() + 1, 10)
                        .stream().filter(item -> item.revision() == event.revision()).findFirst().orElse(null);
                if (!"REVOKE_CANDIDATE".equals(event.action()) || !job.targetId().equals(event.cardId())
                        || !job.targetVersion().equals(event.memoryVersion())
                        || revision == null || !candidate.id().equals(revision.originCandidateId())
                        || !Integer.valueOf(job.candidateRevision()).equals(revision.originCandidateRevision())
                        || !"REVOKED".equals(memory.status()) || !memory.contentHash().equals(jobTargetHash(job)))
                    throw EafException.conflict("WITHDRAWAL_MISMATCH", "TEAM 候选撤回事实与 Learning 收据不匹配。");
                return new TargetWithdrawalReceipt(teamWithdrawalReceiptId(candidate.id(), job.candidateRevision()),
                        job.targetVersion(), memory.contentHash());
            }
            var release = memories.findWithdrawalByOrigin(actor, workspaceId, candidate.id(), job.candidateRevision());
            if (!"REVOKED".equals(release.action()) || !job.targetId().equals(release.memoryId())
                    || !job.targetVersion().equals(release.memoryVersion())
                    || !release.contentHash().equals(jobTargetHash(job)))
                throw EafException.conflict("WITHDRAWAL_MISMATCH", "Memory 撤回事实与候选版本不匹配。");
            return new TargetWithdrawalReceipt(release.releaseId(), job.targetVersion(), release.contentHash());
        } catch (EafException notFound) {
            if (notFound.status() == 404) return null;
            throw notFound;
        }
    }

    private TargetWithdrawalReceipt revokeTarget(ActorContext actor, UUID workspaceId, LearningCandidate candidate,
                                                  CandidateWithdrawal job) {
        requireActor(actor, workspaceId, "learning:withdraw");
        if (actor.type() != ActorType.HUMAN || !candidate.ownerId().equals(actor.actorId()))
            throw EafException.forbidden("撤回必须由当前 HUMAN Owner 发起。");
        requireWithdrawalTargetAccess(actor, workspaceId, candidate);
        var current = load(actor.tenantId(), workspaceId, candidate.id());
        if (!"PUBLISHED".equals(current.status()) || current.revision() != job.candidateRevision())
            throw EafException.conflict("CANDIDATE_STATE_CONFLICT", "候选修订已变化，不能撤回旧发布。");
        if ("KNOWLEDGE_UPDATE".equals(candidate.targetType())) {
            var event = knowledge.revokeCandidateVersion(actor, workspaceId, job.targetId(),
                    Integer.parseInt(job.targetVersion()), jobExpectedVersion(job), candidate.id(), job.candidateRevision());
            return new TargetWithdrawalReceipt(event.eventId(), job.targetVersion(), event.contentHash());
        }
        if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType())) {
            workspaces.require(actor, workspaceId, "memory:scope:team");
            var event = teamExperiences.revokeCandidate(new TeamExperienceService.RevokeCandidateTeamExperience(
                    actor, workspaceId, candidate.id(), candidate.revision(),
                    "learning-candidate-withdrawal:" + candidate.id() + ":" + candidate.revision()));
            var memory = memories.get(actor, workspaceId, job.targetId(), event.memoryVersion());
            if (!"REVOKED".equals(memory.status()))
                throw EafException.conflict("WITHDRAWAL_MISMATCH", "Memory Owner 未撤回目标 TEAM 版本。");
            return new TargetWithdrawalReceipt(teamWithdrawalReceiptId(candidate.id(), candidate.revision()),
                    event.memoryVersion(), memory.contentHash());
        }
        var release = memories.revokeCandidate(actor, workspaceId, job.targetId(), job.targetVersion(),
                jobExpectedVersion(job), candidate.id(), job.candidateRevision());
        return new TargetWithdrawalReceipt(release.releaseId(), release.memoryVersion(), release.contentHash());
    }

    private long jobExpectedVersion(CandidateWithdrawal job) {
        return jdbc.queryForObject("select expected_target_row_version from learning.candidate_withdrawal where id = ?",
                Long.class, job.id());
    }

    private void requireWithdrawalTargetAccess(ActorContext actor, UUID workspaceId, LearningCandidate candidate) {
        if ("KNOWLEDGE_UPDATE".equals(candidate.targetType())) {
            workspaces.require(actor, workspaceId, "knowledge:read");
            workspaces.require(actor, workspaceId, "knowledge:write");
            workspaces.require(actor, workspaceId, "knowledge:publish");
        } else {
            workspaces.require(actor, workspaceId, "memory:read");
            workspaces.require(actor, workspaceId, "memory:write");
            workspaces.require(actor, workspaceId, "memory:publish");
            if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType()))
                workspaces.require(actor, workspaceId, "memory:scope:team");
        }
    }

    private String jobTargetHash(CandidateWithdrawal job) {
        return jdbc.queryForObject("select target_content_hash from learning.candidate_release where id = ?",
                String.class, job.candidateReleaseId());
    }

    private String withdrawalRequestHash(LearningCandidate candidate, CandidateRelease release,
                                         String reasonRef, UUID actorId) {
        return Hashing.sha256(candidate.id() + "|" + candidate.revision() + "|" + release.id() + "|"
                + release.targetReleaseId() + "|" + release.targetVersion() + "|" + candidate.contentHash()
                + "|" + reasonRef + "|" + actorId);
    }

    private CandidateWithdrawal finishWithdrawal(ActorContext actor, UUID workspaceId, CandidateWithdrawal job,
                                                 TargetWithdrawalReceipt receipt) {
        var result = transactions.execute(status -> {
            jdbc.update("update learning.candidate_withdrawal set status = 'WITHDRAWN', target_withdrawal_id = ?, "
                            + "failure_code = null, updated_at = ? where id = ? and status = 'WITHDRAWING'",
                    receipt.withdrawalId(), Timestamp.from(Instant.now(clock)), job.id());
            return withdrawalById(actor.tenantId(), workspaceId, job.id()).orElseThrow(EafException::notFound);
        });
        return result == null ? withdrawalById(actor.tenantId(), workspaceId, job.id()).orElseThrow(EafException::notFound) : result;
    }

    private void failWithdrawal(CandidateWithdrawal job, String failureCode) {
        jdbc.update("update learning.candidate_withdrawal set status = 'FAILED', failure_code = ?, updated_at = ? "
                        + "where id = ? and status = 'WITHDRAWING'",
                failureCode, Timestamp.from(Instant.now(clock)), job.id());
    }

    // 事务只负责冻结精确修订和保存意图；不把 Knowledge/Memory 调用放进 Learning 事务。
    private CandidateRelease prepareReleaseIntent(CandidateReleaseCommand command) {
        var actor = command.actor();
        if (actor == null || actor.type() != ActorType.HUMAN)
            throw EafException.forbidden("只有 HUMAN 可以发起学习资产发布。");
        requireActor(actor, command.workspaceId(), "learning:publish");
        requireActor(actor, command.workspaceId(), "learning:read");
        jdbc.query("select id from learning.candidate where id = ? and tenant_id = ? and workspace_id = ? for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null,
                command.candidateId(), actor.tenantId(), command.workspaceId());
        var candidate = load(actor.tenantId(), command.workspaceId(), command.candidateId());
        if (!candidate.ownerId().equals(actor.actorId())) throw EafException.forbidden("只有候选 Owner 可以发起发布。");
        var requestHash = releaseRequestHash(candidate, command.approvalId(), actor.actorId());
        var prior = releaseByCandidate(actor.tenantId(), command.workspaceId(), candidate.id(), candidate.revision());
        if (prior.isPresent()) {
            var existing = prior.get();
            var stored = jdbc.queryForObject("select request_hash from learning.candidate_release where id = ?",
                    String.class, existing.id());
            if (!requestHash.equals(stored))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "该候选修订已绑定不同批准或发布请求。");
            if (!"FAILED".equals(existing.status())) return existing;
        }
        if (candidate.rowVersion() != command.expectedVersion() || !"APPROVED".equals(candidate.status()))
            throw EafException.conflict("VERSION_CONFLICT", "候选必须处于当前已批准修订才能发布。");
        var approval = approvalById(actor.tenantId(), command.workspaceId(), command.approvalId());
        validateReleaseAuthorization(actor, command.workspaceId(), candidate, approval, null);
        requireCurrentTarget(actor, command.workspaceId(), candidate);
        if ("KNOWLEDGE_UPDATE".equals(candidate.targetType())) {
            workspaces.require(actor, command.workspaceId(), "knowledge:write");
            workspaces.require(actor, command.workspaceId(), "knowledge:publish");
            workspaces.require(actor, command.workspaceId(), "knowledge:read");
        } else {
            workspaces.require(actor, command.workspaceId(), "memory:write");
            workspaces.require(actor, command.workspaceId(), "memory:publish");
            workspaces.require(actor, command.workspaceId(), "memory:read");
            if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType()))
                workspaces.require(actor, command.workspaceId(), "memory:scope:team");
        }
        var now = Timestamp.from(Instant.now(clock));
        if (jdbc.update("update learning.candidate set status = 'PUBLISHING', row_version = row_version + 1, updated_at = ? where id = ? and tenant_id = ? and workspace_id = ? and current_revision = ? and row_version = ? and status = 'APPROVED'",
                now, candidate.id(), actor.tenantId(), command.workspaceId(), candidate.revision(), candidate.rowVersion()) != 1)
            throw EafException.conflict("VERSION_CONFLICT", "候选发布竞争失败。");
        if (prior.isPresent()) {
            jdbc.update("update learning.candidate_release set status = 'PUBLISHING', failure_code = null, attempts = attempts + 1, updated_at = ? where id = ? and status = 'FAILED'",
                    now, prior.get().id());
            return releaseById(actor.tenantId(), command.workspaceId(), prior.get().id()).orElseThrow(EafException::notFound);
        }
        var releaseId = UUID.randomUUID();
        jdbc.update("insert into learning.candidate_release(id, tenant_id, workspace_id, candidate_id, candidate_revision, approval_id, target_type, target_id, base_version, owner_id, candidate_content_hash, request_hash, status, attempts, created_at, updated_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PUBLISHING', 1, ?, ?)",
                releaseId, actor.tenantId(), command.workspaceId(), candidate.id(), candidate.revision(), approval.id(),
                candidate.targetType(), candidate.targetId(), candidate.baseVersion(), candidate.ownerId(),
                candidate.contentHash(), requestHash, now, now);
        return releaseById(actor.tenantId(), command.workspaceId(), releaseId).orElseThrow(EafException::notFound);
    }

    private void validateReleaseAuthorization(ActorContext actor, UUID workspaceId, LearningCandidate candidate,
                                              CandidateApproval approval, CandidateRelease job) {
        requireActor(actor, workspaceId, "learning:publish");
        if (actor.type() != ActorType.HUMAN || !candidate.ownerId().equals(actor.actorId()))
            throw EafException.forbidden("候选发布必须由当前 HUMAN Owner 发起。");
        if (approval == null || !"APPROVED".equals(approval.decision())
                || approval.candidateRevision() != candidate.revision()
                || !approval.targetType().equals(candidate.targetType())
                || !java.util.Objects.equals(approval.targetId(), candidate.targetId())
                || !java.util.Objects.equals(approval.baseVersion(), candidate.baseVersion())
                || !approval.ownerId().equals(candidate.ownerId())
                || !approval.candidateContentHash().equals(candidate.contentHash())
                || !"learning:approve".equals(approval.authorizationAction()))
            throw EafException.conflict("APPROVAL_MISMATCH", "批准记录与当前候选修订不匹配。");
        if (approval.validUntil() == null || !approval.validUntil().isAfter(Instant.now(clock)))
            throw EafException.conflict("APPROVAL_EXPIRED", "候选批准已过期，需要重新完成评测和批准。");
        if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType())) {
            if (!"TEAM_PREPARATION_HELD_OUT".equals(approval.evaluationReportKind())
                    || approval.evaluationReportId() == null)
                throw EafException.conflict("TEAM_APPROVAL_EVIDENCE_MISMATCH", "TEAM 发布必须绑定类型化 HELD_OUT 证据。");
            var evidence = evaluations.getTeamPreparationReleaseEvidence(actor, workspaceId, candidate.id(), candidate.revision());
            if (!evidence.current() || !"ELIGIBLE".equals(evidence.eligibility())
                    || !approval.evaluationReportId().equals(evidence.reportId())
                    || !approval.evaluationReportHash().equals(evidence.reportHash())
                    || !approval.evaluationConfigurationHash().equals(evidence.configurationHash())
                    || !approval.datasetHash().equals(evidence.datasetHash()))
                throw EafException.conflict("TEAM_APPROVAL_EVIDENCE_STALE", "TEAM 批准绑定的报告、数据集或人工语义复核已失效。");
        } else if (!"LEGACY_CANDIDATE_RISK".equals(approval.evaluationReportKind())) {
            throw EafException.conflict("CANDIDATE_EVALUATION_KIND_MISMATCH", "该候选目标不能使用 TEAM 专用评测证据。");
        }
    }

    private String releaseRequestHash(LearningCandidate candidate, UUID approvalId, UUID actorId) {
        return Hashing.sha256(candidate.id() + "|" + candidate.revision() + "|" + approvalId + "|"
                + actorId + "|" + candidate.contentHash() + "|" + candidate.targetType() + "|"
                + candidate.targetId() + "|" + candidate.baseVersion());
    }

    private TargetReceipt findTargetReceipt(ActorContext actor, UUID workspaceId, LearningCandidate candidate,
                                           CandidateRelease job) {
        if ("KNOWLEDGE_UPDATE".equals(candidate.targetType())) {
            KnowledgePublication publication;
            try { publication = knowledge.findReleaseByOrigin(actor, workspaceId, candidate.id(), candidate.revision()); }
            catch (EafException missing) { if (missing.status() == 404) return null; throw missing; }
            var expectedHash = Hashing.sha256(candidate.proposedContent().asText());
            if (!"PUBLISHED".equals(publication.action()) || !candidate.targetId().equals(publication.documentId())
                    || !candidate.baseVersion().equals(String.valueOf(publication.baseVersion()))
                    || !expectedHash.equals(publication.contentHash()))
                throw EafException.conflict("RELEASE_MISMATCH", "Knowledge 已存在的来源发布与批准快照不匹配。");
            return new TargetReceipt(publication.eventId(), Integer.toString(publication.assetVersion()),
                    publication.contentHash(), publication.documentId());
        }
        if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType()))
            return findTeamTargetReceipt(actor, workspaceId, candidate);
        MemoryRelease release;
        try { release = memories.findReleaseByOrigin(actor, workspaceId, candidate.id(), candidate.revision()); }
        catch (EafException missing) { if (missing.status() == 404) return null; throw missing; }
        var memory = memories.get(actor, workspaceId, release.memoryId(), release.memoryVersion());
        if (!"PUBLISHED".equals(release.action()) || !"LEARNING_CANDIDATE".equals(release.releaseOrigin())
                || !java.util.Objects.equals(release.baseVersion(), candidate.baseVersion())
                || (candidate.targetId() != null && !candidate.targetId().equals(release.memoryId()))
                || !matchesMemoryCandidate(memory, candidate))
            throw EafException.conflict("RELEASE_MISMATCH", "Memory 已存在的来源发布与批准快照不匹配。");
        return new TargetReceipt(release.releaseId(), release.memoryVersion(), release.contentHash(), release.memoryId());
    }

    private TargetReceipt findTeamTargetReceipt(ActorContext actor, UUID workspaceId, LearningCandidate candidate) {
        var receipt = teamExperiences.findReleaseByOrigin(actor, workspaceId, candidate.id(), candidate.revision())
                .orElse(null);
        if (receipt == null) return null;
        var proposed = candidate.proposedContent();
        var revision = teamExperiences.versions(actor, workspaceId, candidate.targetId(), receipt.revision() + 1, 10)
                .stream().filter(item -> item.revision() == receipt.revision()).findFirst()
                .orElseThrow(() -> EafException.conflict("RELEASE_MISMATCH", "TEAM 候选发布修订不存在。"));
        var memory = memories.get(actor, workspaceId, candidate.targetId(), receipt.memoryVersion());
        if (!"PUBLISH_CANDIDATE".equals(receipt.action()) || !candidate.targetId().equals(receipt.cardId())
                || receipt.revision() != candidate.baseSnapshot().path("baseRevision").asInt() + 1
                || !candidate.baseVersion().equals(candidate.baseSnapshot().path("baseMemoryVersion").asText())
                || !"PUBLISHED".equals(revision.status()) || !candidate.id().equals(revision.originCandidateId())
                || !Integer.valueOf(candidate.revision()).equals(revision.originCandidateRevision())
                || !proposed.path("title").asText().equals(revision.title())
                || !proposed.path("appliesWhen").asText().equals(revision.appliesWhen())
                || !proposed.path("content").asText().equals(revision.content())
                || !revision.contentHash().equals(memory.contentHash()) || !"PUBLISHED".equals(memory.status())
                || !candidate.ownerId().equals(memory.ownerId()) || !"TEAM".equals(memory.scope()))
            throw EafException.conflict("RELEASE_MISMATCH", "Memory TEAM 来源回执与候选批准快照不匹配。");
        return new TargetReceipt(teamReleaseReceiptId(candidate.id(), candidate.revision()), receipt.memoryVersion(),
                memory.contentHash(), candidate.targetId());
    }

    private UUID teamReleaseReceiptId(UUID candidateId, int revision) {
        return UUID.nameUUIDFromBytes(("team-candidate-release:" + candidateId + ":" + revision)
                .getBytes(StandardCharsets.UTF_8));
    }

    private UUID teamWithdrawalReceiptId(UUID candidateId, int revision) {
        return UUID.nameUUIDFromBytes(("team-candidate-withdrawal:" + candidateId + ":" + revision)
                .getBytes(StandardCharsets.UTF_8));
    }

    private TargetReceipt publishTarget(ActorContext actor, UUID workspaceId, LearningCandidate candidate) {
        if ("KNOWLEDGE_UPDATE".equals(candidate.targetType())) {
            workspaces.require(actor, workspaceId, "knowledge:write");
            workspaces.require(actor, workspaceId, "knowledge:publish");
            var document = knowledge.get(actor, workspaceId, candidate.targetId());
            var version = knowledge.createVersion(new CreateKnowledgeVersionCommand(actor, workspaceId,
                    candidate.targetId(), document.rowVersion(), candidate.proposedContent().asText(),
                    "learning-candidate:" + candidate.id() + ":" + candidate.revision()));
            knowledge.chunk(actor, workspaceId, candidate.targetId(), version.version(), "p3-plain-1");
            var build = knowledge.buildIndex(actor, workspaceId, candidate.targetId(), version.version(),
                    "p3-plain-1", "learning-candidate:" + candidate.id() + ":" + candidate.revision());
            if (!"READY".equals(build.status()))
                throw EafException.conflict("PUBLICATION_NOT_READY", "候选 Knowledge 索引尚未 READY。");
            var current = knowledge.get(actor, workspaceId, candidate.targetId());
            var publication = knowledge.publishCandidate(actor, workspaceId, candidate.targetId(), current.rowVersion(),
                    Integer.parseInt(candidate.baseVersion()), build.id(), candidate.id(), candidate.revision(),
                    Hashing.sha256(candidate.proposedContent().asText()));
            return new TargetReceipt(publication.eventId(), Integer.toString(publication.assetVersion()),
                    publication.contentHash(), publication.documentId());
        }
        if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType())) {
            workspaces.require(actor, workspaceId, "memory:scope:team");
            var proposed = candidate.proposedContent();
            var receipt = teamExperiences.publishCandidate(new TeamExperienceService.PublishCandidateTeamExperience(
                    actor, workspaceId, candidate.targetId(), candidate.id(), candidate.revision(),
                    candidate.baseSnapshot().path("baseRevision").asInt(), candidate.baseVersion(),
                    candidate.baseSnapshot().path("expectedCardVersion").asLong(),
                    proposed.path("title").asText(), proposed.path("appliesWhen").asText(),
                    proposed.path("content").asText(), Hashing.sha256(String.join("\u001f",
                    proposed.path("title").asText(), proposed.path("appliesWhen").asText(),
                    proposed.path("content").asText())),
                    "learning-candidate:" + candidate.id() + ":" + candidate.revision()));
            var revision = teamExperiences.versions(actor, workspaceId, receipt.cardId(), receipt.revision() + 1, 10)
                    .stream().filter(item -> item.revision() == receipt.revision()).findFirst()
                    .orElseThrow(() -> EafException.conflict("RELEASE_MISMATCH", "Memory Owner 未返回候选 TEAM 修订。"));
            var memory = memories.get(actor, workspaceId, receipt.cardId(), receipt.memoryVersion());
            var expectedWorkItemId = UUID.fromString(candidate.baseSnapshot().path("sourceProof").path("workItemId").asText());
            if (!"PUBLISHED".equals(revision.status()) || !candidate.id().equals(revision.originCandidateId())
                    || !Integer.valueOf(candidate.revision()).equals(revision.originCandidateRevision())
                    || !revision.title().equals(proposed.path("title").asText())
                    || !revision.appliesWhen().equals(proposed.path("appliesWhen").asText())
                    || !revision.content().equals(proposed.path("content").asText())
                    || revision.source() == null || !expectedWorkItemId.equals(revision.source().workItemId())
                    || !"PUBLISHED".equals(memory.status()) || !candidate.ownerId().equals(memory.ownerId()))
                throw EafException.conflict("RELEASE_MISMATCH", "Memory Owner 返回的 TEAM 修订与候选不匹配。");
            return new TargetReceipt(teamReleaseReceiptId(candidate.id(), candidate.revision()),
                    receipt.memoryVersion(), memory.contentHash(), receipt.cardId());
        }
        workspaces.require(actor, workspaceId, "memory:write");
        workspaces.require(actor, workspaceId, "memory:publish");
        var release = memories.publishCandidate(new MemoryCandidateReleaseCommand(actor, workspaceId, candidate.id(),
                candidate.revision(), candidate.targetId(), candidate.baseVersion(), candidate.proposedContent()));
        var memory = memories.get(actor, workspaceId, release.memoryId(), release.memoryVersion());
        if (!matchesMemoryCandidate(memory, candidate))
            throw EafException.conflict("RELEASE_MISMATCH", "Memory 目标 API 返回内容与批准修订不一致。");
        return new TargetReceipt(release.releaseId(), release.memoryVersion(), release.contentHash(), release.memoryId());
    }

    private boolean matchesMemoryCandidate(MemoryDefinition memory, LearningCandidate candidate) {
        var proposed = candidate.proposedContent();
        return memory.ownerId().equals(candidate.ownerId())
                && memory.logicalKey().equals(proposed.path("logicalKey").asText())
                && memory.type().equals(proposed.path("type").asText())
                && memory.scope().equals(proposed.path("scope").asText())
                && memory.content().equals(proposed.path("content").asText())
                && Double.compare(memory.confidence(), proposed.path("confidence").asDouble()) == 0
                && memory.expiresAt().toEpochMilli() == Instant.parse(proposed.path("expiresAt").asText()).toEpochMilli()
                && memory.sourceRef().equals(proposed.path("sourceRef").asText())
                && memory.evidenceRefs().equals(readCandidateRefs(proposed.path("evidenceRefs")))
                && java.util.Objects.equals(memory.businessEntityType(), nullableText(proposed, "businessEntityType"))
                && java.util.Objects.equals(memory.businessEntityId(), nullableText(proposed, "businessEntityId"));
    }

    private String nullableText(JsonNode node, String field) {
        return !node.has(field) || node.get(field).isNull() ? null : node.path(field).asText();
    }

    private List<String> readCandidateRefs(JsonNode node) {
        if (!node.isArray()) return List.of();
        var refs = new ArrayList<String>();
        node.forEach(value -> refs.add(value.asText()));
        return List.copyOf(refs);
    }

    private CandidateRelease finishRelease(CandidateRelease job, TargetReceipt receipt) {
        var result = transactions.execute(status -> {
            var now = Timestamp.from(Instant.now(clock));
            var changed = jdbc.update("update learning.candidate_release set status = 'RELEASED', target_release_id = ?, target_version = ?, target_content_hash = ?, failure_code = null, updated_at = ? where id = ? and status = 'PUBLISHING'",
                    receipt.releaseId(), receipt.targetVersion(), receipt.contentHash(), now, job.id());
            if (changed == 1 && jdbc.update("update learning.candidate set status = 'PUBLISHED', row_version = row_version + 1, updated_at = ? where id = ? and tenant_id = ? and workspace_id = ? and current_revision = ? and status = 'PUBLISHING'",
                    now, job.candidateId(), job.tenantId(), job.workspaceId(), job.candidateRevision()) != 1)
                throw EafException.conflict("CANDIDATE_STATE_CONFLICT", "目标已发布，但 Learning 候选状态更新失败。");
            if (changed == 1 && "TEAM_EXPERIENCE_UPDATE".equals(job.targetType()))
                jdbc.update("update learning.improvement_run set status = 'PUBLISHED', row_version = row_version + 1, "
                                + "updated_at = now(), next_poll_at = now() where candidate_id = ? and candidate_revision = ? "
                                + "and status in ('READY','APPROVED','PUBLISHING')",
                        job.candidateId(), job.candidateRevision());
            return releaseById(job.tenantId(), job.workspaceId(), job.id()).orElseThrow(EafException::notFound);
        });
        return result == null ? releaseById(job.tenantId(), job.workspaceId(), job.id()).orElseThrow(EafException::notFound) : result;
    }

    private void failRelease(CandidateRelease job, String failureCode) {
        var code = failureCode == null ? "TARGET_RELEASE_FAILED" : failureCode.substring(0, Math.min(80, failureCode.length()));
        var now = Timestamp.from(Instant.now(clock));
        jdbc.update("update learning.candidate_release set status = 'FAILED', failure_code = ?, updated_at = ? where id = ? and status = 'PUBLISHING'",
                code, now, job.id());
        if (!"RELEASE_MISMATCH".equals(code))
            jdbc.update("update learning.candidate set status = 'APPROVED', row_version = row_version + 1, updated_at = ? where id = ? and tenant_id = ? and workspace_id = ? and current_revision = ? and status = 'PUBLISHING'",
                    now, job.candidateId(), job.tenantId(), job.workspaceId(), job.candidateRevision());
    }

    private void requireCurrentTarget(ActorContext actor, UUID workspaceId, LearningCandidate candidate) {
        try {
            // 批准时验证报告对应的正式基线仍可用；最终发布入口仍须再次校验以消除并发漂移。
            if ("KNOWLEDGE_UPDATE".equals(candidate.targetType())) {
                var publication = knowledge.getCurrentPublication(actor, workspaceId, candidate.targetId());
                if (!"PUBLISHED".equals(publication.action())
                        || !Integer.toString(publication.assetVersion()).equals(candidate.baseVersion()))
                    throw EafException.conflict("TARGET_BASE_CHANGED", "Knowledge 当前发布版本已变化。");
            } else if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType())) {
                var target = teamExperiences.get(actor, workspaceId, candidate.targetId());
                var base = candidate.baseSnapshot();
                var owner = candidate.ownerId().equals(actor.actorId());
                var expectedRevision = base.path("baseRevision").asInt();
                var expectedCardVersion = base.path("expectedCardVersion").asLong();
                // 非 Owner 读取会隐藏草稿版本与真实 rowVersion；审批只核对共享的 active，发布时 Owner 再核对完整基线。
                if (!candidate.ownerId().equals(target.ownerId()) || (owner && (target.version() != expectedCardVersion
                        || target.latestRevision() != expectedRevision)) || target.active() == null
                        || !Integer.valueOf(expectedRevision).equals(target.activeRevision())
                        || !candidate.baseVersion().equals(target.active().memoryVersion())
                        || !target.active().expiresAt().isAfter(Instant.now(clock))
                        || !target.scenarioKey().equals(base.path("scenarioKey").asText()))
                    throw EafException.conflict("TARGET_BASE_CHANGED", "TEAM 基线版本、权限或有效期已变化。");
                var memory = memories.requireUsable(actor, workspaceId, candidate.targetId(), candidate.baseVersion());
                if (!"PUBLISHED".equals(memory.status()) || !"TEAM".equals(memory.scope()))
                    throw EafException.conflict("TARGET_BASE_CHANGED", "TEAM 基线不再是有效发布版本。");
            } else if (candidate.targetId() != null) {
                var memory = memories.requireUsable(actor, workspaceId, candidate.targetId(), candidate.baseVersion());
                if (!"PUBLISHED".equals(memory.status()))
                    throw EafException.conflict("TARGET_BASE_CHANGED", "Memory 基线不再可用。");
            } else {
                var proposal = candidate.proposedContent();
                var existing = memories.findApplicable(actor, workspaceId,
                        optionalText(proposal, "businessEntityType", 80), optionalText(proposal, "businessEntityId", 160));
                if (existing.stream().anyMatch(memory -> memory.logicalKey().equals(proposal.path("logicalKey").asText())))
                    throw EafException.conflict("TARGET_BASE_CHANGED", "同一 Memory 逻辑键已有当前发布版本。");
            }
        } catch (EafException inaccessibleOrStale) {
            if ("TARGET_BASE_CHANGED".equals(inaccessibleOrStale.code())) throw inaccessibleOrStale;
            if (inaccessibleOrStale.status() == 403) throw inaccessibleOrStale;
            throw EafException.conflict("TARGET_BASE_CHANGED", "评测基线已撤回、过期或不可见。");
        }
    }

    private CandidateContextSnapshotCommand candidateContextCommand(ActorContext actor, UUID workspaceId,
                                                                      LearningCandidate candidate) {
        if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType()))
            return teamCandidateContextCommand(actor, workspaceId, candidate);
        EnterpriseContext baseline;
        String candidateText;
        ContextItem candidateItem;
        if ("KNOWLEDGE_UPDATE".equals(candidate.targetType())) {
            var base = json.convertValue(candidate.baseSnapshot(), KnowledgeDocument.class);
            candidateText = candidate.proposedContent().asText();
            baseline = experimentContext(candidateItem("baseline-" + candidate.id() + "-r" + candidate.revision(),
                    base.sourceRef(), base.contentHash(), base.content(), candidate.scope(), List.of(), null, null, null));
            candidateItem = candidateItem("candidate-" + candidate.id() + "-r" + candidate.revision(), base.sourceRef(),
                    Hashing.sha256(candidateText), candidateText, candidate.scope(), List.of(), null, null, null);
        } else {
            var proposed = candidate.proposedContent();
            candidateText = proposed.path("content").asText();
            if (candidate.baseSnapshot() == null) baseline = emptyExperimentContext();
            else {
                var base = json.convertValue(candidate.baseSnapshot(), MemoryDefinition.class);
                baseline = experimentContext(candidateItem("baseline-" + candidate.id() + "-r" + candidate.revision(),
                        base.sourceRef(), base.contentHash(), base.content(), base.scope(), base.evidenceRefs(),
                        base.expiresAt(), base.businessEntityType(), base.businessEntityId()));
            }
            candidateItem = candidateItem("candidate-" + candidate.id() + "-r" + candidate.revision(),
                    proposed.path("sourceRef").asText(), Hashing.sha256(candidateText), candidateText,
                    proposed.path("scope").asText(), stringValues(proposed.path("evidenceRefs")),
                    Instant.parse(proposed.path("expiresAt").asText()),
                    optionalText(proposed, "businessEntityType", 80), optionalText(proposed, "businessEntityId", 160));
        }
        var candidateContext = experimentContext(candidateItem);
        return new CandidateContextSnapshotCommand(actor, workspaceId, candidate.id(), candidate.revision(),
                candidate.targetType(), candidate.targetId(), candidate.baseVersion(), candidate.ownerId(), candidate.scope(),
                candidate.contentHash(), baseline, candidateContext);
    }

    private CandidateContextSnapshotCommand teamCandidateContextCommand(ActorContext actor, UUID workspaceId,
                                                                         LearningCandidate candidate) {
        var baseSnapshot = candidate.baseSnapshot();
        var base = baseSnapshot.path("base");
        var cardId = candidate.targetId();
        var baseRevision = baseSnapshot.path("baseRevision").asInt();
        var baseMemoryVersion = baseSnapshot.path("baseMemoryVersion").asText();
        var cardVersion = baseSnapshot.path("expectedCardVersion").asLong();
        var scenarioKey = baseSnapshot.path("scenarioKey").asText();
        var expiresAt = Instant.parse(base.path("expiresAt").asText());
        var sourceProof = baseSnapshot.path("sourceProof");
        var sourceWorkItemId = UUID.fromString(sourceProof.path("workItemId").asText());
        var current = teamExperiences.get(actor, workspaceId, cardId);
        if (!candidate.ownerId().equals(current.ownerId()) || current.version() != cardVersion
                || current.active() == null || current.activeRevision() == null
                || current.activeRevision() != baseRevision || current.active().revision() != baseRevision
                || !current.active().memoryVersion().equals(baseMemoryVersion)
                || !current.scenarioKey().equals(scenarioKey) || !current.active().expiresAt().equals(expiresAt)
                || !current.active().source().workItemId().equals(sourceWorkItemId))
            throw EafException.conflict("TEAM_EXPERIENCE_BASE_CHANGED", "候选基线在事实审核期间已变化。");
        var memory = memories.requireUsable(actor, workspaceId, cardId, baseMemoryVersion);
        var baselineText = "团队经验：" + base.path("title").asText() + "\n适用条件："
                + base.path("appliesWhen").asText() + "\n建议：" + base.path("content").asText();
        var baselineUsage = new TeamExperienceUsage.Included(cardId, baseRevision, memory.id(),
                memory.version(), memory.contentHash());
        var baselineItem = new ContextItem("team-experience-1", "MEMORY", null, 0, null, null,
                memory.id(), memory.version(), memory.sourceRef(), memory.contentHash(), baselineText, null,
                estimateTokens(baselineText), "TEAM", memory.evidenceRefs(), memory.expiresAt(),
                "SERVICE_REQUEST", scenarioKey);
        var baseline = teamExperienceContext(baselineItem, baselineUsage);

        var proposed = candidate.proposedContent();
        var proposedText = "团队经验：" + proposed.path("title").asText() + "\n适用条件："
                + proposed.path("appliesWhen").asText() + "\n建议：" + proposed.path("content").asText();
        var pseudoVersion = "candidate:" + candidate.id() + ":r" + candidate.revision();
        var candidateUsage = new TeamExperienceUsage.Included(cardId, baseRevision + 1, cardId, pseudoVersion,
                Hashing.sha256(proposedText));
        var candidateItem = new ContextItem("team-experience-1", "CANDIDATE", null, 0, null, null,
                cardId, pseudoVersion, "learning-candidate:" + candidate.id(), Hashing.sha256(proposedText),
                proposedText, null, estimateTokens(proposedText), "TEAM", List.of("candidate:" + candidate.id()),
                expiresAt, "SERVICE_REQUEST", scenarioKey);
        var candidateContext = teamExperienceContext(candidateItem, candidateUsage);
        var run = jdbc.query("select b.improvement_run_id, i.quality_run_id from learning.team_candidate_binding b "
                        + "join learning.improvement_run i on i.id = b.improvement_run_id "
                        + "where b.candidate_id = ? and b.candidate_revision = ? and i.owner_id = ? "
                        + "and i.card_id = ? and i.status = 'AWAITING_REVIEW'",
                rs -> rs.next() ? new UUID[]{rs.getObject("improvement_run_id", UUID.class),
                        rs.getObject("quality_run_id", UUID.class)} : null,
                candidate.id(), candidate.revision(), candidate.ownerId(), cardId);
        if (run == null) throw EafException.conflict("IMPROVEMENT_RUN_CHANGED", "候选已失去改进运行绑定。");
        var binding = new TeamPreparationSnapshotBinding(run[0], run[1], cardId, baseRevision,
                baseMemoryVersion, cardVersion, scenarioKey, expiresAt, sourceWorkItemId,
                Hashing.sha256(writeJson(candidate.evidenceRefs())));
        return new CandidateContextSnapshotCommand(actor, workspaceId, candidate.id(), candidate.revision(),
                candidate.targetType(), cardId, baseMemoryVersion, candidate.ownerId(), "TEAM", candidate.contentHash(),
                baseline, candidateContext, binding);
    }

    private EnterpriseContext teamExperienceContext(ContextItem item, TeamExperienceUsage.Included usage) {
        return new EnterpriseContext("READY", 1, 2_000, item.estimatedTokens(), 0, null, List.of(item), null,
                new TeamExperienceUsage(List.of(usage)));
    }

    private UUID teamCandidateSnapshotId(ActorContext actor, UUID workspaceId, LearningCandidate candidate) {
        var id = jdbc.query("select id from evaluation.candidate_context_snapshot where tenant_id = ? and workspace_id = ? "
                        + "and candidate_id = ? and candidate_revision = ? and target_type = 'TEAM_EXPERIENCE_UPDATE' "
                        + "and invalidated_at is null",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null,
                actor.tenantId(), workspaceId, candidate.id(), candidate.revision());
        if (id == null) throw EafException.conflict("TEAM_SNAPSHOT_MISSING", "候选隔离快照未成功保存。");
        return id;
    }

    private ContextItem candidateItem(String citationId, String sourceRef, String contentHash, String content,
                                      String scope, List<String> evidenceRefs, Instant expiresAt,
                                      String entityType, String entityId) {
        var tokens = estimateTokens(content);
        return new ContextItem(citationId, "CANDIDATE", null, 0, null, null, null, null, sourceRef, contentHash,
                content, null, tokens, scope, evidenceRefs, expiresAt, entityType, entityId);
    }

    private EnterpriseContext experimentContext(ContextItem item) {
        var tokens = item.estimatedTokens();
        if (tokens > 2_000) throw EafException.invalid("候选完整正文超过隔离评测上下文的 2,000 token 上限。");
        // ponytail: 完整正文按 code point/4 估算并限制 2,000 token；需评测大文档时再走统一候选检索/切块。
        return new EnterpriseContext("READY", 1, 2_000, tokens, 0, null, List.of(item));
    }

    private EnterpriseContext emptyExperimentContext() {
        return new EnterpriseContext("NO_EVIDENCE", 1, 2_000, 0, 0, null, List.of());
    }

    private int estimateTokens(String content) {
        return Math.max(1, (content.codePointCount(0, content.length()) + 3) / 4);
    }

    private List<String> stringValues(JsonNode node) {
        var result = new ArrayList<String>();
        if (node != null && node.isArray()) node.forEach(value -> { if (value.isTextual()) result.add(value.asText()); });
        return List.copyOf(result);
    }

    private CandidateDraft fromFeedback(ActorContext actor, UUID workspaceId, Feedback feedback, ContextSourceRef ref) {
        var evidenceRefs = List.of("feedback:" + feedback.id(), "task:" + feedback.taskId(), "context:" + ref.citationId());
        if ("KNOWLEDGE_UPDATE".equals(sourceType(ref))) {
            workspaces.require(actor, workspaceId, "knowledge:write");
            var current = knowledge.getCurrentPublication(actor, workspaceId, ref.documentId());
            if (current.assetVersion() != ref.documentVersion() || !current.contentHash().equals(ref.contentHash()))
                throw EafException.conflict("BASE_VERSION_CHANGED", "反馈来源 Knowledge 已不再是当前发布版本。");
            var base = knowledge.getVersion(actor, workspaceId, ref.documentId(), ref.documentVersion());
            var after = base.content() + "\n\n[待核实反馈 " + feedback.id() + "]\n" + feedback.correction();
            if (after.getBytes(StandardCharsets.UTF_8).length > 100 * 1024) throw EafException.invalid("候选 Knowledge 草稿超过正文限制。");
            var baseNode = json.valueToTree(base);
            return new CandidateDraft("KNOWLEDGE_UPDATE", base.id(), Integer.toString(base.version()), base.ownerId(),
                    "WORKSPACE", baseNode, TextNode.valueOf(after), evidenceRefs, true);
        }
        workspaces.require(actor, workspaceId, "memory:write");
        var base = memories.requireUsable(actor, workspaceId, ref.memoryId(), ref.memoryVersion());
        if (!base.contentHash().equals(ref.contentHash())) throw EafException.conflict("BASE_VERSION_CHANGED", "反馈来源 Memory 摘要已变化。");
        if ("TEAM".equals(base.scope())) throw EafException.forbidden("团队经验卡只能由显式改进运行提出候选。");
        var proposed = memoryContent(base, base.content() + "\n\n[待核实反馈 " + feedback.id() + "]\n" + feedback.correction(),
                evidenceRefs);
        validateMemoryContent(actor, workspaceId, proposed, base);
        if (proposed.path("content").asText().length() > 8_000) throw EafException.invalid("候选 Memory 草稿超过正文限制。");
        return new CandidateDraft("MEMORY_UPSERT", base.id(), base.version(), base.ownerId(), base.scope(),
                json.valueToTree(base), proposed, evidenceRefs, true);
    }

    private CandidateDraft prepare(ActorContext actor, UUID workspaceId, String targetType, UUID targetId,
                                   String baseVersion, JsonNode proposed, List<String> refs, boolean evidenceGap) {
        if ("KNOWLEDGE_UPDATE".equals(targetType)) {
            if (targetId == null || blank(baseVersion) || !baseVersion.matches("[1-9][0-9]*"))
                throw EafException.invalid("Knowledge 候选必须绑定文档及正整数 baseVersion。");
            workspaces.require(actor, workspaceId, "knowledge:write");
            var current = knowledge.getCurrentPublication(actor, workspaceId, targetId);
            int version;
            try { version = Integer.parseInt(baseVersion); }
            catch (NumberFormatException invalidVersion) { throw EafException.invalid("Knowledge baseVersion 超出支持范围。"); }
            if (current.assetVersion() != version) throw EafException.conflict("BASE_VERSION_CHANGED", "候选 Knowledge baseVersion 已变化。");
            var base = knowledge.getVersion(actor, workspaceId, targetId, version);
            if (!"PUBLISHED".equals(base.status()) || !base.contentHash().equals(current.contentHash()))
                throw EafException.conflict("BASE_VERSION_CHANGED", "候选 Knowledge 基线不再可用。");
            if (proposed == null || !proposed.isTextual() || blank(proposed.asText())
                    || proposed.asText().getBytes(StandardCharsets.UTF_8).length > 100 * 1024)
                throw EafException.invalid("Knowledge 候选必须提供限长的完整正文。");
            return new CandidateDraft(targetType, targetId, baseVersion, base.ownerId(), "WORKSPACE",
                    json.valueToTree(base), proposed.deepCopy(), refs, evidenceGap);
        }
        if (!"MEMORY_UPSERT".equals(targetType)) throw EafException.invalid("候选目标类型不受支持。");
        workspaces.require(actor, workspaceId, "memory:write");
        if (targetId == null) {
            if (baseVersion != null) throw EafException.invalid("新 Memory 候选不能有 baseVersion。");
            var scope = validateMemoryContent(actor, workspaceId, proposed, null);
            return new CandidateDraft(targetType, null, null, actor.actorId(), scope, null, proposed.deepCopy(), refs, evidenceGap);
        }
        if (blank(baseVersion)) throw EafException.invalid("更新 Memory 候选必须绑定 baseVersion。");
        var base = memories.requireUsable(actor, workspaceId, targetId, baseVersion);
        if (!actor.actorId().equals(base.ownerId())) throw EafException.forbidden("只有 Memory Owner 可以提议修改该资产。");
        var scope = validateMemoryContent(actor, workspaceId, proposed, base);
        return new CandidateDraft(targetType, targetId, baseVersion, base.ownerId(), scope,
                json.valueToTree(base), proposed.deepCopy(), refs, evidenceGap);
    }

    private String validateMemoryContent(ActorContext actor, UUID workspaceId, JsonNode node, MemoryDefinition base) {
        if (node == null || !node.isObject()) throw EafException.invalid("Memory 候选内容必须是对象。");
        var names = new LinkedHashSet<String>();
        node.fieldNames().forEachRemaining(names::add);
        if (!MEMORY_FIELDS.containsAll(names)) throw EafException.invalid("Memory 候选包含未允许字段。");
        var logicalKey = text(node, "logicalKey", 120);
        if (!logicalKey.matches("[a-z0-9]+(?:[.-][a-z0-9]+)*")) throw EafException.invalid("Memory logicalKey 格式无效。");
        var type = text(node, "type", 20);
        if (!List.of("EPISODIC", "SEMANTIC", "PROCEDURAL", "PREFERENCE").contains(type)) throw EafException.invalid("Memory type 无效。");
        var scope = text(node, "scope", 20);
        if (!List.of("PERSONAL", "TEAM").contains(scope)) throw EafException.forbidden("当前只支持 PERSONAL 或 TEAM 范围。");
        if ("TEAM".equals(scope)) workspaces.require(actor, workspaceId, "memory:scope:team");
        var content = contentText(node, "content", 8_000);
        var confidence = node.path("confidence");
        if (!confidence.isNumber() || !Double.isFinite(confidence.asDouble()) || confidence.asDouble() < 0 || confidence.asDouble() > 1)
            throw EafException.invalid("Memory confidence 必须在 0 到 1 之间。");
        var expiry = node.path("expiresAt");
        try {
            if (!expiry.isTextual() || !Instant.parse(expiry.asText()).isAfter(Instant.now(clock))) throw new IllegalArgumentException();
        } catch (RuntimeException invalidExpiry) { throw EafException.invalid("Memory expiresAt 必须是未来 ISO 时间。"); }
        text(node, "sourceRef", 500);
        var memoryRefs = stringArray(node.path("evidenceRefs"), 1, 16, 500);
        if (memoryRefs.isEmpty()) throw EafException.invalid("Memory 候选必须提供 evidenceRefs。");
        var entityType = optionalText(node, "businessEntityType", 80);
        var entityId = optionalText(node, "businessEntityId", 160);
        if ((entityType == null) != (entityId == null)) throw EafException.invalid("业务实体类型和标识必须同时提供。");
        if (base != null && (!base.logicalKey().equals(logicalKey) || !base.scope().equals(scope)
                || !java.util.Objects.equals(base.businessEntityType(), entityType)
                || !java.util.Objects.equals(base.businessEntityId(), entityId)))
            throw EafException.conflict("CANDIDATE_SCOPE_CONFLICT", "更新 Memory 时不能改变逻辑键、范围或业务实体适用性。");
        return scope;
    }

    private ObjectNode memoryContent(MemoryDefinition memory, String content, List<String> extraRefs) {
        var result = json.createObjectNode();
        result.put("logicalKey", memory.logicalKey()); result.put("type", memory.type()); result.put("scope", memory.scope());
        result.put("content", content); result.put("confidence", memory.confidence()); result.put("expiresAt", memory.expiresAt().toString());
        result.put("sourceRef", memory.sourceRef());
        var refs = result.putArray("evidenceRefs");
        var unique = new LinkedHashSet<>(memory.evidenceRefs()); unique.addAll(extraRefs);
        unique.forEach(refs::add);
        if (memory.businessEntityType() != null) result.put("businessEntityType", memory.businessEntityType());
        if (memory.businessEntityId() != null) result.put("businessEntityId", memory.businessEntityId());
        return result;
    }

    private CandidateSubmission insertCandidate(ActorContext actor, UUID workspaceId, UUID feedbackId,
                                                String key, CandidateDraft draft) {
        var now = Timestamp.from(Instant.now(clock));
        var requestHash = Hashing.sha256(writeJson(new CandidateRequestHash(feedbackId, revisionHash(draft))));
        var id = UUID.randomUUID();
        var inserted = jdbc.update("insert into learning.candidate(id, tenant_id, workspace_id, proposer_id, source_feedback_id, current_revision, status, row_version, idempotency_key, request_hash, created_at, updated_at) values (?, ?, ?, ?, ?, 1, 'PROPOSED', 1, ?, ?, ?, ?) on conflict (tenant_id, workspace_id, proposer_id, idempotency_key) do nothing",
                id, actor.tenantId(), workspaceId, actor.actorId(), feedbackId, key, requestHash, now, now);
        if (inserted == 0) {
            var existing = jdbc.query("select id, request_hash from learning.candidate where tenant_id = ? and workspace_id = ? and proposer_id = ? and idempotency_key = ?",
                    rs -> rs.next() ? new ExistingCandidate(rs.getObject("id", UUID.class), rs.getString("request_hash")) : null,
                    actor.tenantId(), workspaceId, actor.actorId(), key);
            if (existing == null || !requestHash.equals(existing.requestHash()))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "候选幂等键已对应不同目标或内容。");
            return new CandidateSubmission(get(actor, workspaceId, existing.id()), false);
        }
        insertRevision(id, 1, draft, requestHash, now);
        return new CandidateSubmission(load(actor.tenantId(), workspaceId, id), true);
    }

    private void insertRevision(UUID candidateId, int revision, CandidateDraft draft, String hash, Timestamp now) {
        jdbc.update("insert into learning.candidate_revision(candidate_id, revision, target_type, target_id, base_version, owner_id, scope, base_snapshot, proposed_content, evidence_refs, evidence_gap, content_hash, created_at) values (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?, ?)",
                candidateId, revision, draft.targetType(), draft.targetId(), draft.baseVersion(), draft.ownerId(), draft.scope(),
                draft.baseSnapshot() == null ? null : writeJson(draft.baseSnapshot()), writeJson(draft.proposedContent()),
                writeJson(draft.evidenceRefs()), draft.evidenceGap(), hash, now);
    }

    private boolean claimSource(ActorContext actor, UUID workspaceId, UUID feedbackId, String targetType, UUID targetId) {
        return jdbc.update("insert into learning.candidate_source(tenant_id, workspace_id, feedback_id, target_type, target_id, outcome, processed_at) values (?, ?, ?, ?, ?, 'PROCESSING', ?) on conflict do nothing",
                actor.tenantId(), workspaceId, feedbackId, targetType, targetId, Timestamp.from(Instant.now(clock))) == 1;
    }

    private CandidateSourceResult existingSourceOutcome(ActorContext actor, UUID workspaceId, UUID feedbackId,
                                                         String targetType, UUID targetId) {
        return jdbc.query("select outcome, reason_code, candidate_id from learning.candidate_source where tenant_id = ? and workspace_id = ? and feedback_id = ? and target_type = ? and target_id = ?",
                rs -> {
                    if (!rs.next()) return new CandidateSourceResult(targetType, targetId, "SKIPPED", "SOURCE_STATE_MISSING", null);
                    var candidateId = rs.getObject("candidate_id", UUID.class);
                    var outcome = rs.getString("outcome");
                    return new CandidateSourceResult(targetType, targetId,
                            "CREATED".equals(outcome) ? "REPLAYED" : outcome, rs.getString("reason_code"), candidateId);
                }, actor.tenantId(), workspaceId, feedbackId, targetType, targetId);
    }

    private CandidateSourceResult skipSource(ActorContext actor, UUID workspaceId, UUID feedbackId,
                                             String targetType, UUID targetId, String reason) {
        jdbc.update("update learning.candidate_source set outcome = 'SKIPPED', reason_code = ?, processed_at = ? where tenant_id = ? and workspace_id = ? and feedback_id = ? and target_type = ? and target_id = ? and outcome = 'PROCESSING'",
                reason, Timestamp.from(Instant.now(clock)), actor.tenantId(), workspaceId, feedbackId, targetType, targetId);
        return new CandidateSourceResult(targetType, targetId, "SKIPPED", reason, null);
    }

    private Feedback requireFeedback(ActorContext actor, UUID workspaceId, UUID feedbackId) {
        var feedback = jdbc.query("select id, tenant_id, workspace_id, task_id, task_attempt, task_status, actor_id, source_type, correction, evidence, source_snapshot::text, created_at from learning.feedback where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? mapFeedback(rs) : null, feedbackId, actor.tenantId(), workspaceId);
        if (feedback == null || !"USER".equals(feedback.sourceType())) throw EafException.notFound();
        var task = tasks.get(actor, workspaceId, feedback.taskId());
        if (!"USER".equals(task.source()) || !terminal(task.status())) throw EafException.forbidden("反馈来源任务已不满足学习条件。");
        return feedback;
    }

    private CandidateService.ImprovementRun loadImprovementRun(ActorContext actor, UUID workspaceId, UUID runId) {
        var row = jdbc.query("select id, workspace_id, owner_id, card_id, base_revision, base_memory_version, expected_card_version, "
                        + "source_feedbacks::text, shared_correction, dataset_key, dataset_version, quality_run_id, generation_task_id, "
                        + "candidate_id, candidate_revision, dev_report_id, held_out_report_id, status, stop_reason, stop_requested, "
                        + "row_version, created_at, updated_at, deadline_at from learning.improvement_run "
                        + "where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new ImprovementRunRow(rs.getObject("id", UUID.class), rs.getObject("workspace_id", UUID.class),
                        rs.getObject("owner_id", UUID.class), rs.getObject("card_id", UUID.class), rs.getInt("base_revision"),
                        rs.getString("base_memory_version"), rs.getLong("expected_card_version"), rs.getString("source_feedbacks"),
                        rs.getString("shared_correction"), rs.getString("dataset_key"), rs.getString("dataset_version"),
                        rs.getObject("quality_run_id", UUID.class), rs.getObject("generation_task_id", UUID.class),
                        rs.getObject("candidate_id", UUID.class), rs.getObject("candidate_revision", Integer.class),
                        rs.getObject("dev_report_id", UUID.class), rs.getObject("held_out_report_id", UUID.class),
                        rs.getString("status"), rs.getString("stop_reason"), rs.getBoolean("stop_requested"),
                        rs.getLong("row_version"), rs.getTimestamp("created_at").toInstant(),
                        rs.getTimestamp("updated_at").toInstant(), rs.getTimestamp("deadline_at").toInstant()) : null,
                runId, actor.tenantId(), workspaceId);
        if (row == null || !actor.actorId().equals(row.ownerId())) throw EafException.notFound();
        try {
            List<CandidateService.ImprovementSourceRef> sources = json.readerForListOf(
                    CandidateService.ImprovementSourceRef.class).readValue(row.sourcesJson());
            return new CandidateService.ImprovementRun(row.id(), row.workspaceId(), row.ownerId(), row.cardId(),
                    row.baseRevision(), row.baseMemoryVersion(), row.expectedCardVersion(), sources, row.sharedCorrection(),
                    row.datasetKey(), row.datasetVersion(), row.qualityRunId(), row.generationTaskId(), row.candidateId(),
                    row.candidateRevision(), row.devReportId(), row.heldOutReportId(), row.status(), row.stopReason(),
                    row.stopRequested(), row.rowVersion(), row.createdAt(), row.updatedAt(), row.deadlineAt());
        } catch (Exception invalidJson) { throw new IllegalStateException("运行来源摘要无法读取。", invalidJson); }
    }

    private boolean isLinked(Feedback feedback, String targetType, UUID targetId) {
        return feedback.source().contextSources().stream().anyMatch(ref -> targetType.equals(sourceType(ref))
                && targetId.equals(sourceTargetId(ref)));
    }

    private void checkTargetReadable(ActorContext actor, UUID workspaceId, LearningCandidate candidate) {
        if (candidate.targetId() == null) return;
        if ("KNOWLEDGE_UPDATE".equals(candidate.targetType()))
            knowledge.getVersion(actor, workspaceId, candidate.targetId(), Integer.parseInt(candidate.baseVersion()));
        else if ("TEAM_EXPERIENCE_UPDATE".equals(candidate.targetType()))
            teamExperiences.get(actor, workspaceId, candidate.targetId());
        else memories.get(actor, workspaceId, candidate.targetId(), candidate.baseVersion());
    }

    private LearningCandidate load(UUID tenantId, UUID workspaceId, UUID candidateId) {
        var result = jdbc.query("select c.id, c.tenant_id, c.workspace_id, c.proposer_id, c.source_feedback_id, c.current_revision, c.status, c.row_version, c.created_at, c.updated_at, r.target_type, r.target_id, r.base_version, r.owner_id, r.scope, r.base_snapshot::text, r.proposed_content::text, r.evidence_refs::text, (r.evidence_gap and not exists (select 1 from learning.candidate_review cr where cr.candidate_id = c.id and cr.candidate_revision = c.current_revision and cr.decision = 'ACCEPTED')) as evidence_gap, r.content_hash from learning.candidate c join learning.candidate_revision r on r.candidate_id = c.id and r.revision = c.current_revision where c.id = ? and c.tenant_id = ? and c.workspace_id = ?",
                rs -> rs.next() ? mapCandidate(rs) : null, candidateId, tenantId, workspaceId);
        if (result == null) throw EafException.notFound();
        var reviews = jdbc.query("select candidate_revision, reviewer_id, decision, reason, fact_evidence_refs::text, created_at from learning.candidate_review where candidate_id = ? order by candidate_revision, created_at, id",
                (rs, row) -> mapReview(rs), candidateId);
        var approvals = jdbc.query("select id, candidate_revision, approver_id, decision, target_type, target_id, base_version, owner_id, scope, candidate_content_hash, evidence_hash, evaluation_report_id, evaluation_report_kind, evaluation_report_hash, evaluation_configuration_hash, dataset_hash, evaluation_summary::text, authorization_action, valid_until, reason, created_at from learning.candidate_approval where candidate_id = ? and tenant_id = ? and workspace_id = ? order by candidate_revision, created_at, id",
                (rs, row) -> mapApproval(rs), candidateId, tenantId, workspaceId);
        return new LearningCandidate(result.id(), result.tenantId(), result.workspaceId(), result.proposerId(),
                result.sourceFeedbackId(), result.revision(), result.rowVersion(), result.status(), result.targetType(),
                result.targetId(), result.baseVersion(), result.ownerId(), result.scope(), result.baseSnapshot(),
                result.proposedContent(), result.evidenceRefs(), result.evidenceGap(), result.contentHash(),
                reviews, approvals, result.createdAt(), result.updatedAt());
    }

    private CandidateApproval approvalById(UUID tenantId, UUID workspaceId, UUID approvalId) {
        return jdbc.query("select id, candidate_revision, approver_id, decision, target_type, target_id, base_version, owner_id, scope, candidate_content_hash, evidence_hash, evaluation_report_id, evaluation_report_kind, evaluation_report_hash, evaluation_configuration_hash, dataset_hash, evaluation_summary::text, authorization_action, valid_until, reason, created_at from learning.candidate_approval where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? mapApproval(rs) : null, approvalId, tenantId, workspaceId);
    }

    private java.util.Optional<CandidateRelease> releaseByCandidate(UUID tenantId, UUID workspaceId,
                                                                    UUID candidateId, int revision) {
        return jdbc.query("select id, tenant_id, workspace_id, candidate_id, candidate_revision, approval_id, target_type, target_id, base_version, status, target_release_id, target_version, target_content_hash, failure_code, attempts, created_at, updated_at from learning.candidate_release where tenant_id = ? and workspace_id = ? and candidate_id = ? and candidate_revision = ?",
                rs -> rs.next() ? java.util.Optional.of(mapRelease(rs)) : java.util.Optional.empty(),
                tenantId, workspaceId, candidateId, revision);
    }

    private java.util.Optional<CandidateRelease> releaseById(UUID tenantId, UUID workspaceId, UUID releaseId) {
        return jdbc.query("select id, tenant_id, workspace_id, candidate_id, candidate_revision, approval_id, target_type, target_id, base_version, status, target_release_id, target_version, target_content_hash, failure_code, attempts, created_at, updated_at from learning.candidate_release where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? java.util.Optional.of(mapRelease(rs)) : java.util.Optional.empty(),
                releaseId, tenantId, workspaceId);
    }

    private CandidateRelease mapRelease(ResultSet rs) throws SQLException {
        return new CandidateRelease(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getObject("candidate_id", UUID.class),
                rs.getInt("candidate_revision"), rs.getObject("approval_id", UUID.class), rs.getString("target_type"),
                rs.getObject("target_id", UUID.class), rs.getString("base_version"), rs.getString("status"),
                rs.getObject("target_release_id", UUID.class), rs.getString("target_version"),
                rs.getString("target_content_hash"), rs.getString("failure_code"), rs.getInt("attempts"),
                rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant());
    }

    private CandidateApproval mapApproval(ResultSet rs) throws SQLException {
        try {
            var summary = rs.getString("evaluation_summary");
            return new CandidateApproval(rs.getObject("id", UUID.class), rs.getInt("candidate_revision"),
                    rs.getObject("approver_id", UUID.class), rs.getString("decision"), rs.getString("target_type"),
                    rs.getObject("target_id", UUID.class), rs.getString("base_version"), rs.getObject("owner_id", UUID.class),
                    rs.getString("scope"), rs.getString("candidate_content_hash"), rs.getString("evidence_hash"),
                    rs.getObject("evaluation_report_id", UUID.class), rs.getString("evaluation_report_kind"),
                    rs.getString("evaluation_report_hash"),
                    rs.getString("evaluation_configuration_hash"), rs.getString("dataset_hash"),
                    summary == null ? null : json.readTree(summary), rs.getString("authorization_action"),
                    rs.getTimestamp("valid_until") == null ? null : rs.getTimestamp("valid_until").toInstant(),
                    rs.getString("reason"), rs.getTimestamp("created_at").toInstant());
        } catch (Exception invalidJson) { throw new IllegalStateException("候选批准绑定无法读取。", invalidJson); }
    }

    private java.util.Optional<CandidateWithdrawal> withdrawalByCandidate(UUID tenantId, UUID workspaceId,
                                                                          UUID candidateId, int revision) {
        return jdbc.query("select id, candidate_id, candidate_revision, candidate_release_id, target_type, target_id, "
                        + "target_version, target_withdrawal_id, status, reason_ref, failure_code, attempts, created_at, updated_at "
                        + "from learning.candidate_withdrawal where tenant_id = ? and workspace_id = ? "
                        + "and candidate_id = ? and candidate_revision = ?",
                rs -> rs.next() ? java.util.Optional.of(mapWithdrawal(rs)) : java.util.Optional.empty(),
                tenantId, workspaceId, candidateId, revision);
    }

    private java.util.Optional<CandidateWithdrawal> withdrawalById(UUID tenantId, UUID workspaceId, UUID id) {
        return jdbc.query("select id, candidate_id, candidate_revision, candidate_release_id, target_type, target_id, "
                        + "target_version, target_withdrawal_id, status, reason_ref, failure_code, attempts, created_at, updated_at "
                        + "from learning.candidate_withdrawal where tenant_id = ? and workspace_id = ? and id = ?",
                rs -> rs.next() ? java.util.Optional.of(mapWithdrawal(rs)) : java.util.Optional.empty(),
                tenantId, workspaceId, id);
    }

    private CandidateWithdrawal mapWithdrawal(ResultSet rs) throws SQLException {
        return new CandidateWithdrawal(rs.getObject("id", UUID.class), rs.getObject("candidate_id", UUID.class),
                rs.getInt("candidate_revision"), rs.getObject("candidate_release_id", UUID.class),
                rs.getString("target_type"), rs.getObject("target_id", UUID.class), rs.getString("target_version"),
                rs.getObject("target_withdrawal_id", UUID.class), rs.getString("status"), rs.getString("reason_ref"),
                rs.getString("failure_code"), rs.getInt("attempts"), rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    private LearningCandidate mapCandidate(ResultSet rs) throws SQLException {
        try {
            var base = rs.getString("base_snapshot");
            return new LearningCandidate(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                    rs.getObject("workspace_id", UUID.class), rs.getObject("proposer_id", UUID.class),
                    rs.getObject("source_feedback_id", UUID.class), rs.getInt("current_revision"), rs.getLong("row_version"),
                    rs.getString("status"), rs.getString("target_type"), rs.getObject("target_id", UUID.class),
                    rs.getString("base_version"), rs.getObject("owner_id", UUID.class), rs.getString("scope"),
                    base == null ? null : json.readTree(base), json.readTree(rs.getString("proposed_content")),
                    json.readerForListOf(String.class).readValue(rs.getString("evidence_refs")), rs.getBoolean("evidence_gap"),
                    rs.getString("content_hash"), List.of(), List.of(), rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant());
        } catch (Exception invalidJson) { throw new IllegalStateException("候选修订快照无法读取。", invalidJson); }
    }

    private CandidateReview mapReview(ResultSet rs) throws SQLException {
        try {
            return new CandidateReview(rs.getInt("candidate_revision"), rs.getObject("reviewer_id", UUID.class),
                    rs.getString("decision"), rs.getString("reason"),
                    json.readerForListOf(String.class).readValue(rs.getString("fact_evidence_refs")),
                    rs.getTimestamp("created_at").toInstant());
        } catch (Exception invalidJson) { throw new IllegalStateException("候选审核历史无法读取。", invalidJson); }
    }

    private Feedback mapFeedback(ResultSet rs) throws SQLException {
        try {
            return new Feedback(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                    rs.getObject("workspace_id", UUID.class), rs.getObject("task_id", UUID.class), rs.getInt("task_attempt"),
                    rs.getString("task_status"), rs.getObject("actor_id", UUID.class), rs.getString("source_type"),
                    rs.getString("correction"), rs.getString("evidence"), json.readValue(rs.getString("source_snapshot"), Feedback.Source.class),
                    rs.getTimestamp("created_at").toInstant());
        } catch (Exception invalidJson) { throw new IllegalStateException("反馈来源快照无法读取。", invalidJson); }
    }

    private void requireActor(ActorContext actor, UUID workspaceId, String action) {
        if (actor == null || actor.type() != ActorType.HUMAN) throw EafException.forbidden("组织学习只接受已认证 HUMAN 身份。");
        if (workspaceId == null) throw EafException.invalid("Workspace 必填。");
        workspaces.require(actor, workspaceId, action);
    }

    private List<String> validateRefs(List<String> refs) {
        if (refs == null || refs.isEmpty() || refs.size() > 16) throw EafException.invalid("证据引用需要 1 到 16 项。");
        var clean = new LinkedHashSet<String>();
        for (var ref : refs) {
            if (blank(ref) || ref.length() > 500 || hasControl(ref)) throw EafException.invalid("证据引用格式无效。");
            clean.add(ref.trim());
        }
        if (clean.size() != refs.size()) throw EafException.invalid("证据引用不能重复。");
        return List.copyOf(clean);
    }

    private List<String> stringArray(JsonNode node, int min, int max, int maxLength) {
        if (!node.isArray() || node.size() < min || node.size() > max) throw EafException.invalid("候选证据引用数量无效。");
        var result = new ArrayList<String>();
        for (var value : node) {
            if (!value.isTextual() || blank(value.asText()) || value.asText().length() > maxLength || hasControl(value.asText()))
                throw EafException.invalid("候选证据引用格式无效。");
            result.add(value.asText().trim());
        }
        if (new LinkedHashSet<>(result).size() != result.size()) throw EafException.invalid("候选证据引用不能重复。");
        return result;
    }

    private String text(JsonNode node, String name, int maxLength) {
        var value = node.path(name);
        if (!value.isTextual() || blank(value.asText()) || value.asText().length() > maxLength || hasControl(value.asText()))
            throw EafException.invalid("Memory 候选字段 " + name + " 无效。");
        return value.asText().trim();
    }

    private String contentText(JsonNode node, String name, int maxLength) {
        var value = node.path(name);
        if (!value.isTextual() || blank(value.asText()) || value.asText().length() > maxLength
                || value.asText().chars().anyMatch(ch -> ch < 0x20 && ch != '\n' && ch != '\r' && ch != '\t' || ch == 0x7f))
            throw EafException.invalid("Memory 候选正文无效。");
        return value.asText().trim();
    }

    private String optionalText(JsonNode node, String name, int maxLength) {
        var value = node.path(name);
        if (value.isMissingNode() || value.isNull()) return null;
        if (!value.isTextual() || blank(value.asText()) || value.asText().length() > maxLength || hasControl(value.asText()))
            throw EafException.invalid("Memory 候选字段 " + name + " 无效。");
        return value.asText().trim();
    }

    private String revisionHash(CandidateDraft draft) {
        return Hashing.sha256(writeJson(new RevisionHash(draft.targetType(), draft.targetId(), draft.baseVersion(),
                draft.ownerId(), draft.scope(), draft.baseSnapshot(), draft.proposedContent(), draft.evidenceRefs(), draft.evidenceGap())));
    }

    private String writeJson(Object value) {
        try { return json.writeValueAsString(value); }
        catch (JsonProcessingException invalid) { throw new IllegalStateException("候选内容无法序列化。", invalid); }
    }

    private String sourceType(ContextSourceRef ref) {
        if ("KNOWLEDGE".equals(ref.sourceType()) || ref.sourceType() == null) return "KNOWLEDGE_UPDATE";
        if ("MEMORY".equals(ref.sourceType())) return "MEMORY_UPSERT";
        return null;
    }

    private UUID sourceTargetId(ContextSourceRef ref) {
        return "MEMORY".equals(ref.sourceType()) ? ref.memoryId() : ref.documentId();
    }

    private boolean feedbackOnlyRef(String ref) { return ref.startsWith("feedback:") || ref.startsWith("task:"); }
    private boolean isSensitive(String content) { return SECRET.matcher(content).find(); }
    private boolean terminal(TaskStatus status) { return status == TaskStatus.SUCCEEDED || status == TaskStatus.FAILED || status == TaskStatus.TIMED_OUT || status == TaskStatus.CANCELLED; }
    private boolean blank(String value) { return value == null || value.isBlank(); }
    private boolean hasControl(String value) { return value.chars().anyMatch(ch -> ch < 0x20 || ch == 0x7f); }

    private record CandidateDraft(String targetType, UUID targetId, String baseVersion, UUID ownerId, String scope,
                                  JsonNode baseSnapshot, JsonNode proposedContent, List<String> evidenceRefs,
                                  boolean evidenceGap) { }
    private record ImprovementCandidateRun(UUID ownerId, UUID cardId, int baseRevision, String baseMemoryVersion,
                                          long expectedCardVersion, String sourceFeedbacks, String sharedCorrection,
                                          String sharedCorrectionHash, String generationInputHash, UUID qualityRunId,
                                          UUID generationTaskId, String status, UUID candidateId,
                                          Integer candidateRevision, long rowVersion) { }
    private record ImprovementRunRequestHash(UUID cardId, int baseRevision, long cardVersion,
            List<CandidateService.ImprovementSourceRef> sources, String sharedCorrection, String datasetKey,
            String datasetVersion, Instant deadlineAt) { }
    private record ImprovementRunKey(UUID id, String requestHash) { }
    private record ImprovementRunStop(UUID ownerId, UUID qualityRunId, UUID generationTaskId,
            UUID devReportId, UUID heldOutReportId, String status, long rowVersion, boolean stopRequested) { }
    private record GenerationPoll(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, UUID qualityRunId,
            UUID generationTaskId, UUID devReportId, UUID heldOutReportId, String status,
            boolean stopRequested, Instant deadlineAt) { }
    private record TeamReviewBinding(UUID improvementRunId, UUID qualityRunId, Instant deadlineAt,
            String datasetKey, String datasetVersion) { }
    private record ImprovementEvaluationPoll(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId,
            UUID qualityRunId, UUID generationTaskId, UUID candidateId, Integer candidateRevision,
            UUID devReportId, UUID heldOutReportId, String status, boolean stopRequested, Instant deadlineAt) { }
    private record ImprovementRunRow(UUID id, UUID workspaceId, UUID ownerId, UUID cardId, int baseRevision,
            String baseMemoryVersion, long expectedCardVersion, String sourcesJson, String sharedCorrection,
            String datasetKey, String datasetVersion, UUID qualityRunId, UUID generationTaskId, UUID candidateId,
            Integer candidateRevision, UUID devReportId, UUID heldOutReportId, String status, String stopReason,
            boolean stopRequested, long rowVersion, Instant createdAt, Instant updatedAt, Instant deadlineAt) { }
    private record ExistingCandidate(UUID id, String requestHash) { }
    private record CandidateRequestHash(UUID sourceFeedbackId, String revisionHash) { }
    private record RevisionHash(String targetType, UUID targetId, String baseVersion, UUID ownerId, String scope,
                                JsonNode baseSnapshot, JsonNode proposedContent, List<String> evidenceRefs,
                                boolean evidenceGap) { }
    private record TargetReceipt(UUID releaseId, String targetVersion, String contentHash, UUID targetId) { }
    private record WithdrawalPreparation(LearningCandidate candidate, CandidateRelease release,
                                         CandidateWithdrawal job) { }
    private record WithdrawalTargetSnapshot(UUID targetId, String targetVersion, long expectedRowVersion) { }
    private record TargetWithdrawalReceipt(UUID withdrawalId, String targetVersion, String contentHash) { }
}
// Learning 只保存受限的候选快照与审核记录；正式资产发布和撤回都经目标领域 API 完成。
