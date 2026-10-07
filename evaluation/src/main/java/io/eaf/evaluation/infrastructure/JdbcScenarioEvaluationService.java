package io.eaf.evaluation.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.agent.api.AgentDefinition;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.evaluation.api.ScenarioEvaluationService;
import io.eaf.evaluation.api.ScenarioEvaluationService.*;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.task.api.CreateQualityRunTaskCommand;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.usage.api.UsageRecord;
import io.eaf.usage.api.UsageRecorder;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** 的专用、顺序场景推进器；模型执行与租约仍由 Task Owner 负责。 */
@Service
public final class JdbcScenarioEvaluationService implements ScenarioEvaluationService {
    private static final String DATASET = "service-request-synthetic";
    private static final String SCENE = "service-request-analysis";
    private static final String PURPOSE = "SERVICE_REQUEST_SCENARIO";
    private static final UUID P15_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000010");
    private static final UUID P15_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000012");
    private final JdbcTemplate jdbc;
    private final AgentCatalog agents;
    private final CapabilityService capabilities;
    private final TaskService tasks;
    private final RuntimeQuery runtime;
    private final UsageRecorder usage;
    private final WorkspaceAuthorization workspaces;
    private final ScenarioContextManifest contextManifest;
    private final ObjectMapper json;
    private final Clock clock;
    private final TransactionTemplate tx;
    private final boolean enabled;

    public JdbcScenarioEvaluationService(JdbcTemplate jdbc, AgentCatalog agents, CapabilityService capabilities,
            TaskService tasks, RuntimeQuery runtime, UsageRecorder usage, WorkspaceAuthorization workspaces,
            ScenarioContextManifest contextManifest, ObjectMapper json, Clock clock,
            PlatformTransactionManager transactionManager,
            @Value("${eaf.evaluation.scenario-enabled:true}") boolean enabled) {
        this.jdbc = jdbc; this.agents = agents; this.capabilities = capabilities; this.tasks = tasks;
        this.runtime = runtime; this.usage = usage; this.workspaces = workspaces;
        this.contextManifest = contextManifest; this.json = json; this.clock = clock; this.enabled = enabled;
        this.tx = new TransactionTemplate(transactionManager);
        this.tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    }

    @Override
    public List<ScenarioDatasetSummary> listDatasets(ActorContext actor, UUID workspaceId) {
        requireDirect(actor, workspaceId, "evaluation:read");
        return jdbc.query("select d.dataset_key, d.dataset_version, d.scene_key, d.scoring_version, d.manifest_hash, "
                        + "count(*) filter (where c.split = 'DEV') as dev_count, "
                        + "count(*) filter (where c.split = 'HELD_OUT') as held_count "
                        + "from evaluation.scenario_dataset d join evaluation.scenario_case c "
                        + "on c.dataset_key = d.dataset_key and c.dataset_version = d.dataset_version "
                        + "where d.workspace_id = ? and d.status = 'ACTIVE' group by d.dataset_key, d.dataset_version, "
                        + "d.scene_key, d.scoring_version, d.manifest_hash order by d.dataset_key, d.dataset_version",
                (rs, row) -> new ScenarioDatasetSummary(rs.getString("dataset_key"), rs.getString("dataset_version"),
                        rs.getString("scene_key"), rs.getString("scoring_version"), rs.getInt("dev_count"),
                        rs.getInt("held_count"), rs.getString("manifest_hash")), workspaceId);
    }

    @Override
    public ScenarioRunResult createRun(ActorContext actor, UUID workspaceId, ScenarioRunRequest request,
                                       String idempotencyKey) {
        requireDirect(actor, workspaceId, "evaluation:run");
        if (request == null || idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200)
            throw EafException.invalid("运行请求或幂等键无效。");
        String requestHash = requestHash(request);
        var existing = findByKey(actor, workspaceId, idempotencyKey);
        if (existing != null) return replay(actor, workspaceId, existing, requestHash);
        if (!enabled) throw EafException.conflict("SCENARIO_EVALUATION_DISABLED", "场景评测当前未接纳新运行。");

        var now = Instant.now(clock);
        var deadline = request.deadlineAt() == null ? now.plus(Duration.ofMinutes(30)) : request.deadlineAt();
        if (!deadline.isAfter(now) || deadline.isAfter(now.plus(Duration.ofHours(24))))
            throw EafException.invalid("截止时间必须在当前时间之后且不超过 24 小时。");
        if (!DATASET.equals(request.datasetKey()) || blank(request.datasetVersion())
                || request.split() == null || !Set.of("DEV", "HELD_OUT").contains(request.split())
                || request.mode() == null || !Set.of("SINGLE", "PAIRED").contains(request.mode()))
            throw EafException.invalid("数据集、分区或运行模式不受支持。");
        var dataset = jdbc.query("select scene_key, scoring_version, manifest_hash from evaluation.scenario_dataset "
                        + "where dataset_key = ? and dataset_version = ? and workspace_id = ? and status = 'ACTIVE'",
                rs -> rs.next() ? new Dataset(rs.getString(1), rs.getString(2), rs.getString(3)) : null,
                request.datasetKey(), request.datasetVersion(), workspaceId);
        if (dataset == null || !SCENE.equals(dataset.sceneKey())) throw EafException.notFound();
        var allCases = jdbc.query("select c.case_id, c.split, c.request_text, c.input_hash, a.expected_category, a.expected_outcome, "
                        + "a.expected_evidence_refs::text, a.answer_hash "
                        + "from evaluation.scenario_case c left join evaluation.scenario_answer a using (dataset_key, dataset_version, case_id) "
                        + "where c.dataset_key = ? and c.dataset_version = ? order by c.case_id",
                (rs, row) -> new ScenarioCase(rs.getString("case_id"), rs.getString("split"), rs.getString("request_text"),
                        rs.getString("input_hash"), rs.getString("expected_category"), rs.getString("expected_outcome"),
                        rs.getString("expected_evidence_refs"), rs.getString("answer_hash")),
                request.datasetKey(), request.datasetVersion());
        long devCount = allCases.stream().filter(c -> "DEV".equals(c.split())).count();
        long heldOutCount = allCases.stream().filter(c -> "HELD_OUT".equals(c.split())).count();
        if (devCount < 1 || devCount > 20 || heldOutCount < 1 || heldOutCount > 20
                || allCases.stream().anyMatch(c -> c.answerHash() == null || c.expectedOutcome() == null
                || c.requestText().isBlank() || c.requestText().length() > 8000
                || !Hashing.sha256(c.requestText()).equals(c.inputHash())
                || !ScenarioDatasetHash.answer(json, c.expectedCategory(), c.expectedOutcome(), c.expectedEvidenceRefs()).equals(c.answerHash()))
                || !ScenarioDatasetHash.dataset(request.datasetKey(), request.datasetVersion(), dataset.scoringVersion(),
                allCases.stream().map(c -> new ScenarioDatasetHash.Entry(c.caseId(), c.split(), c.inputHash(), c.answerHash())).toList())
                .equals(dataset.manifestHash()))
            throw EafException.conflict("SCENARIO_DATASET_INVALID", "固定数据集的用例数量或输入摘要无效。");
        var cases = allCases.stream().filter(c -> request.split().equals(c.split())).toList();

        var baseline = resolveAsset(actor, workspaceId,
                request.baseline() == null ? null : request.baseline().capabilityId(),
                request.baseline() == null ? null : request.baseline().version());
        var comparison = "PAIRED".equals(request.mode())
                ? resolveAsset(actor, workspaceId, request.comparison() == null ? null : request.comparison().capabilityId(),
                        request.comparison() == null ? null : request.comparison().version()) : null;
        if ("SINGLE".equals(request.mode()) && request.comparison() != null)
            throw EafException.invalid("SINGLE 不接受 comparison 资产。");
        if (comparison != null && !compatible(baseline, comparison))
            throw EafException.invalid("PAIRED 仅支持相同服务请求协议、模型配置、检索模式和只读权限的已发布资产。");
        if (comparison != null && isP24Pair(baseline, comparison)
                && !sameP24Configuration(baseline, comparison))
            throw EafException.invalid("对照只允许呈现策略不同的固定资产组合。");
        var knowledge = contextManifest.capture(actor, workspaceId);
        var manifest = buildManifest(dataset, request, cases, baseline, comparison, knowledge, deadline);
        var manifestJson = write(manifest);
        var manifestHash = Hashing.sha256(manifestJson);

        return tx.execute(status -> {
            var proposed = UUID.randomUUID();
            jdbc.update("insert into evaluation.quality_run_registration(id, tenant_id, workspace_id, owner_id, idempotency_key, purpose, source, request_hash, status, created_at) "
                            + "values (?, ?, ?, ?, ?, ?, 'EVALUATION', ?, 'REGISTERED', ?) on conflict (tenant_id, workspace_id, owner_id, idempotency_key) do nothing",
                    proposed, actor.tenantId(), workspaceId, actor.actorId(), idempotencyKey, PURPOSE, requestHash,
                    Timestamp.from(now));
            var registration = jdbc.query("select id, purpose, source, request_hash from evaluation.quality_run_registration "
                            + "where tenant_id = ? and workspace_id = ? and owner_id = ? and idempotency_key = ? for update",
                    rs -> rs.next() ? new Registration(rs.getObject("id", UUID.class), rs.getString("purpose"),
                            rs.getString("source"), rs.getString("request_hash")) : null,
                    actor.tenantId(), workspaceId, actor.actorId(), idempotencyKey);
            if (registration == null || !PURPOSE.equals(registration.purpose())
                    || !"EVALUATION".equals(registration.source()) || !requestHash.equals(registration.requestHash()))
                throw EafException.conflict("SCENARIO_IDEMPOTENCY_CONFLICT", "幂等键已绑定不同运行请求。");
            if (!proposed.equals(registration.id())) return new ScenarioRunResult(loadReport(actor, workspaceId, registration.id()), false);
            jdbc.update("insert into evaluation.scenario_run(id, tenant_id, workspace_id, owner_id, idempotency_key, request_hash, "
                            + "dataset_key, dataset_version, split, mode, baseline_capability_id, baseline_capability_version, "
                            + "comparison_capability_id, comparison_capability_version, manifest, manifest_hash, status, deadline_at) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, 'QUEUED', ?)",
                    proposed, actor.tenantId(), workspaceId, actor.actorId(), idempotencyKey, requestHash,
                    request.datasetKey(), request.datasetVersion(), request.split(), request.mode(), baseline.capability().id(),
                    baseline.capability().version(), comparison == null ? null : comparison.capability().id(),
                    comparison == null ? null : comparison.capability().version(), manifestJson, manifestHash, Timestamp.from(deadline));
            for (var c : cases) {
                insertSample(proposed, c, "BASELINE", baseline);
                if (comparison != null) insertSample(proposed, c, "COMPARISON", comparison);
            }
            return new ScenarioRunResult(loadReport(actor, workspaceId, proposed), true);
        });
    }

    @Override
    public ScenarioRunReport getRun(ActorContext actor, UUID workspaceId, UUID runId) {
        requireOwner(actor, workspaceId, runId, "evaluation:read");
        return loadReport(actor, workspaceId, runId);
    }

    @Override
    public List<ScenarioSampleView> listSamples(ActorContext actor, UUID workspaceId, UUID runId, int limit) {
        requireOwner(actor, workspaceId, runId, "evaluation:read");
        if (limit < 1 || limit > 40) throw EafException.invalid("样本页大小必须为 1—40。");
        var current = sourceCurrent(actor, workspaceId, runId);
        return jdbc.query("select id, case_id, side, status, task_id, task_attempt, result_hash, error_code, scores::text, queue_ms, execution_ms "
                        + "from evaluation.scenario_sample where run_id = ? order by case_id, case when side = 'BASELINE' then 0 else 1 end limit ?",
                (rs, row) -> sampleView(rs, current), runId, limit);
    }

    @Override
    public ScenarioSampleEvidence getEvidence(ActorContext actor, UUID workspaceId, UUID runId, UUID sampleId) {
        requireDirect(actor, workspaceId, "evaluation:read");
        var row = jdbc.query("select r.owner_id, r.manifest::text, s.id, s.case_id, s.task_id, s.side "
                        + "from evaluation.scenario_run r join evaluation.scenario_sample s on s.run_id = r.id "
                        + "where r.id = ? and r.tenant_id = ? and r.workspace_id = ? and s.id = ?",
                rs -> rs.next() ? new EvidenceRow(rs.getObject("owner_id", UUID.class), rs.getString("manifest"),
                        rs.getObject("id", UUID.class), rs.getString("case_id"), rs.getObject("task_id", UUID.class),
                        rs.getString("side")) : null, runId, actor.tenantId(), workspaceId, sampleId);
        if (row == null || !actor.actorId().equals(row.ownerId())) throw EafException.notFound();
        if (row.taskId() == null || !manifestCurrent(actor, workspaceId, row.manifest()))
            return new ScenarioSampleEvidence(sampleId, row.caseId(), row.side(), null, null, true, List.of());
        var task = tasks.getScenarioEvaluationTask(actor, workspaceId, row.taskId());
        var sources = runtime.scenarioContextSourceContents(actor, workspaceId, row.taskId());
        if (sources.unavailable()) return new ScenarioSampleEvidence(sampleId, row.caseId(), row.side(), null, null, true, List.of());
        return new ScenarioSampleEvidence(sampleId, row.caseId(), row.side(), task.inputText(), task.resultJson(), false,
                sources.items().stream().map(item -> item.citationId()).filter(s -> s != null).toList());
    }

    @Override
    public ScenarioRunReport stopRun(ActorContext actor, UUID workspaceId, UUID runId, long expectedVersion) {
        requireOwner(actor, workspaceId, runId, "evaluation:run");
        var changed = jdbc.update("update evaluation.scenario_run set stop_requested = true, stop_reason = coalesce(stop_reason, 'HUMAN_STOP'), "
                        + "status = case when status in ('QUEUED','RUNNING') then 'STOPPING' else status end, version = version + 1, updated_at = now() "
                        + "where id = ? and tenant_id = ? and workspace_id = ? and owner_id = ? and version = ? "
                        + "and status in ('QUEUED','RUNNING','STOPPING')",
                runId, actor.tenantId(), workspaceId, actor.actorId(), expectedVersion);
        if (changed == 0) {
            var current = loadReport(actor, workspaceId, runId);
            if (current == null) throw EafException.notFound();
            if (current.version() != expectedVersion) throw EafException.conflict("VERSION_CONFLICT", "运行版本已变化。");
            throw EafException.conflict("SCENARIO_RUN_NOT_STOPPABLE", "运行已终结。");
        }
        stopAndCancel(actor.tenantId(), workspaceId, actor.actorId(), runId, "HUMAN_STOP");
        return loadReport(actor, workspaceId, runId);
    }

    @Override
    public ScenarioReviewResult review(ActorContext actor, UUID workspaceId, UUID runId, UUID sampleId,
                                       ScenarioReviewCommand command, String idempotencyKey) {
        requireOwner(actor, workspaceId, runId, "evaluation:review");
        if (command == null || idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200
                || !Set.of("CORRECT", "INCORRECT", "UNCERTAIN").contains(command.verdict())
                || !Set.of("CATEGORY", "OUTCOME", "CITATION", "ADVICE", "OTHER").contains(command.issueType())
                || command.comment() == null || command.comment().length() > 2000)
            throw EafException.invalid("人工复核字段无效。");
        var sample = jdbc.query("select s.task_id, s.task_attempt, s.result_hash, s.status, d.scoring_version "
                        + "from evaluation.scenario_sample s join evaluation.scenario_run r on r.id = s.run_id "
                        + "join evaluation.scenario_dataset d on d.dataset_key = r.dataset_key and d.dataset_version = r.dataset_version "
                        + "where s.id = ? and s.run_id = ? and r.owner_id = ? and r.tenant_id = ? and r.workspace_id = ?",
                rs -> rs.next() ? new ReviewSample(rs.getObject("task_id", UUID.class), rs.getInt("task_attempt"),
                        rs.getString("result_hash"), rs.getString("status"), rs.getString("scoring_version")) : null,
                sampleId, runId, actor.actorId(), actor.tenantId(), workspaceId);
        if (sample == null) throw EafException.notFound();
        if (!"SCORED".equals(sample.status()) || sample.resultHash() == null)
            throw EafException.conflict("SCENARIO_SAMPLE_NOT_REVIEWABLE", "只有已完成并留有输出摘要的样本可以复核。");
        if (!sourceCurrent(actor, workspaceId, runId)) throw EafException.conflict("SCENARIO_SOURCE_UNAVAILABLE", "来源已失效，不能复核该样本。");
        var requestHash = Hashing.sha256(String.join("\u001f", command.verdict(), command.issueType(), command.comment(),
                String.valueOf(command.supersedesReviewId())));
        return tx.execute(status -> {
            var prior = jdbc.query("select id, request_hash from evaluation.scenario_review where run_id = ? and sample_id = ? "
                            + "and reviewer_id = ? and idempotency_key = ?",
                    rs -> rs.next() ? new String[]{rs.getObject("id", UUID.class).toString(), rs.getString("request_hash")} : null,
                    runId, sampleId, actor.actorId(), idempotencyKey);
            if (prior != null) {
                if (!requestHash.equals(prior[1])) throw EafException.conflict("SCENARIO_REVIEW_IDEMPOTENCY_CONFLICT", "复核幂等键已绑定其他内容。");
                return new ScenarioReviewResult(loadReview(UUID.fromString(prior[0])), false);
            }
            var current = jdbc.query("select id, revision from evaluation.scenario_review where run_id = ? and sample_id = ? "
                            + "and reviewer_id = ? order by revision desc limit 1 for update",
                    rs -> rs.next() ? new Object[]{rs.getObject("id", UUID.class), rs.getInt("revision")} : null,
                    runId, sampleId, actor.actorId());
            int revision = current == null ? 1 : (int) current[1] + 1;
            UUID supersedes = current == null ? null : (UUID) current[0];
            if (!java.util.Objects.equals(supersedes, command.supersedesReviewId()))
                throw EafException.conflict("SCENARIO_REVIEW_REVISION_CONFLICT", "修订必须引用本人当前复核版本。");
            var id = UUID.randomUUID();
            jdbc.update("insert into evaluation.scenario_review(id, tenant_id, workspace_id, run_id, sample_id, reviewer_id, revision, "
                            + "supersedes_review_id, idempotency_key, request_hash, task_attempt, result_hash, rubric_version, verdict, issue_type, comment, self_reviewed) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true)",
                    id, actor.tenantId(), workspaceId, runId, sampleId, actor.actorId(), revision, supersedes,
                    idempotencyKey, requestHash, sample.taskAttempt(), sample.resultHash(), sample.rubricVersion(),
                    command.verdict(), command.issueType(), command.comment());
            return new ScenarioReviewResult(loadReview(id), true);
        });
    }

    @Override
    public List<ScenarioReview> listReviews(ActorContext actor, UUID workspaceId, UUID runId, UUID sampleId) {
        requireOwner(actor, workspaceId, runId, "evaluation:read");
        if (!sourceCurrent(actor, workspaceId, runId)) return List.of();
        return jdbc.query("select id from evaluation.scenario_review where run_id = ? and sample_id = ? "
                        + "order by reviewer_id, revision", (rs, row) -> loadReview(rs.getObject(1, UUID.class)), runId, sampleId);
    }

    @Override
    public void dispatchNext() {
        var claim = tx.execute(status -> claimRun());
        if (claim == null) return;
        try { tx.executeWithoutResult(status -> advance(claim.run(), claim.owner(), claim.fence())); }
        catch (RuntimeException failure) {
            if (failure instanceof EafException e && "TASK_CAPACITY_EXCEEDED".equals(e.code())) {
                jdbc.update("update evaluation.scenario_run set next_poll_at = now() + interval '5 seconds', lease_owner = null, lease_until = null "
                        + "where id = ? and lease_owner = ? and lease_fence = ?", claim.run().id(), claim.owner(), claim.fence());
                return;
            }
            // Task 创建和 sample 绑定共事务；失败后只在回滚完成后收敛运行，不遗留半条关联。
            stopAndCancel(claim.run().tenantId(), claim.run().workspaceId(), claim.run().ownerId(), claim.run().id(), safeError(failure));
            tx.executeWithoutResult(status -> finishRun(claim.run().id(), claim.owner(), claim.fence(), "FAILED", safeError(failure)));
        }
    }

    private ClaimedRun claimRun() {
        var run = jdbc.query("select id, tenant_id, workspace_id, owner_id, status, stop_requested, stop_reason, deadline_at, manifest::text, lease_fence "
                        + "from evaluation.scenario_run where status in ('QUEUED','RUNNING','STOPPING') and next_poll_at <= now() "
                        + "and (lease_until is null or lease_until < now()) order by created_at for update skip locked limit 1",
                rs -> rs.next() ? new DispatchRun(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("status"),
                        rs.getBoolean("stop_requested"), rs.getString("stop_reason"), rs.getTimestamp("deadline_at").toInstant(),
                        rs.getString("manifest"), rs.getLong("lease_fence")) : null);
        if (run == null) return null;
        var owner = UUID.randomUUID(); long fence = run.fence() + 1;
        jdbc.update("update evaluation.scenario_run set lease_owner = ?, lease_until = now() + interval '30 seconds', lease_fence = ?, "
                        + "status = case when status = 'QUEUED' then 'RUNNING' else status end, version = version + 1, updated_at = now() where id = ?",
                owner, fence, run.id());
        return new ClaimedRun(new DispatchRun(run.id(), run.tenantId(), run.workspaceId(), run.ownerId(),
                "QUEUED".equals(run.status()) ? "RUNNING" : run.status(), run.stopRequested(), run.stopReason(),
                run.deadlineAt(), run.manifest(), fence), owner, fence);
    }

    @Scheduled(fixedDelayString = "${eaf.evaluation.scenario-poll-delay-ms:1000}")
    void scheduledDispatch() { dispatchNext(); }

    private void advance(DispatchRun run, UUID lease, long fence) {
        if (!isLease(run.id(), lease, fence)) return;
        if (run.stopRequested() || "STOPPING".equals(run.status())) {
            settleStop(run, lease, fence); return;
        }
        if (!run.deadlineAt().isAfter(Instant.now(clock))) {
            requestStop(run.id(), "DEADLINE_EXCEEDED");
            stopAndCancel(run.tenantId(), run.workspaceId(), run.ownerId(), run.id(), "DEADLINE_EXCEEDED");
            settleStopped(run, lease, fence, "TIMED_OUT", "DEADLINE_EXCEEDED"); return;
        }
        var actor = internalActor(run.tenantId(), run.ownerId(), run.workspaceId());
        if (!manifestCurrent(actor, run.workspaceId(), run.manifest())) {
            requestStop(run.id(), "COMPARISON_INVALIDATED");
            stopAndCancel(run.tenantId(), run.workspaceId(), run.ownerId(), run.id(), "COMPARISON_INVALIDATED");
            settleStopped(run, lease, fence, "FAILED", "COMPARISON_INVALIDATED"); return;
        }
        var active = jdbc.query("select id, task_id, case_id, side, task_attempt from evaluation.scenario_sample "
                        + "where run_id = ? and status = 'ACTIVE' order by case_id, case when side = 'BASELINE' then 0 else 1 end limit 1",
                rs -> rs.next() ? new ActiveSample(rs.getObject("id", UUID.class), rs.getObject("task_id", UUID.class),
                        rs.getString("case_id"), rs.getString("side"), rs.getInt("task_attempt")) : null, run.id());
        if (active != null) {
            var task = tasks.getScenarioEvaluationTask(actor, run.workspaceId(), active.taskId());
            if (!terminal(task.status())) { defer(run.id(), lease, fence); return; }
            scoreSample(actor, run, active, task);
            defer(run.id(), lease, fence);
            return;
        }
        var pending = jdbc.query("select s.id, s.case_id, s.side, c.request_text, s.task_key "
                        + "from evaluation.scenario_sample s join evaluation.scenario_run r on r.id = s.run_id "
                        + "join evaluation.scenario_case c on c.dataset_key = r.dataset_key and c.dataset_version = r.dataset_version and c.case_id = s.case_id "
                        + "where s.run_id = ? and s.status = 'PENDING' order by s.case_id, case when s.side = 'BASELINE' then 0 else 1 end limit 1",
                rs -> rs.next() ? new PendingSample(rs.getObject("id", UUID.class), rs.getString("case_id"),
                        rs.getString("side"), rs.getString("request_text"), rs.getString("task_key")) : null, run.id());
        if (pending == null) { completeRun(run, lease, fence); return; }
        var asset = sampleAsset(pending.id());
        var deadline = run.deadlineAt();
        var task = tasks.createQualityRunTask(new CreateQualityRunTaskCommand(actor, run.workspaceId(), run.id(),
                asset.agent().id(), asset.agent().version(), pending.input(), null, null, pending.taskKey(),
                "p22-" + run.id(), "EVALUATION", pending.id(), asset.binding(), deadline));
        jdbc.update("update evaluation.scenario_sample set task_id = ?, task_attempt = ?, status = 'ACTIVE' "
                        + "where id = ? and status = 'PENDING' and task_id is null", task.id(), task.attempt(), pending.id());
        jdbc.update("update evaluation.quality_run_registration set status = 'RUNNING' where id = ? and status = 'REGISTERED'", run.id());
        defer(run.id(), lease, fence);
    }

    private void scoreSample(ActorContext actor, DispatchRun run, ActiveSample sample, TaskSnapshot task) {
        var timing = tasks.getScenarioEvaluationTiming(actor, run.workspaceId(), task.id(), task.attempt());
        if (task.status() == TaskStatus.CANCELLED) {
            jdbc.update("update evaluation.scenario_sample set status = 'CANCELLED', error_code = 'TASK_CANCELLED', queue_ms = ?, execution_ms = ?, completed_at = now() where id = ? and status = 'ACTIVE'",
                    timing == null ? null : timing.queueMillis(), timing == null ? null : timing.executionMillis(), sample.id());
            return;
        }
        if (task.status() == TaskStatus.TIMED_OUT) {
            jdbc.update("update evaluation.scenario_sample set status = 'TIMED_OUT', error_code = 'TASK_TIMED_OUT', queue_ms = ?, execution_ms = ?, completed_at = now() where id = ? and status = 'ACTIVE'",
                    timing == null ? null : timing.queueMillis(), timing == null ? null : timing.executionMillis(), sample.id());
            return;
        }
        var expected = jdbc.query("select a.expected_category, a.expected_outcome, a.expected_evidence_refs::text, a.answer_hash "
                        + "from evaluation.scenario_answer a join evaluation.scenario_run r on r.dataset_key = a.dataset_key and r.dataset_version = a.dataset_version "
                        + "join evaluation.scenario_sample s on s.run_id = r.id and s.case_id = a.case_id "
                        + "where r.id = ? and s.id = ?", rs -> rs.next() ? new Expected(rs.getString("expected_category"),
                        rs.getString("expected_outcome"), rs.getString("expected_evidence_refs"), rs.getString("answer_hash")) : null,
                run.id(), sample.id());
        if (expected == null || !ScenarioDatasetHash.answer(json, expected.category(), expected.outcome(), expected.evidenceRefsJson()).equals(expected.answerHash())
                || !answerHashBound(run.manifest(), sample.caseId(), expected.answerHash()))
            throw EafException.conflict("SCENARIO_ANSWER_BINDING_INVALID", "固定答案摘要与运行清单不一致。");
        var runtimeSources = runtime.scenarioContextSourceContents(actor, run.workspaceId(), task.id());
        if (runtimeSources.unavailable()) throw EafException.conflict("SCENARIO_SOURCE_UNAVAILABLE", "本次运行来源已失效。");
        var authorizedCitations = runtimeSources.items().stream().map(ref -> ref.citationId()).filter(v -> v != null).toList();
        var evidenceRefs = parseEvidenceRefs(expected.evidenceRefsJson());
        var steps = runtime.steps(actor, run.workspaceId(), task.id());
        int toolCalls = (int) steps.stream().filter(s -> "TOOL_CALL".equals(s.type()) || "TOOL_EXECUTION".equals(s.type())).count();
        int toolExecutions = (int) steps.stream().filter(s -> "TOOL_RESULT".equals(s.type())
                && "SUCCEEDED".equals(s.validation())).count();
        var scored = new ScenarioScorer(json).score(task.status().name(), task.resultJson(), expected.category(),
                expected.outcome(), authorizedCitations, evidenceRefs, toolCalls, toolExecutions);
        var scores = json.createObjectNode();
        scores.set("structure", json.valueToTree(scored.structure()));
        scores.set("category", json.valueToTree(scored.category()));
        scores.set("outcome", json.valueToTree(scored.outcome()));
        scores.set("citationBinding", json.valueToTree(scored.citationBinding()));
        scores.set("evidenceHit", json.valueToTree(scored.evidenceHit()));
        scores.set("readOnlySafety", json.valueToTree(scored.readOnlySafety()));
        scores.put("executionStatus", task.status().name());
        scores.put("toolCalls", toolCalls);
        scores.put("toolExecutions", toolExecutions);
        String resultHash = task.resultJson() == null ? null : Hashing.sha256(task.resultJson());
        String errorCode = task.status() == TaskStatus.SUCCEEDED ? null : task.errorCode();
        String sampleStatus = task.status() == TaskStatus.SUCCEEDED ? "SCORED" : "ERROR";
        jdbc.update("update evaluation.scenario_sample set status = ?, result_hash = ?, scores = ?::jsonb, error_code = ?, completed_at = now(), "
                        + "queue_ms = ?, execution_ms = ? where id = ? and status = 'ACTIVE'",
                sampleStatus, resultHash, write(scores), errorCode, timing == null ? null : timing.queueMillis(),
                timing == null ? null : timing.executionMillis(), sample.id());
        if (task.status() == TaskStatus.FAILED && fatal(task.errorCode())) {
            requestStop(run.id(), task.errorCode());
            stopAndCancel(run.tenantId(), run.workspaceId(), run.ownerId(), run.id(), task.errorCode());
        }
    }

    private void settleStop(DispatchRun run, UUID lease, long fence) {
        var taskId = jdbc.query("select task_id from evaluation.scenario_sample where run_id = ? and status = 'ACTIVE' limit 1",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, run.id());
        if (taskId != null) {
            var actor = internalActor(run.tenantId(), run.ownerId(), run.workspaceId());
            var current = tasks.getScenarioEvaluationTask(actor, run.workspaceId(), taskId);
            var sample = jdbc.query("select id, case_id, side, task_attempt from evaluation.scenario_sample where run_id = ? and task_id = ? and status = 'ACTIVE'",
                    rs -> rs.next() ? new ActiveSample(rs.getObject("id", UUID.class), taskId, rs.getString("case_id"),
                            rs.getString("side"), rs.getInt("task_attempt")) : null, run.id(), taskId);
            if (!terminal(current.status())) {
                try { tasks.cancelScenarioEvaluationTask(run.tenantId(), run.workspaceId(), run.ownerId(), taskId); }
                catch (RuntimeException ignored) { defer(run.id(), lease, fence); return; }
                jdbc.update("update evaluation.scenario_sample set status = 'CANCELLED', error_code = coalesce(error_code, 'HUMAN_STOP'), completed_at = now() where task_id = ? and status = 'ACTIVE'", taskId);
            } else if ("SUCCEEDED".equals(current.status().name())
                    && manifestCurrent(actor, run.workspaceId(), run.manifest()) && sample != null) {
                scoreSample(actor, run, sample, current);
            } else if (sample != null && manifestCurrent(actor, run.workspaceId(), run.manifest())) {
                scoreSample(actor, run, sample, current);
            } else if (sample != null) {
                jdbc.update("update evaluation.scenario_sample set status = 'CANCELLED', error_code = 'SOURCE_UNAVAILABLE', completed_at = now() where id = ? and status = 'ACTIVE'", sample.id());
            }
        }
        var reason = run.stopReason() == null ? "HUMAN_STOP" : run.stopReason();
        settleStopped(run, lease, fence, "STOPPED", reason);
    }

    private void settleStopped(DispatchRun run, UUID lease, long fence, String status, String reason) {
        jdbc.update("update evaluation.scenario_sample set status = 'NOT_RUN', error_code = ?, completed_at = now() where run_id = ? and status = 'PENDING'",
                reason, run.id());
        jdbc.update("update evaluation.scenario_run set status = ?, stop_reason = coalesce(stop_reason, ?), stop_requested = true, "
                        + "report_snapshot = '{}'::jsonb, version = version + 1, updated_at = now(), lease_owner = null, lease_until = null "
                        + "where id = ? and lease_owner = ? and lease_fence = ?",
                status, reason, run.id(), lease, fence);
        jdbc.update("update evaluation.quality_run_registration set status = ? where id = ? and status <> 'COMPLETED'",
                "FAILED".equals(status) || "TIMED_OUT".equals(status) ? "FAILED" : "FAILED", run.id());
    }

    private void completeRun(DispatchRun run, UUID lease, long fence) {
        var errors = jdbc.queryForObject("select count(*) from evaluation.scenario_sample where run_id = ? and status in ('ERROR','TIMED_OUT','CANCELLED','NOT_RUN')",
                Integer.class, run.id());
        String status = errors != null && errors > 0 ? "COMPLETED_WITH_ERRORS" : "COMPLETED";
        jdbc.update("update evaluation.scenario_run set status = ?, report_snapshot = '{}'::jsonb, version = version + 1, updated_at = now(), "
                        + "lease_owner = null, lease_until = null where id = ? and lease_owner = ? and lease_fence = ?",
                status, run.id(), lease, fence);
        jdbc.update("update evaluation.quality_run_registration set status = 'COMPLETED' where id = ?", run.id());
    }

    private void stopAndCancel(UUID tenantId, UUID workspaceId, UUID ownerId, UUID runId, String reason) {
        requestStop(runId, reason);
        usage.stopSpendScope(tenantId, workspaceId, "EVALUATION", runId, reason);
        var taskId = jdbc.query("select task_id from evaluation.scenario_sample where run_id = ? and status = 'ACTIVE' limit 1",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, runId);
        if (taskId != null) {
            try { tasks.cancelScenarioEvaluationTask(tenantId, workspaceId, ownerId, taskId); }
            catch (RuntimeException ignored) { /* 停止标记仍会阻止后续模型步骤和样本分派。 */ }
        }
    }

    private void requestStop(UUID runId, String reason) {
        jdbc.update("update evaluation.scenario_run set stop_requested = true, status = case when status in ('QUEUED','RUNNING') then 'STOPPING' else status end, "
                        + "stop_reason = coalesce(stop_reason, ?), version = version + 1, updated_at = now() where id = ? and status in ('QUEUED','RUNNING','STOPPING') "
                        + "and (not stop_requested or stop_reason is null)",
                reason, runId);
    }

    private void defer(UUID runId, UUID lease, long fence) {
        jdbc.update("update evaluation.scenario_run set next_poll_at = now() + interval '1 second', lease_owner = null, lease_until = null "
                        + "where id = ? and lease_owner = ? and lease_fence = ?", runId, lease, fence);
    }

    private void finishRun(UUID runId, UUID lease, long fence, String status, String reason) {
        jdbc.update("update evaluation.scenario_sample set status = 'NOT_RUN', error_code = ?, completed_at = now() where run_id = ? and status = 'PENDING'", reason, runId);
        jdbc.update("update evaluation.scenario_sample set status = 'ERROR', error_code = ?, completed_at = now() where run_id = ? and status = 'ACTIVE'", reason, runId);
        jdbc.update("update evaluation.scenario_run set status = ?, stop_requested = true, stop_reason = coalesce(stop_reason, ?), version = version + 1, updated_at = now(), lease_owner = null, lease_until = null "
                        + "where id = ? and lease_owner = ? and lease_fence = ?", status, reason, runId, lease, fence);
        jdbc.update("update evaluation.quality_run_registration set status = 'FAILED' where id = ?", runId);
    }

    private boolean isLease(UUID runId, UUID lease, long fence) {
        return jdbc.query("select id from evaluation.scenario_run where id = ? and lease_owner = ? and lease_fence = ? and lease_until > now() for update",
                (org.springframework.jdbc.core.ResultSetExtractor<Boolean>) rs -> rs.next(), runId, lease, fence);
    }

    private void insertSample(UUID runId, ScenarioCase c, String side, ResolvedAsset asset) {
        var sampleId = UUID.randomUUID();
        String taskKey = "p22:%s:%s:%s".formatted(runId, c.caseId(), side);
        jdbc.update("insert into evaluation.scenario_sample(id, run_id, case_id, side, input_hash, task_key, agent_id, agent_version, "
                        + "capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash, status) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')",
                sampleId, runId, c.caseId(), side, c.inputHash(), taskKey, asset.agent().id(), asset.agent().version(),
                asset.capability().id(), asset.capability().version(), asset.capability().contentHash(), asset.capability().skillId(),
                asset.capability().skillVersion(), asset.capability().skillContentHash());
    }

    private ResolvedAsset sampleAsset(UUID sampleId) {
        var row = jdbc.query("select capability_id, capability_version, agent_id, agent_version from evaluation.scenario_sample where id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("capability_id", UUID.class), rs.getString("capability_version"),
                        rs.getObject("agent_id", UUID.class), rs.getString("agent_version")} : null, sampleId);
        if (row == null) throw EafException.notFound();
        UUID capabilityId = (UUID) row[0]; String capabilityVersion = (String) row[1];
        UUID agentId = (UUID) row[2]; String agentVersion = (String) row[3];
        var run = jdbc.query("select tenant_id, workspace_id, owner_id from evaluation.scenario_run r join evaluation.scenario_sample s on s.run_id = r.id where s.id = ?",
                rs -> rs.next() ? new UUID[]{rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class)} : null,
                sampleId);
        if (run == null) throw EafException.notFound();
        var actor = internalActor(run[0], run[2], run[1]);
        var cap = capabilities.requirePublished(actor, run[1], capabilityId, capabilityVersion);
        var agent = agents.requirePublished(run[0], run[1], agentId, agentVersion);
        if (!cap.contentHash().equals(jdbc.queryForObject("select capability_hash from evaluation.scenario_sample where id = ?", String.class, sampleId)))
            throw EafException.conflict("SCENARIO_ASSET_CHANGED", "Capability 已变化或撤回。");
        return new ResolvedAsset(cap, agent);
    }

    private ResolvedAsset resolveAsset(ActorContext actor, UUID workspaceId, UUID capabilityId, String version) {
        if (capabilityId == null || blank(version)) throw EafException.invalid("必须指定精确发布 Capability 版本。");
        var cap = capabilities.requirePublished(actor, workspaceId, capabilityId, version);
        var agent = agents.requirePublished(actor.tenantId(), workspaceId, cap.agentId(), cap.agentVersion());
        if (!"SERVICE_REQUEST_PLAN_V1".equals(agent.responseProfile()) || !agent.ragEnabled()
                || !"HYBRID".equals(agent.retrievalMode()) || !"NONE".equals(agent.evidencePolicy())
                || agent.modelProfileId() == null || cap.promptId() == null
                || cap.toolDependencies() == null || !cap.toolDependencies().isEmpty()
                || !agents.tools(actor.tenantId(), workspaceId, agent.id(), agent.version()).isEmpty()
                || cap.skillId() == null || blank(cap.skillVersion()) || blank(cap.skillContentHash()))
            throw EafException.invalid("Capability 必须是无工具、HYBRID 检索且满足只读结构合同的已发布资产。");
        return new ResolvedAsset(cap, agent);
    }

    private boolean compatible(ResolvedAsset a, ResolvedAsset b) {
        return a.agent().responseProfile().equals(b.agent().responseProfile())
                && a.agent().modelProfileId().equals(b.agent().modelProfileId())
                && a.agent().ragEnabled() == b.agent().ragEnabled()
                && a.agent().retrievalMode().equals(b.agent().retrievalMode())
                && a.agent().evidencePolicy().equals(b.agent().evidencePolicy());
    }

    private JsonNode buildManifest(Dataset dataset, ScenarioRunRequest request, List<ScenarioCase> cases,
                                   ResolvedAsset baseline, ResolvedAsset comparison, JsonNode knowledge, Instant deadline) {
        var manifest = json.createObjectNode();
        manifest.put("sceneKey", SCENE); manifest.put("datasetKey", request.datasetKey());
        manifest.put("datasetVersion", request.datasetVersion()); manifest.put("datasetManifestHash", dataset.manifestHash());
        manifest.put("scoringVersion", dataset.scoringVersion()); manifest.put("split", request.split());
        manifest.put("mode", request.mode()); manifest.put("deadlineAt", deadline.toString()); manifest.put("order", "CASE_ID_BASELINE_THEN_COMPARISON");
        var inputs = manifest.putArray("cases");
        for (var c : cases) { var x = inputs.addObject(); x.put("caseId", c.caseId()); x.put("inputHash", c.inputHash()); x.put("answerHash", c.answerHash()); }
        manifest.set("baseline", assetNode(baseline));
        if (comparison != null) manifest.set("comparison", assetNode(comparison));
        manifest.set("knowledgeManifest", knowledge);
        manifest.put("knowledgeManifestHash", Hashing.sha256(write(knowledge)));
        manifest.put("taskSource", "EVALUATION"); manifest.put("executionPath", "P15_SERVICE_REQUEST_PLAN_V1");
        if (comparison != null && isP24Pair(baseline, comparison)) {
            var experiment = manifest.putObject("optimizationExperiment");
            experiment.put("name", "P15_KNOWLEDGE_PRESENTATION_V1");
            experiment.put("schemaVersion", 1);
            experiment.put("baselineStrategy", "FULL_CONTEXT_V1");
            experiment.put("comparisonStrategy", "KNOWLEDGE_COMPACT_V1");
            experiment.put("serializationVersion", "JACKSON_V1");
            experiment.put("providerConfiguration", "NOT_SNAPSHOTTED");
            experiment.putArray("allowedDifferences").add("agentVersion").add("capabilityVersion").add("presentationStrategy");
            var fixed = experiment.putObject("fixedConfiguration");
            fixed.put("promptId", baseline.capability().promptId().toString());
            fixed.put("promptVersion", baseline.capability().promptVersion());
            fixed.put("skillId", baseline.capability().skillId().toString());
            fixed.put("skillVersion", baseline.capability().skillVersion());
            fixed.put("skillHash", baseline.capability().skillContentHash());
            fixed.put("modelProfileId", baseline.agent().modelProfileId().toString());
            fixed.put("retrievalMode", baseline.agent().retrievalMode());
            fixed.put("evidencePolicy", baseline.agent().evidencePolicy());
            fixed.put("ragEnabled", baseline.agent().ragEnabled());
            fixed.put("datasetManifestHash", dataset.manifestHash());
            fixed.put("knowledgeManifestHash", Hashing.sha256(write(knowledge)));
            var inputHashes = fixed.putArray("inputHashes");
            for (var c : cases) { var input = inputHashes.addObject(); input.put("caseId", c.caseId()); input.put("inputHash", c.inputHash()); }
            experiment.put("fixedConfigurationHash", fixedConfigurationHash(fixed));
        }
        return manifest;
    }

    private boolean isP24Pair(ResolvedAsset baseline, ResolvedAsset comparison) {
        return P15_CAPABILITY_ID.equals(baseline.capability().id()) && "1.0.0".equals(baseline.capability().version())
                && P15_AGENT_ID.equals(baseline.agent().id()) && "1.0.0".equals(baseline.agent().version())
                && P15_CAPABILITY_ID.equals(comparison.capability().id()) && "1.1.0".equals(comparison.capability().version())
                && P15_AGENT_ID.equals(comparison.agent().id()) && "1.1.0".equals(comparison.agent().version());
    }

    private boolean sameP24Configuration(ResolvedAsset baseline, ResolvedAsset comparison) {
        var a = baseline.capability(); var b = comparison.capability();
        var x = baseline.agent(); var y = comparison.agent();
        return Objects.equals(a.promptId(), b.promptId()) && Objects.equals(a.promptVersion(), b.promptVersion())
                && Objects.equals(a.skillId(), b.skillId()) && Objects.equals(a.skillVersion(), b.skillVersion())
                && Objects.equals(a.skillContentHash(), b.skillContentHash())
                && Objects.equals(x.modelProfileId(), y.modelProfileId())
                && Objects.equals(x.responseProfile(), y.responseProfile())
                && Objects.equals(x.retrievalMode(), y.retrievalMode())
                && Objects.equals(x.evidencePolicy(), y.evidencePolicy()) && x.ragEnabled() == y.ragEnabled();
    }

    private String fixedConfigurationHash(JsonNode fixed) {
        var ordered = new TreeMap<String, JsonNode>();
        fixed.fields().forEachRemaining(entry -> ordered.put(entry.getKey(), entry.getValue()));
        return Hashing.sha256(write(ordered));
    }

    private JsonNode assetNode(ResolvedAsset asset) {
        var node = json.createObjectNode(); var c = asset.capability(); var a = asset.agent();
        node.put("capabilityId", c.id().toString()); node.put("capabilityVersion", c.version()); node.put("capabilityHash", c.contentHash());
        node.put("agentId", a.id().toString()); node.put("agentVersion", a.version());
        node.put("promptId", c.promptId().toString()); node.put("promptVersion", c.promptVersion());
        node.put("skillId", c.skillId().toString()); node.put("skillVersion", c.skillVersion()); node.put("skillHash", c.skillContentHash());
        node.put("modelProfileId", a.modelProfileId().toString()); node.put("responseProfile", a.responseProfile());
        node.put("retrievalMode", a.retrievalMode()); node.put("evidencePolicy", a.evidencePolicy()); node.put("ragEnabled", a.ragEnabled());
        return node;
    }

    private ScenarioRunReport loadReport(ActorContext actor, UUID workspaceId, UUID runId) {
        var row = jdbc.query("select r.id, r.tenant_id, r.owner_id, r.status, r.version, r.stop_reason, r.split, r.mode, r.manifest::text, r.manifest_hash, r.created_at, r.updated_at, r.deadline_at, "
                        + "r.baseline_capability_id, r.baseline_capability_version, r.comparison_capability_id, r.comparison_capability_version "
                        + "from evaluation.scenario_run r where r.id = ? and r.tenant_id = ? and r.workspace_id = ? and r.owner_id = ?",
                rs -> rs.next() ? new RunRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("owner_id", UUID.class), rs.getString("status"), rs.getLong("version"), rs.getString("stop_reason"),
                        rs.getString("split"), rs.getString("mode"), rs.getString("manifest"), rs.getString("manifest_hash"),
                        rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant(), rs.getTimestamp("deadline_at").toInstant(),
                        rs.getObject("baseline_capability_id", UUID.class), rs.getString("baseline_capability_version"),
                        rs.getObject("comparison_capability_id", UUID.class), rs.getString("comparison_capability_version")) : null,
                runId, actor.tenantId(), workspaceId, actor.actorId());
        if (row == null) throw EafException.notFound();
        boolean current = manifestCurrent(actor, workspaceId, row.manifest());
        var counts = jdbc.query("select count(*) as planned, count(*) filter (where status in ('SCORED','ERROR','CANCELLED','TIMED_OUT')) as completed, "
                        + "count(*) filter (where status in ('ERROR','CANCELLED','TIMED_OUT')) as failed, count(*) filter (where status in ('PENDING','NOT_RUN')) as not_run, "
                        + "count(*) filter (where status = 'SCORED') as successful "
                        + "from evaluation.scenario_sample where run_id = ?", rs -> rs.next()
                        ? new int[]{rs.getInt(1),rs.getInt(2),rs.getInt(3),rs.getInt(4),rs.getInt(5)} : new int[5], runId);
        var samples = jdbc.query("select s.scores::text, a.expected_category, a.expected_outcome, a.expected_evidence_refs::text, s.queue_ms, s.execution_ms "
                        + "from evaluation.scenario_sample s join evaluation.scenario_run r on r.id = s.run_id "
                        + "join evaluation.scenario_answer a on a.dataset_key = r.dataset_key and a.dataset_version = r.dataset_version and a.case_id = s.case_id "
                        + "where s.run_id = ?", (rs, i) -> new ScoreRow(rs.getString(1), rs.getString(2),
                        rs.getString(3), rs.getString(4), rs.getObject(5, Long.class), rs.getObject(6, Long.class)), runId);
        var execution = metric(counts[4], counts[0], counts[1], current ? null : "SOURCE_UNAVAILABLE");
        var structure = aggregate(samples, "structure", current);
        var category = aggregate(samples, "category", current);
        var outcome = aggregate(samples, "outcome", current);
        var citation = aggregate(samples, "citationBinding", current);
        var evidence = aggregate(samples, "evidenceHit", current);
        var safety = aggregate(samples, "readOnlySafety", current);
        var pair = pairCounts(runId);
        boolean sameAsset = row.comparisonId() != null && row.baselineId().equals(row.comparisonId())
                && row.baselineVersion().equals(row.comparisonVersion());
        String comparisonReason = !current ? "COMPARISON_INVALIDATED" : row.comparisonId() == null ? "SINGLE_RUN" : sameAsset ? "SAME_ASSET" : null;
        boolean comparable = row.comparisonId() != null && !sameAsset && current && terminalRun(row.status())
                && pair[4] == counts[0] / 2 && pair[3] == 0;
        if (comparisonReason == null && row.comparisonId() != null && !comparable) comparisonReason = "INCOMPLETE_PAIRS";
        return new ScenarioRunReport(row.id(), row.status(), row.version(), !terminalRun(row.status()), row.stopReason(),
                counts[0], counts[1], counts[2], counts[3], comparable, sameAsset, comparisonReason, execution, structure,
                category, outcome, citation, evidence, safety, pair[0], pair[1], pair[2], pair[3], row.manifestHash(),
                row.createdAt(), row.updatedAt(), row.deadlineAt(), usageSummary(row.tenantId(), workspaceId, runId),
                timingSummary(samples, row), optimizationSummary(actor, workspaceId, row, current));
    }

    private ScenarioOptimizationSummary optimizationSummary(ActorContext actor, UUID workspaceId, RunRow run, boolean current) {
        JsonNode manifest;
        try { manifest = json.readTree(run.manifest()); }
        catch (Exception invalid) { manifest = json.createObjectNode(); }
        var experiment = manifest.path("optimizationExperiment");
        if (!"P15_KNOWLEDGE_PRESENTATION_V1".equals(experiment.path("name").asText()))
            return new ScenarioOptimizationSummary(1, null, run.manifestHash(), Instant.now(clock), "NOT_RECORDED",
                    List.of("NO_P24_EXPERIMENT_MANIFEST"), false, List.of("NO_P24_EXPERIMENT_MANIFEST"),
                    null, null, List.of(), new ScenarioOptimizationPairedSummary(0, 0, Map.of(), Map.of()));

        var baseline = optimizationSide(actor, workspaceId, run, "BASELINE", run.baselineVersion(), current);
        var comparison = optimizationSide(actor, workspaceId, run, "COMPARISON", run.comparisonVersion(), current);
        var conditions = new ArrayList<String>();
        if (!current) conditions.add("SOURCE_UNAVAILABLE");
        if (!fixedConfigurationHashMatches(experiment)) conditions.add("FIXED_CONFIGURATION_UNVERIFIED");
        if ("NOT_SNAPSHOTTED".equals(experiment.path("providerConfiguration").asText()))
            conditions.add("PROVIDER_CONFIGURATION_UNVERIFIED");
        if (baseline.modelIdentities().isEmpty() || comparison.modelIdentities().isEmpty())
            conditions.add("MODEL_IDENTITY_NOT_RECORDED");
        else if (!baseline.modelIdentities().equals(comparison.modelIdentities()))
            conditions.add("MODEL_IDENTITY_CHANGED");
        if (baseline.missingTasks() > 0 || comparison.missingTasks() > 0) conditions.add("PRESENTATION_MISSING");
        var reasonCodes = new ArrayList<String>(conditions);
        if (!"COMPLETED".equals(run.status())) reasonCodes.add("RUN_INCOMPLETE");
        if (!conditions.isEmpty()) reasonCodes.add("CONFIG_UNVERIFIED");

        var inputHashes = new HashMap<String, String>();
        for (var input : manifest.path("cases")) inputHashes.put(input.path("caseId").asText(), input.path("inputHash").asText());
        var baselineByCase = new HashMap<String, OptimizationSample>();
        baseline.samples().forEach(sample -> baselineByCase.put(sample.caseId(), sample));
        var comparisonByCase = new HashMap<String, OptimizationSample>();
        comparison.samples().forEach(sample -> comparisonByCase.put(sample.caseId(), sample));
        var caseIds = new java.util.TreeSet<String>();
        caseIds.addAll(baselineByCase.keySet()); caseIds.addAll(comparisonByCase.keySet());
        var pairs = new ArrayList<ScenarioOptimizationPair>();
        var deltas = new LinkedHashMap<String, List<Double>>();
        var metricDeltas = new LinkedHashMap<String, List<BigDecimal>>();
        var metricUnits = new LinkedHashMap<String, String>();
        var completePairs = 0;
        for (var caseId : caseIds) {
            var left = baselineByCase.get(caseId); var right = comparisonByCase.get(caseId);
            boolean complete = left != null && right != null && "SCORED".equals(left.status()) && "SCORED".equals(right.status());
            if (complete) completePairs++;
            var pairDeltas = complete ? qualityDeltas(left.scores(), right.scores()) : Map.<String, Double>of();
            pairDeltas.forEach((key, delta) -> deltas.computeIfAbsent(key, ignored -> new ArrayList<>()).add(delta));
            var pairMetrics = complete ? metricDeltas(left, right, baseline.usageByTask(), comparison.usageByTask()) : Map.<String, MetricDeltaPoint>of();
            var pairMetricValues = new LinkedHashMap<String, BigDecimal>();
            var pairMetricUnits = new LinkedHashMap<String, String>();
            pairMetrics.forEach((key, value) -> {
                pairMetricValues.put(key, value.delta());
                pairMetricUnits.put(key, value.unit());
                metricDeltas.computeIfAbsent(key, ignored -> new ArrayList<>()).add(value.delta());
                metricUnits.putIfAbsent(key, value.unit());
            });
            pairs.add(new ScenarioOptimizationPair(caseId, inputHashes.get(caseId),
                    left == null ? null : left.id(), right == null ? null : right.id(),
                    left == null ? null : left.taskId(), right == null ? null : right.taskId(),
                    left == null ? null : left.taskAttempt(), right == null ? null : right.taskAttempt(),
                    left == null ? null : left.resultHash(), right == null ? null : right.resultHash(), complete,
                    sourceRelation(left, right, baseline.presentations(), comparison.presentations()), pairDeltas,
                    Map.copyOf(pairMetricValues), Map.copyOf(pairMetricUnits)));
        }
        var qualityDeltas = new LinkedHashMap<String, ScenarioMetricDelta>();
        for (var key : List.of("structure", "category", "outcome", "citationBinding", "evidenceHit", "readOnlySafety")) {
            var values = deltas.getOrDefault(key, List.of());
            qualityDeltas.put(key, new ScenarioMetricDelta(values.size(), values.isEmpty() ? null
                    : values.stream().mapToDouble(Double::doubleValue).average().orElseThrow()));
        }
        var quantitativeDeltas = new LinkedHashMap<String, ScenarioOptimizationMetricDelta>();
        var comparisonMetrics = new java.util.TreeSet<String>(List.of("inputTokens", "outputTokens", "queueMillis",
                "taskMillis", "modelCallMillis"));
        comparisonMetrics.addAll(metricDeltas.keySet());
        if (comparisonMetrics.stream().noneMatch(key -> key.startsWith("estimatedCost:"))) comparisonMetrics.add("estimatedCost:UNKNOWN");
        if (comparisonMetrics.stream().noneMatch(key -> key.startsWith("actualBilledCost:"))) comparisonMetrics.add("actualBilledCost:UNKNOWN");
        for (var key : comparisonMetrics) {
            var values = metricDeltas.getOrDefault(key, List.of()).stream().sorted().toList();
            var unavailable = Math.max(0, completePairs - values.size());
            var unit = metricUnits.get(key);
            if (unit == null && key.contains(":")) unit = "UNKNOWN".equals(key.substring(key.indexOf(':') + 1))
                    ? null : key.substring(key.indexOf(':') + 1);
            quantitativeDeltas.put(key, new ScenarioOptimizationMetricDelta(values.size(), unavailable,
                    values.isEmpty() ? null : values.stream().reduce(BigDecimal.ZERO, BigDecimal::add),
                    medianDecimal(values), unit, values.isEmpty() ? "UNKNOWN" : unavailable > 0 ? "PARTIAL" : "KNOWN"));
        }
        if (pairs.stream().anyMatch(pair -> !pair.complete())) reasonCodes.add("INCOMPLETE_PAIRS");
        String status = baseline.summary().preparedRequests() + comparison.summary().preparedRequests() == 0
                || baseline.missingTasks() > 0 || comparison.missingTasks() > 0 || !current || !"COMPLETED".equals(run.status())
                ? "PARTIAL" : "AVAILABLE";
        return new ScenarioOptimizationSummary(1, experiment.path("name").asText(), run.manifestHash(),
                Instant.now(clock), status, reasonCodes.stream().distinct().toList(), conditions.isEmpty(),
                conditions.stream().distinct().toList(), baseline.summary(), comparison.summary(), List.copyOf(pairs),
                new ScenarioOptimizationPairedSummary(completePairs, pairs.size() - completePairs, qualityDeltas,
                        quantitativeDeltas));
    }

    private boolean fixedConfigurationHashMatches(JsonNode experiment) {
        var fixed = experiment.path("fixedConfiguration");
        return fixed.isObject() && experiment.path("fixedConfigurationHash").asText()
                .equals(fixedConfigurationHash(fixed));
    }

    private OptimizationSideData optimizationSide(ActorContext actor, UUID workspaceId, RunRow run, String side,
                                                    String capabilityVersion, boolean current) {
        var samples = jdbc.query("select s.id, s.case_id, s.status, s.task_id, s.task_attempt, s.result_hash, s.scores::text, "
                        + "a.expected_category, a.expected_outcome, a.expected_evidence_refs::text, s.queue_ms, s.execution_ms "
                        + "from evaluation.scenario_sample s join evaluation.scenario_run r on r.id = s.run_id "
                        + "join evaluation.scenario_answer a on a.dataset_key = r.dataset_key and a.dataset_version = r.dataset_version and a.case_id = s.case_id "
                        + "where s.run_id = ? and s.side = ? order by s.case_id",
                (rs, n) -> new OptimizationSample(rs.getObject("id", UUID.class), rs.getString("case_id"), side,
                        rs.getString("status"), rs.getObject("task_id", UUID.class), rs.getObject("task_attempt", Integer.class),
                        rs.getString("result_hash"), rs.getString("scores"), rs.getString("expected_category"),
                        rs.getString("expected_outcome"), rs.getString("expected_evidence_refs"),
                        rs.getObject("queue_ms", Long.class), rs.getObject("execution_ms", Long.class)), run.id(), side);
        var scoreRows = samples.stream().map(sample -> new ScoreRow(sample.scores(), sample.expectedCategory(),
                sample.expectedOutcome(), sample.expectedEvidenceRefs(), sample.queueMillis(), sample.executionMillis())).toList();
        var presentationByTask = new HashMap<UUID, Map<Integer, JsonNode>>();
        var usageByCall = new LinkedHashMap<String, UsageRecord>();
        var usageByTask = new HashMap<UUID, LinkedHashMap<String, UsageRecord>>();
        var identities = new HashSet<String>();
        var tasksWithPresentation = new HashSet<UUID>();
        var expectedStrategy = "BASELINE".equals(side) ? "FULL_CONTEXT_V1" : "KNOWLEDGE_COMPACT_V1";
        for (var sample : samples) {
            if (sample.taskId() == null) continue;
            var taskPresentations = presentationByTask.computeIfAbsent(sample.taskId(), ignored -> new LinkedHashMap<>());
            for (var step : runtime.steps(actor, workspaceId, sample.taskId())) {
                if (!"SERVICE_REQUEST_PRESENTATION".equals(step.type()) || step.content() == null) continue;
                try {
                    var meta = json.readTree(step.content());
                    if (meta.path("schemaVersion").asInt() != 1 || meta.path("taskAttempt").asInt(-1) != sample.taskAttempt()
                            || !"PREPARED".equals(meta.path("status").asText())
                            || !expectedStrategy.equals(meta.path("strategy").asText())) continue;
                    int round = meta.path("round").asInt(-1);
                    if (round > 0) taskPresentations.putIfAbsent(round, meta);
                } catch (Exception ignored) { }
            }
            if (!taskPresentations.isEmpty()) tasksWithPresentation.add(sample.taskId());
            for (var call : usage.findForTask(actor.tenantId(), workspaceId, sample.taskId())) {
                if (!"EVALUATION".equals(call.source()) || !"EVALUATION".equals(call.scopeType())
                        || !run.id().equals(call.scopeId()) || blank(call.callKey())) continue;
                usageByCall.putIfAbsent(call.callKey(), call);
                usageByTask.computeIfAbsent(sample.taskId(), ignored -> new LinkedHashMap<>()).putIfAbsent(call.callKey(), call);
                if (!blank(call.provider()) && !blank(call.model())) identities.add(call.provider() + "/" + call.model());
            }
        }
        var prepared = new LinkedHashMap<String, JsonNode>();
        var modelDurations = new ArrayList<Long>();
        for (var entries : presentationByTask.values()) for (var meta : entries.values()) {
            var callKey = meta.path("callKey").asText();
            if (!blank(callKey)) prepared.putIfAbsent(callKey, meta);
        }
        for (var call : usageByCall.values()) if (call.startedAt() != null && call.endedAt() != null
                && !call.endedAt().isBefore(call.startedAt())) modelDurations.add(Duration.between(call.startedAt(), call.endedAt()).toMillis());

        var reviewed = jdbc.queryForObject("select count(distinct s.id) filter (where r.sample_id is not null), "
                        + "count(distinct s.id) filter (where r.self_reviewed) from evaluation.scenario_sample s "
                        + "left join evaluation.scenario_review r on r.sample_id = s.id where s.run_id = ? and s.side = ?",
                (rs, n) -> rs.next() ? new int[]{rs.getInt(1), rs.getInt(2)} : new int[]{0, 0}, run.id(), side);
        int completed = (int) samples.stream().filter(sample -> Set.of("SCORED", "ERROR", "CANCELLED", "TIMED_OUT").contains(sample.status())).count();
        int failed = (int) samples.stream().filter(sample -> Set.of("ERROR", "CANCELLED", "TIMED_OUT").contains(sample.status())).count();
        int successful = (int) samples.stream().filter(sample -> "SCORED".equals(sample.status())).count();
        int notRun = (int) samples.stream().filter(sample -> Set.of("PENDING", "NOT_RUN").contains(sample.status())).count();
        int missingTasks = (int) samples.stream().filter(sample -> sample.taskId() != null).map(OptimizationSample::taskId).distinct().count()
                - tasksWithPresentation.size();
        long contextChars = sum(prepared, "contextChars"), contextBytes = sum(prepared, "contextUtf8Bytes");
        long messageChars = sum(prepared, "messageChars"), messageBytes = sum(prepared, "messageUtf8Bytes");
        long fullContextChars = sum(prepared, "fullContextChars"), fullContextBytes = sum(prepared, "fullContextUtf8Bytes");
        long fullMessageChars = sum(prepared, "fullMessageChars"), fullMessageBytes = sum(prepared, "fullMessageUtf8Bytes");
        var allUsage = List.copyOf(usageByCall.values());
        var queue = samples.stream().map(OptimizationSample::queueMillis).filter(Objects::nonNull).sorted().toList();
        var taskTime = samples.stream().map(OptimizationSample::executionMillis).filter(Objects::nonNull).sorted().toList();
        int chatCalls = (int) allUsage.stream().filter(call -> "CHAT".equals(call.callType())).count();
        int embeddingCalls = (int) allUsage.stream().filter(call -> "EMBEDDING".equals(call.callType())).count();
        var evidence = new java.util.TreeSet<String>();
        for (var call : allUsage) {
            addEvidence(evidence, "usageStatus", call.usageStatus());
            addEvidence(evidence, "costSource", call.costSource());
            addEvidence(evidence, "priceVersion", call.priceVersion());
            addEvidence(evidence, "priceSourceVersion", call.priceSourceVersion());
            addEvidence(evidence, "billingUnit", call.billingUnit());
            addEvidence(evidence, "billingSource", call.billingSource());
        }
        var chatCallKeys = allUsage.stream().filter(call -> "CHAT".equals(call.callType()))
                .map(UsageRecord::callKey).collect(java.util.stream.Collectors.toSet());
        int matchedGenerationCalls = (int) prepared.keySet().stream().filter(chatCallKeys::contains).count();
        var sideUsage = allUsage.isEmpty() && prepared.isEmpty()
                && samples.stream().noneMatch(sample -> sample.taskId() != null)
                ? knownNoUsageSummary() : usageSummary(allUsage);
        var sideSummary = new ScenarioOptimizationSide(capabilityVersion, samples.size(), completed, failed, notRun,
                metric(successful, samples.size(), completed, current ? null : "SOURCE_UNAVAILABLE"),
                aggregate(scoreRows, "structure", current), aggregate(scoreRows, "category", current),
                aggregate(scoreRows, "outcome", current), aggregate(scoreRows, "citationBinding", current),
                aggregate(scoreRows, "evidenceHit", current), aggregate(scoreRows, "readOnlySafety", current),
                reviewed[0], reviewed[1], Math.max(0, samples.size() - reviewed[0]), prepared.size(),
                tasksWithPresentation.size(), Math.max(0, missingTasks), matchedGenerationCalls,
                Math.max(0, prepared.size() - matchedGenerationCalls), Math.max(0, chatCalls - matchedGenerationCalls),
                contextChars, contextBytes, messageChars,
                messageBytes, fullContextChars, fullContextBytes, fullMessageChars, fullMessageBytes,
                fullContextChars - contextChars, fullContextBytes - contextBytes, fullMessageChars - messageChars,
                fullMessageBytes - messageBytes, chatCalls, embeddingCalls,
                identities.stream().sorted().limit(20).toList(), evidence.stream().limit(40).toList(), sideUsage,
                new ScenarioOptimizationTiming(median(queue), median(taskTime), median(modelDurations), modelDurations.size()));
        var callsByTask = new HashMap<UUID, List<UsageRecord>>();
        usageByTask.forEach((taskId, calls) -> callsByTask.put(taskId, List.copyOf(calls.values())));
        return new OptimizationSideData(sideSummary, samples, presentationByTask, callsByTask, Set.copyOf(identities), missingTasks);
    }

    private Map<String, MetricDeltaPoint> metricDeltas(OptimizationSample baseline, OptimizationSample comparison,
            Map<UUID, List<UsageRecord>> baselineUsage, Map<UUID, List<UsageRecord>> comparisonUsage) {
        var result = new LinkedHashMap<String, MetricDeltaPoint>();
        if (baseline.queueMillis() != null && comparison.queueMillis() != null)
            addDelta(result, "queueMillis", comparison.queueMillis() - baseline.queueMillis(), "ms");
        if (baseline.executionMillis() != null && comparison.executionMillis() != null)
            addDelta(result, "taskMillis", comparison.executionMillis() - baseline.executionMillis(), "ms");
        var leftCalls = baseline.taskId() == null ? List.<UsageRecord>of() : baselineUsage.getOrDefault(baseline.taskId(), List.of());
        var rightCalls = comparison.taskId() == null ? List.<UsageRecord>of() : comparisonUsage.getOrDefault(comparison.taskId(), List.of());
        var left = usageSummary(leftCalls);
        var right = usageSummary(rightCalls);
        if (left.inputTokens() != null && right.inputTokens() != null)
            addDelta(result, "inputTokens", right.inputTokens() - left.inputTokens(), "tokens");
        if (left.outputTokens() != null && right.outputTokens() != null)
            addDelta(result, "outputTokens", right.outputTokens() - left.outputTokens(), "tokens");
        // 费用只在两侧完整且币种一致时相减；混币种、缺账单和未知都保留为缺测。
        if ("KNOWN".equals(left.estimatedCostStatus()) && "KNOWN".equals(right.estimatedCostStatus())
                && Objects.equals(left.estimatedCurrency(), right.estimatedCurrency())
                && left.estimatedCost() != null && right.estimatedCost() != null)
            addDelta(result, "estimatedCost:" + left.estimatedCurrency(), right.estimatedCost().subtract(left.estimatedCost()), left.estimatedCurrency());
        if ("KNOWN".equals(left.actualCostStatus()) && "KNOWN".equals(right.actualCostStatus())
                && Objects.equals(left.actualCurrency(), right.actualCurrency())
                && left.actualBilledCost() != null && right.actualBilledCost() != null)
            addDelta(result, "actualBilledCost:" + left.actualCurrency(),
                    right.actualBilledCost().subtract(left.actualBilledCost()), left.actualCurrency());
        var leftDuration = totalCallMillis(leftCalls); var rightDuration = totalCallMillis(rightCalls);
        if (leftDuration != null && rightDuration != null)
            addDelta(result, "modelCallMillis", rightDuration - leftDuration, "ms");
        return Map.copyOf(result);
    }

    private Long totalCallMillis(List<UsageRecord> calls) {
        if (calls.isEmpty() || calls.stream().anyMatch(call -> call.startedAt() == null || call.endedAt() == null
                || call.endedAt().isBefore(call.startedAt()))) return null;
        return calls.stream().mapToLong(call -> Duration.between(call.startedAt(), call.endedAt()).toMillis()).sum();
    }

    private void addDelta(Map<String, MetricDeltaPoint> target, String key, long delta, String unit) {
        target.put(key, new MetricDeltaPoint(BigDecimal.valueOf(delta), unit));
    }

    private void addDelta(Map<String, MetricDeltaPoint> target, String key, BigDecimal delta, String unit) {
        target.put(key, new MetricDeltaPoint(delta, unit));
    }

    private void addEvidence(Set<String> evidence, String key, String value) {
        if (!blank(value)) evidence.add(key + "=" + value);
    }

    private BigDecimal medianDecimal(List<BigDecimal> sortedValues) {
        if (sortedValues.isEmpty()) return null;
        int middle = sortedValues.size() / 2;
        if (sortedValues.size() % 2 == 1) return sortedValues.get(middle);
        return sortedValues.get(middle - 1).add(sortedValues.get(middle)).divide(BigDecimal.valueOf(2));
    }

    private long sum(Map<String, JsonNode> values, String field) {
        return values.values().stream().mapToLong(value -> value.path(field).asLong(0)).sum();
    }

    private Map<String, Double> qualityDeltas(String baselineJson, String comparisonJson) {
        var result = new LinkedHashMap<String, Double>();
        try {
            var baseline = json.readTree(baselineJson); var comparison = json.readTree(comparisonJson);
            for (var key : List.of("structure", "category", "outcome", "citationBinding", "evidenceHit", "readOnlySafety")) {
                Double left = metricValue(baseline.path(key)); Double right = metricValue(comparison.path(key));
                if (left != null && right != null) result.put(key, right - left);
            }
        } catch (Exception ignored) { }
        return Map.copyOf(result);
    }

    private Double metricValue(JsonNode metric) {
        int denominator = metric.path("denominator").asInt();
        if (denominator <= 0 || metric.path("scored").asInt() != denominator) return null;
        return (double) metric.path("numerator").asInt() / denominator;
    }

    private String sourceRelation(OptimizationSample baseline, OptimizationSample comparison,
            Map<UUID, Map<Integer, JsonNode>> baselineSteps, Map<UUID, Map<Integer, JsonNode>> comparisonSteps) {
        if (baseline == null || comparison == null || baseline.taskId() == null || comparison.taskId() == null) return "NOT_RECORDED";
        var left = baselineSteps.getOrDefault(baseline.taskId(), Map.of());
        var right = comparisonSteps.getOrDefault(comparison.taskId(), Map.of());
        if (left.isEmpty() || right.isEmpty()) return "NOT_RECORDED";
        if (!left.keySet().equals(right.keySet())) return "TRAJECTORY_DIFF";
        boolean citationChanged = false;
        for (var round : left.keySet()) {
            var a = left.get(round); var b = right.get(round);
            if (!a.path("queryHash").asText().equals(b.path("queryHash").asText())
                    || !a.path("formalSourceSetHash").asText().equals(b.path("formalSourceSetHash").asText()))
                return "TRAJECTORY_DIFF";
            citationChanged |= !a.path("citationMapHash").asText().equals(b.path("citationMapHash").asText());
        }
        return citationChanged ? "CITATION_MAPPING_DIFF" : "SAME_SOURCE_TRAJECTORY";
    }

    private ScenarioTimingSummary timingSummary(List<ScoreRow> rows, RunRow run) {
        var queues = rows.stream().map(ScoreRow::queueMillis).filter(java.util.Objects::nonNull).sorted().toList();
        var executions = rows.stream().map(ScoreRow::executionMillis).filter(java.util.Objects::nonNull).sorted().toList();
        var asOf = terminalRun(run.status()) ? run.updatedAt() : Instant.now(clock);
        return new ScenarioTimingSummary(median(queues), median(executions),
                Math.max(0, Duration.between(run.createdAt(), asOf).toMillis()));
    }

    private Long median(List<Long> values) {
        if (values.isEmpty()) return null;
        int middle = values.size() / 2;
        return values.size() % 2 == 1 ? values.get(middle)
                : Math.round(values.get(middle - 1) / 2.0 + values.get(middle) / 2.0);
    }

    private ScenarioMetric aggregate(List<ScoreRow> rows, String key, boolean current) {
        if (!current) return unavailable("SOURCE_UNAVAILABLE");
        int numerator = 0, denominator = 0, scored = 0;
        for (var row : rows) {
            boolean applicable = switch (key) {
                case "structure", "readOnlySafety" -> true;
                case "category" -> row.expectedCategory() != null;
                case "outcome" -> row.expectedOutcome() != null;
                case "evidenceHit" -> hasExpectedEvidence(row.expectedEvidenceRefs());
                case "citationBinding" -> "READY".equals(row.expectedOutcome()) || hasScoredMetric(row.scores(), key);
                default -> false;
            };
            if (!applicable) continue;
            denominator++;
            if (row.scores() == null) continue;
            try {
                var metric = json.readTree(row.scores()).path(key);
                if (metric.isMissingNode()) continue;
                numerator += metric.path("numerator").asInt();
                scored += metric.path("scored").asInt();
            } catch (Exception ignored) { }
        }
        return denominator == 0 ? unavailable("NO_APPLICABLE_LABEL")
                : new ScenarioMetric(numerator == denominator && scored == denominator ? "PASS" : "FAIL", numerator,
                        denominator, scored, scored == 0 ? null : (double) numerator / denominator,
                        numerator == denominator && scored == denominator ? null : "PARTIAL_OR_RULE_FAILURE");
    }

    private boolean hasExpectedEvidence(String raw) {
        try { var node = json.readTree(raw); return node.isArray() && !node.isEmpty(); }
        catch (Exception ignored) { return false; }
    }

    private boolean hasScoredMetric(String scores, String key) {
        if (scores == null) return false;
        try { return json.readTree(scores).path(key).path("denominator").asInt() > 0; }
        catch (Exception ignored) { return false; }
    }

    private int[] pairCounts(UUID runId) {
        return jdbc.query("with paired as (select b.case_id, b.scores::text as left_scores, c.scores::text as right_scores, b.status as left_status, c.status as right_status "
                        + "from evaluation.scenario_sample b join evaluation.scenario_sample c on c.run_id = b.run_id and c.case_id = b.case_id and c.side = 'COMPARISON' "
                        + "where b.run_id = ? and b.side = 'BASELINE') select * from paired",
                rs -> { int improved=0, regressed=0, unchanged=0, unevaluable=0, complete=0;
                    while (rs.next()) {
                        if (!"SCORED".equals(rs.getString("left_status")) || !"SCORED".equals(rs.getString("right_status"))) { unevaluable++; continue; }
                        complete++;
                        try {
                            var l = pairScore(json.readTree(rs.getString("left_scores")));
                            var r = pairScore(json.readTree(rs.getString("right_scores")));
                            if (r > l) improved++; else if (r < l) regressed++; else unchanged++;
                        } catch (Exception invalid) { unevaluable++; complete--; }
                    }
                    return new int[]{improved,regressed,unchanged,unevaluable,complete}; }, runId);
    }

    private int pairScore(JsonNode scores) {
        int score = 0;
        for (var key : List.of("structure", "category", "outcome", "citationBinding", "evidenceHit", "readOnlySafety")) {
            var metric = scores.path(key);
            if ("PASS".equals(metric.path("status").asText())) score++;
        }
        return score;
    }

    private ScenarioSampleView sampleView(java.sql.ResultSet rs, boolean current) throws java.sql.SQLException {
        String scores = rs.getString("scores"); JsonNode n = null;
        try { if (scores != null && current) n = json.readTree(scores); } catch (Exception ignored) { }
        String semantic = jdbc.query("select verdict from evaluation.scenario_review where sample_id = ? order by revision desc limit 1",
                r -> r.next() ? r.getString(1) : "UNREVIEWED", rs.getObject("id", UUID.class));
        return new ScenarioSampleView(rs.getObject("id", UUID.class), rs.getString("case_id"), rs.getString("side"),
                rs.getString("status"), rs.getObject("task_id", UUID.class), rs.getObject("task_attempt", Integer.class),
                current ? rs.getString("result_hash") : null, current ? rs.getString("error_code") : "SOURCE_UNAVAILABLE",
                metricFrom(n, "structure"), metricFrom(n, "category"), metricFrom(n, "outcome"), metricFrom(n, "citationBinding"),
                metricFrom(n, "evidenceHit"), metricFrom(n, "readOnlySafety"), current ? semantic : "UNREVIEWED", !current,
                current ? rs.getObject("queue_ms", Long.class) : null, current ? rs.getObject("execution_ms", Long.class) : null);
    }

    private ScenarioUsageSummary usageSummary(UUID tenantId, UUID workspaceId, UUID runId) {
        return usageSummary(usage.findForScope(tenantId, workspaceId, "EVALUATION", runId));
    }

    private ScenarioUsageSummary usageSummary(List<UsageRecord> rows) {
        int input = 0, output = 0, knownTokens = 0, unknownTokens = 0, knownCosts = 0, knownActual = 0;
        BigDecimal estimated = BigDecimal.ZERO, actual = BigDecimal.ZERO;
        String estimatedCurrency = null, actualCurrency = null;
        boolean mixedEstimated = false, mixedActual = false;
        for (UsageRecord row : rows) {
            if (row.inputTokens() != null && row.outputTokens() != null) { input += row.inputTokens(); output += row.outputTokens(); knownTokens++; }
            else unknownTokens++;
            if (row.estimatedCost() != null && row.costCurrency() != null) {
                if (estimatedCurrency != null && !estimatedCurrency.equals(row.costCurrency())) mixedEstimated = true;
                estimatedCurrency = estimatedCurrency == null ? row.costCurrency() : estimatedCurrency;
                estimated = estimated.add(row.estimatedCost()); knownCosts++;
            }
            if (row.actualCost() != null && row.actualCostCurrency() != null) {
                if (actualCurrency != null && !actualCurrency.equals(row.actualCostCurrency())) mixedActual = true;
                actualCurrency = actualCurrency == null ? row.actualCostCurrency() : actualCurrency;
                actual = actual.add(row.actualCost()); knownActual++;
            }
        }
        String estimatedStatus = mixedEstimated ? "MIXED_CURRENCY" : knownCosts == rows.size() && !rows.isEmpty() ? "KNOWN" : "UNKNOWN";
        String actualStatus = mixedActual ? "MIXED_CURRENCY" : knownActual == rows.size() && !rows.isEmpty() ? "KNOWN" : "UNKNOWN";
        return new ScenarioUsageSummary(rows.size(), knownTokens == rows.size() ? input : null,
                knownTokens == rows.size() ? output : null, unknownTokens,
                "KNOWN".equals(estimatedStatus) ? estimated : null,
                mixedEstimated ? null : estimatedCurrency, estimatedStatus,
                "KNOWN".equals(actualStatus) ? actual : null,
                mixedActual ? null : actualCurrency, actualStatus, Instant.now(clock));
    }

    private ScenarioUsageSummary knownNoUsageSummary() {
        return new ScenarioUsageSummary(0, 0, 0, 0, BigDecimal.ZERO, null, "KNOWN",
                BigDecimal.ZERO, null, "KNOWN", Instant.now(clock));
    }

    private ScenarioReview loadReview(UUID id) {
        return jdbc.query("select id, sample_id, revision, supersedes_review_id, task_attempt, result_hash, rubric_version, verdict, issue_type, comment, self_reviewed, created_at from evaluation.scenario_review where id = ?",
                rs -> rs.next() ? new ScenarioReview(rs.getObject("id", UUID.class), rs.getObject("sample_id", UUID.class),
                        rs.getInt("revision"), rs.getObject("supersedes_review_id", UUID.class), rs.getInt("task_attempt"),
                        rs.getString("result_hash"), rs.getString("rubric_version"), rs.getString("verdict"), rs.getString("issue_type"),
                        rs.getString("comment"), rs.getBoolean("self_reviewed"), rs.getTimestamp("created_at").toInstant()) : null, id);
    }

    private RunRow findByKey(ActorContext actor, UUID workspaceId, String key) {
        return jdbc.query("select id, tenant_id, owner_id, status, version, stop_reason, split, mode, manifest::text, manifest_hash, created_at, updated_at, deadline_at, baseline_capability_id, baseline_capability_version, comparison_capability_id, comparison_capability_version from evaluation.scenario_run where tenant_id = ? and workspace_id = ? and owner_id = ? and idempotency_key = ?",
                rs -> rs.next() ? mapRun(rs) : null, actor.tenantId(), workspaceId, actor.actorId(), key);
    }

    private ScenarioRunResult replay(ActorContext actor, UUID workspaceId, RunRow row, String requestHash) {
        if (!requestHash.equals(jdbc.queryForObject("select request_hash from evaluation.scenario_run where id = ?", String.class, row.id())))
            throw EafException.conflict("SCENARIO_IDEMPOTENCY_CONFLICT", "幂等键已绑定其他运行请求。");
        return new ScenarioRunResult(loadReport(actor, workspaceId, row.id()), false);
    }

    private RunRow mapRun(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new RunRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("owner_id", UUID.class),
                rs.getString("status"), rs.getLong("version"), rs.getString("stop_reason"), rs.getString("split"), rs.getString("mode"),
                rs.getString("manifest"), rs.getString("manifest_hash"), rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(), rs.getTimestamp("deadline_at").toInstant(),
                rs.getObject("baseline_capability_id", UUID.class), rs.getString("baseline_capability_version"),
                rs.getObject("comparison_capability_id", UUID.class), rs.getString("comparison_capability_version"));
    }

    private boolean sourceCurrent(ActorContext actor, UUID workspaceId, UUID runId) {
        var manifest = jdbc.query("select manifest::text from evaluation.scenario_run where id = ? and tenant_id = ? and workspace_id = ? and owner_id = ?",
                rs -> rs.next() ? rs.getString(1) : null, runId, actor.tenantId(), workspaceId, actor.actorId());
        return manifest != null && manifestCurrent(actor, workspaceId, manifest);
    }

    private boolean manifestCurrent(ActorContext actor, UUID workspaceId, String manifestJson) {
        try {
            var root = json.readTree(manifestJson);
            var frozen = root.get("knowledgeManifest");
            if (frozen == null || !frozen.equals(contextManifest.capture(actor, workspaceId))) return false;
            for (var side : List.of("baseline", "comparison")) {
                var expected = root.get(side);
                if (expected == null) continue;
                var asset = resolveAsset(actor, workspaceId, UUID.fromString(expected.path("capabilityId").asText()),
                        expected.path("capabilityVersion").asText());
                if (!assetNode(asset).equals(expected)) return false;
            }
            return true;
        } catch (RuntimeException | java.io.IOException stale) { return false; }
    }

    private boolean answerHashBound(String manifest, String caseId, String answerHash) {
        try { for (var c : json.readTree(manifest).path("cases")) if (caseId.equals(c.path("caseId").asText())) return answerHash.equals(c.path("answerHash").asText()); }
        catch (Exception ignored) { }
        return false;
    }

    private List<String> parseEvidenceRefs(String text) {
        try { var refs = json.readTree(text); var out = new ArrayList<String>();
            for (var r : refs) {
                if (r.isTextual()) out.add(r.asText());
                else if (r.isObject()) out.add(r.path("documentId").asText() + ":" + r.path("documentVersion").asInt()
                        + ":" + r.path("buildId").asText() + ":" + r.path("chunkId").asText());
            }
            return out;
        } catch (Exception ignored) { return List.of(); }
    }

    private ScenarioMetric metricFrom(JsonNode node, String key) {
        if (node == null) return unavailable("SOURCE_UNAVAILABLE");
        try { var m = node.path(key); return m.isMissingNode() ? unavailable("NOT_APPLICABLE") : json.treeToValue(m, ScenarioMetric.class); }
        catch (Exception ignored) { return unavailable("INVALID_STORED_SCORE"); }
    }

    private ScenarioMetric metric(long numerator, int denominator, int scored, String reason) {
        if (denominator <= 0) return unavailable(reason == null ? "NO_SAMPLES" : reason);
        return new ScenarioMetric(numerator == denominator && scored == denominator ? "PASS" : "FAIL",
                (int) numerator, denominator, scored, scored == 0 ? null : (double) numerator / denominator,
                numerator == denominator && scored == denominator ? null : "TASK_COVERAGE_OR_FAILURE");
    }

    private ScenarioMetric unavailable(String reason) { return new ScenarioMetric("NOT_SCORED", 0, 0, 0, null, reason); }

    private void requireOwner(ActorContext actor, UUID workspaceId, UUID runId, String action) {
        requireDirect(actor, workspaceId, action);
        Integer n = jdbc.queryForObject("select count(*) from evaluation.scenario_run where id = ? and tenant_id = ? and workspace_id = ? and owner_id = ?",
                Integer.class, runId, actor.tenantId(), workspaceId, actor.actorId());
        if (n == null || n != 1) throw EafException.notFound();
    }

    private void requireDirect(ActorContext actor, UUID workspaceId, String action) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated()) throw EafException.forbidden("仅支持直接 HUMAN 操作。");
        workspaces.require(actor, workspaceId, action);
    }

    private ActorContext internalActor(UUID tenantId, UUID ownerId, UUID workspaceId) {
        return new ActorContext(ownerId, tenantId, ActorType.HUMAN, workspaces.actions(tenantId, ownerId, workspaceId));
    }

    private boolean terminal(TaskStatus s) { return Set.of(TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.TIMED_OUT, TaskStatus.CANCELLED).contains(s); }
    private boolean terminalRun(String s) { return Set.of("COMPLETED", "COMPLETED_WITH_ERRORS", "STOPPED", "FAILED", "TIMED_OUT").contains(s); }
    private boolean fatal(String code) { return Set.of("SCENARIO_RUN_NOT_CURRENT", "WORKSPACE_ACCESS_REVOKED", "SCENARIO_ASSET_CHANGED", "SCENARIO_INDEX_NOT_READY").contains(code); }
    private String safeError(RuntimeException e) { return e instanceof EafException x ? x.code() : "ORCHESTRATION_FAILED"; }
    private boolean blank(String s) { return s == null || s.isBlank(); }

    private String requestHash(ScenarioRunRequest r) {
        var baseline = r.baseline(); var comparison = r.comparison();
        return Hashing.sha256(String.join("\u001f", String.valueOf(r.datasetKey()), String.valueOf(r.datasetVersion()),
                String.valueOf(r.split()), String.valueOf(r.mode()),
                String.valueOf(baseline == null ? null : baseline.capabilityId()),
                String.valueOf(baseline == null ? null : baseline.version()),
                String.valueOf(comparison == null ? null : comparison.capabilityId()),
                String.valueOf(comparison == null ? null : comparison.version()), String.valueOf(r.deadlineAt())));
    }

    private String write(Object value) {
        try { return json.writeValueAsString(value); } catch (Exception e) { throw new IllegalStateException("JSON serialization failed", e); }
    }

    private record Dataset(String sceneKey, String scoringVersion, String manifestHash) { }
    private record ScenarioCase(String caseId, String split, String requestText, String inputHash, String expectedCategory,
                                 String expectedOutcome, String expectedEvidenceRefs, String answerHash) { }
    private record Registration(UUID id, String purpose, String source, String requestHash) { }
    private record ResolvedAsset(CapabilityDefinition capability, AgentDefinition agent) {
        TaskAssetBinding binding() { return new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash()); }
    }
    private record DispatchRun(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String status, boolean stopRequested,
                               String stopReason, Instant deadlineAt, String manifest, long fence) { }
    private record ClaimedRun(DispatchRun run, UUID owner, long fence) { }
    private record PendingSample(UUID id, String caseId, String side, String input, String taskKey) { }
    private record ActiveSample(UUID id, UUID taskId, String caseId, String side, int taskAttempt) { }
    private record Expected(String category, String outcome, String evidenceRefsJson, String answerHash) { }
    private record EvidenceRow(UUID ownerId, String manifest, UUID id, String caseId, UUID taskId, String side) { }
    private record ReviewSample(UUID taskId, int taskAttempt, String resultHash, String status, String rubricVersion) { }
    private record ScoreRow(String scores, String expectedCategory, String expectedOutcome, String expectedEvidenceRefs,
                            Long queueMillis, Long executionMillis) { }
    private record OptimizationSample(UUID id, String caseId, String side, String status, UUID taskId,
                                      Integer taskAttempt, String resultHash, String scores, String expectedCategory,
                                      String expectedOutcome, String expectedEvidenceRefs, Long queueMillis,
                                      Long executionMillis) { }
    private record OptimizationSideData(ScenarioOptimizationSide summary, List<OptimizationSample> samples,
                                        Map<UUID, Map<Integer, JsonNode>> presentations,
                                        Map<UUID, List<UsageRecord>> usageByTask, Set<String> modelIdentities,
                                        int missingTasks) { }
    private record MetricDeltaPoint(BigDecimal delta, String unit) { }
    private record RunRow(UUID id, UUID tenantId, UUID ownerId, String status, long version, String stopReason,
                          String split, String mode, String manifest, String manifestHash, Instant createdAt,
                          Instant updatedAt, Instant deadlineAt, UUID baselineId, String baselineVersion,
                          UUID comparisonId, String comparisonVersion) { }
}
