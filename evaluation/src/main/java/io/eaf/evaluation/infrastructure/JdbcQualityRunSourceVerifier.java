package io.eaf.evaluation.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Hashing;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.QualityRunSourceVerifier;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.time.Instant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** 仅 Evaluation 能读取其质量运行登记，并向 Task 暴露来源一致性结果。 */
@Repository
public class JdbcQualityRunSourceVerifier implements QualityRunSourceVerifier {
    private final JdbcTemplate jdbc;
    private final ScenarioContextManifest contextManifest;
    private final ObjectMapper json;
    private final WorkspaceAuthorization workspaces;

    public JdbcQualityRunSourceVerifier(JdbcTemplate jdbc, ScenarioContextManifest contextManifest, ObjectMapper json,
                                        WorkspaceAuthorization workspaces) {
        this.jdbc = jdbc;
        this.contextManifest = contextManifest;
        this.json = json;
        this.workspaces = workspaces;
    }

    @Override
    public boolean sourceMatches(UUID tenantId, UUID workspaceId, UUID qualityRunId, String source) {
        if (tenantId == null || workspaceId == null || qualityRunId == null || source == null) return false;
        // 候选成对快照、完整保留集运行和通用验收登记均归 Evaluation；单对照的 pair ID 固定为 EVALUATION。
        List<String> sources = jdbc.query("select source from evaluation.quality_run_registration where id = ? and tenant_id = ? and workspace_id = ? "
                        + "union all select source from evaluation.candidate_eval_run where id = ? and tenant_id = ? and workspace_id = ? "
                        + "union all select 'EVALUATION' as source from evaluation.candidate_pair_run where id = ? and tenant_id = ? and workspace_id = ?",
                (rs, rowNum) -> rs.getString("source"), qualityRunId, tenantId, workspaceId,
                qualityRunId, tenantId, workspaceId, qualityRunId, tenantId, workspaceId);
        return sources.size() == 1 && source.equals(sources.getFirst());
    }

    @Override
    public boolean workflowMatches(UUID tenantId, UUID workspaceId, UUID qualityRunId,
                                   UUID workflowId, String workflowVersion) {
        if (tenantId == null || workspaceId == null || qualityRunId == null
                || workflowId == null || workflowVersion == null) return false;
        // 协作登记必须命中 ACTIVE 清单中的任一精确版本；其他评测用途保持既有 Workflow 行为。
        var matches = jdbc.query("select case when r.purpose = 'SERVICE_REQUEST_SCENARIO' then false "
                        + "when r.purpose <> 'COLLABORATION_HELD_OUT' then true "
                        + "else r.source = 'EVALUATION' and m.status = 'ACTIVE' and "
                        + "((m.baseline_workflow_id = ? and m.baseline_workflow_version = ?) "
                        + "or (m.reviewer_workflow_id = ? and m.reviewer_workflow_version = ?)) end as allowed "
                        + "from evaluation.quality_run_registration r left join evaluation.collaboration_analysis_manifest m "
                        + "on m.id = r.collaboration_manifest_id and m.tenant_id = r.tenant_id and m.workspace_id = r.workspace_id "
                        + "where r.id = ? and r.tenant_id = ? and r.workspace_id = ?",
                (rs, row) -> rs.getBoolean("allowed"), workflowId, workflowVersion, workflowId, workflowVersion,
                qualityRunId, tenantId, workspaceId);
        // 候选评测 ID 存在于 Evaluation 的其他自有表；sourceMatches 已验证其归属，且不使用协作 Workflow manifest。
        return matches.isEmpty() || matches.size() == 1 && matches.getFirst();
    }

    @Override
    public boolean workflowStepMatches(UUID tenantId, UUID workspaceId, UUID qualityRunId,
                                       UUID workflowId, String workflowVersion, String stepId) {
        if (tenantId == null || workspaceId == null || qualityRunId == null || workflowId == null
                || workflowVersion == null || stepId == null) return false;
        var matches = jdbc.query("select case when r.purpose = 'SERVICE_REQUEST_SCENARIO' then false "
                        + "when r.purpose <> 'COLLABORATION_HELD_OUT' then true "
                        + "else r.source = 'EVALUATION' and m.status = 'ACTIVE' and ("
                        + "(m.baseline_workflow_id = ? and m.baseline_workflow_version = ? and ? = any(m.baseline_step_ids)) "
                        + "or (m.reviewer_workflow_id = ? and m.reviewer_workflow_version = ? and ? = any(m.reviewer_step_ids))) end as allowed "
                        + "from evaluation.quality_run_registration r left join evaluation.collaboration_analysis_manifest m "
                        + "on m.id = r.collaboration_manifest_id and m.tenant_id = r.tenant_id and m.workspace_id = r.workspace_id "
                        + "where r.id = ? and r.tenant_id = ? and r.workspace_id = ? "
                        + "union all select true from evaluation.candidate_eval_run where id = ? and tenant_id = ? and workspace_id = ? "
                        + "union all select true from evaluation.candidate_pair_run where id = ? and tenant_id = ? and workspace_id = ?",
                (rs, row) -> rs.getBoolean("allowed"), workflowId, workflowVersion, stepId,
                workflowId, workflowVersion, stepId,
                qualityRunId, tenantId, workspaceId, qualityRunId, tenantId, workspaceId,
                qualityRunId, tenantId, workspaceId);
        // 普通候选评测延续原 Workflow 行为；协作清单同时约束 baseline 和 reviewer 的精确步骤。
        return matches.size() == 1 && matches.getFirst();
    }

    @Override
    public boolean collaborationReviewerStepMatches(UUID tenantId, UUID workspaceId, UUID qualityRunId,
                                                     UUID workflowId, String workflowVersion, String stepId) {
        if (tenantId == null || workspaceId == null || qualityRunId == null || workflowId == null
                || workflowVersion == null || stepId == null) return false;
        var matches = jdbc.query("select r.source = 'EVALUATION' and r.purpose = 'COLLABORATION_HELD_OUT' "
                        + "and m.status = 'ACTIVE' and m.reviewer_workflow_id = ? and m.reviewer_workflow_version = ? "
                        + "and ? = any(m.reviewer_step_ids) as allowed "
                        + "from evaluation.quality_run_registration r join evaluation.collaboration_analysis_manifest m "
                        + "on m.id = r.collaboration_manifest_id and m.tenant_id = r.tenant_id and m.workspace_id = r.workspace_id "
                        + "where r.id = ? and r.tenant_id = ? and r.workspace_id = ?",
                (rs, row) -> rs.getBoolean("allowed"), workflowId, workflowVersion, stepId,
                qualityRunId, tenantId, workspaceId);
        return matches.size() == 1 && matches.getFirst();
    }

    @Override
    public boolean scenarioSampleMatches(UUID tenantId, UUID workspaceId, UUID actorId, UUID qualityRunId,
                                         UUID sampleId, String taskKey, UUID agentId, String agentVersion,
                                         String inputHash, TaskAssetBinding binding) {
        if (tenantId == null || workspaceId == null || actorId == null || qualityRunId == null
                || sampleId == null || taskKey == null || agentId == null || agentVersion == null
                || inputHash == null || binding == null) return false;
        var rows = jdbc.query("select r.manifest::text from evaluation.scenario_run r "
                        + "join evaluation.scenario_sample s on s.run_id = r.id "
                        + "join evaluation.scenario_case c on c.dataset_key = r.dataset_key and c.dataset_version = r.dataset_version and c.case_id = s.case_id "
                        + "where r.id = ? and r.tenant_id = ? and r.workspace_id = ? and r.owner_id = ? "
                        + "and r.status = 'RUNNING' and not r.stop_requested and r.deadline_at > now() "
                        + "and s.id = ? and s.task_key = ? and s.status = 'PENDING' and s.task_id is null "
                        + "and c.input_hash = ? and c.input_hash = s.input_hash and c.split = r.split "
                        + "and s.agent_id = ? and s.agent_version = ? and s.capability_id = ? and s.capability_version = ? "
                        + "and s.capability_hash = ? and s.skill_id = ? and s.skill_version = ? and s.skill_hash = ?",
                (rs, row) -> rs.getString(1), qualityRunId, tenantId, workspaceId, actorId, sampleId, taskKey, inputHash,
                agentId, agentVersion, binding.capabilityId(), binding.capabilityVersion(), binding.capabilityHash(),
                binding.skillId(), binding.skillVersion(), binding.skillHash());
        return rows.size() == 1 && scenarioContextCurrent(tenantId, workspaceId, actorId, rows.getFirst());
    }

    @Override
    public boolean scenarioTaskMatches(UUID tenantId, UUID workspaceId, UUID actorId, UUID taskId, UUID rootTaskId,
                                       UUID qualityRunId, int attempt, UUID agentId, String agentVersion,
                                       String source, String runKind, String inputHash, TaskAssetBinding binding) {
        if (tenantId == null || workspaceId == null || actorId == null || taskId == null || rootTaskId == null
                || qualityRunId == null || agentId == null || agentVersion == null || binding == null) return false;
        var rows = jdbc.query("select r.id, r.owner_id, r.manifest::text, r.status, r.stop_requested, r.deadline_at, "
                        + "s.status as sample_status, s.task_attempt, s.input_hash, s.agent_id, s.agent_version, "
                        + "s.capability_id, s.capability_version, s.capability_hash, s.skill_id, s.skill_version, s.skill_hash "
                        + "from evaluation.scenario_sample s join evaluation.scenario_run r on r.id = s.run_id "
                        + "where s.task_id = ? and r.id = ? and r.tenant_id = ? and r.workspace_id = ? and r.deadline_at > now()",
                (rs, row) -> new ScenarioBinding(rs.getObject("id", UUID.class), rs.getObject("owner_id", UUID.class),
                        rs.getString("manifest"), rs.getString("status"), rs.getBoolean("stop_requested"),
                        rs.getTimestamp("deadline_at").toInstant(), rs.getString("sample_status"),
                        rs.getInt("task_attempt"), rs.getString("input_hash"), rs.getObject("agent_id", UUID.class),
                        rs.getString("agent_version"), rs.getObject("capability_id", UUID.class),
                        rs.getString("capability_version"), rs.getString("capability_hash"),
                        rs.getObject("skill_id", UUID.class), rs.getString("skill_version"), rs.getString("skill_hash")),
                taskId, qualityRunId, tenantId, workspaceId);
        if (rows.isEmpty()) return teamPreparationTaskMatches(tenantId, workspaceId, actorId, taskId, rootTaskId,
                qualityRunId, attempt, agentId, agentVersion, source, runKind, inputHash, binding);
        if (rows.size() != 1) return false;
        var row = rows.getFirst();
        if (!List.of("RUNNING", "STOPPING").contains(row.runStatus()) || row.stopRequested()
                || !"ACTIVE".equals(row.sampleStatus())
                || !row.id().equals(qualityRunId) || !taskId.equals(rootTaskId) || !"AGENT".equals(runKind)
                || !"EVALUATION".equals(source) || !row.ownerId().equals(actorId) || attempt != row.taskAttempt()
                || !agentId.equals(row.agentId()) || !agentVersion.equals(row.agentVersion())
                || !inputHash.equals(row.inputHash()) || !binding.capabilityId().equals(row.capabilityId())
                || !binding.capabilityVersion().equals(row.capabilityVersion())
                || !binding.capabilityHash().equals(row.capabilityHash()) || !binding.skillId().equals(row.skillId())
                || !binding.skillVersion().equals(row.skillVersion()) || !binding.skillHash().equals(row.skillHash())) return false;
        return scenarioContextCurrent(tenantId, workspaceId, actorId, row.manifest());
    }

    private boolean scenarioContextCurrent(UUID tenantId, UUID workspaceId, UUID actorId, String manifestJson) {
        try {
            JsonNode frozen = json.readTree(manifestJson).get("knowledgeManifest");
            var actor = new ActorContext(actorId, tenantId, ActorType.HUMAN,
                    workspaces.actions(tenantId, actorId, workspaceId));
            return frozen != null && frozen.equals(contextManifest.capture(actor, workspaceId));
        } catch (RuntimeException | java.io.IOException invalidOrStale) { return false; }
    }

    @Override
    public boolean isScenarioRun(UUID tenantId, UUID workspaceId, UUID qualityRunId) {
        if (tenantId == null || workspaceId == null || qualityRunId == null) return false;
        Integer count = jdbc.queryForObject("select count(*) from evaluation.quality_run_registration "
                        + "where id = ? and tenant_id = ? and workspace_id = ? and purpose = 'SERVICE_REQUEST_SCENARIO'",
                Integer.class, qualityRunId, tenantId, workspaceId);
        return count != null && count == 1;
    }

    @Override
    public boolean isScenarioTask(UUID tenantId, UUID workspaceId, UUID taskId) {
        if (tenantId == null || workspaceId == null || taskId == null) return false;
        Integer count = jdbc.queryForObject("select (select count(*) from evaluation.scenario_sample s join evaluation.scenario_run r on r.id = s.run_id "
                        + "where s.task_id = ? and r.tenant_id = ? and r.workspace_id = ?) + "
                        + "(select count(*) from evaluation.team_preparation_sample s join evaluation.team_preparation_run r on r.id = s.run_id "
                        + "where s.task_id = ? and r.tenant_id = ? and r.workspace_id = ?)",
                Integer.class, taskId, tenantId, workspaceId, taskId, tenantId, workspaceId);
        return count != null && count == 1;
    }

    @Override
    public boolean isTeamImprovementRun(UUID tenantId, UUID workspaceId, UUID qualityRunId) {
        if (tenantId == null || workspaceId == null || qualityRunId == null) return false;
        Integer count = jdbc.queryForObject("select count(*) from evaluation.team_improvement_generation "
                        + "where quality_run_id = ? and tenant_id = ? and workspace_id = ?",
                Integer.class, qualityRunId, tenantId, workspaceId);
        return count != null && count == 1;
    }

    @Override
    public boolean isTeamPreparationRun(UUID tenantId, UUID workspaceId, UUID qualityRunId) {
        if (tenantId == null || workspaceId == null || qualityRunId == null) return false;
        Integer count = jdbc.queryForObject("select count(*) from evaluation.team_preparation_run "
                        + "where quality_run_id = ? and tenant_id = ? and workspace_id = ?",
                Integer.class, qualityRunId, tenantId, workspaceId);
        return count != null && count > 0;
    }

    @Override
    public boolean teamImprovementGenerationMatches(UUID tenantId, UUID workspaceId, UUID ownerId, UUID qualityRunId,
            UUID improvementRunId, String taskKey, UUID agentId, String agentVersion, String inputHash,
            TaskAssetBinding binding, Instant deadlineAt) {
        if (tenantId == null || workspaceId == null || ownerId == null || qualityRunId == null || improvementRunId == null
                || taskKey == null || agentId == null || agentVersion == null || inputHash == null || binding == null
                || deadlineAt == null) return false;
        Integer count = jdbc.queryForObject("select count(*) from evaluation.team_improvement_generation g "
                        + "join evaluation.quality_run_registration q on q.id = g.quality_run_id "
                        + "where g.quality_run_id = ? and g.tenant_id = ? and g.workspace_id = ? and g.owner_id = ? "
                        + "and g.improvement_run_id = ? and g.task_key = ? and g.agent_id = ? and g.agent_version = ? "
                        + "and g.input_hash = ? and g.capability_id = ? and g.capability_version = ? and g.capability_hash = ? "
                        + "and g.skill_id = ? and g.skill_version = ? and g.skill_hash = ? and g.deadline_at = ? "
                        + "and g.deadline_at > now() and g.status in ('READY','ACTIVE') and q.purpose = 'TEAM_EXPERIENCE_IMPROVEMENT' "
                        + "and q.source = 'EVALUATION' and q.owner_id = g.owner_id and q.status in ('REGISTERED','RUNNING')",
                Integer.class, qualityRunId, tenantId, workspaceId, ownerId, improvementRunId, taskKey, agentId,
                agentVersion, inputHash, binding.capabilityId(), binding.capabilityVersion(), binding.capabilityHash(),
                binding.skillId(), binding.skillVersion(), binding.skillHash(),
                java.sql.Timestamp.from(deadlineAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS)));
        return count != null && count == 1;
    }

    @Override
    public boolean bindTeamImprovementGenerationTask(UUID tenantId, UUID workspaceId, UUID ownerId, UUID qualityRunId,
            UUID improvementRunId, UUID taskId, int attempt, String taskKey, UUID agentId, String agentVersion,
            String inputHash, TaskAssetBinding binding, Instant deadlineAt) {
        if (taskId == null || attempt < 1 || !teamImprovementGenerationMatches(tenantId, workspaceId, ownerId,
                qualityRunId, improvementRunId, taskKey, agentId, agentVersion, inputHash, binding, deadlineAt)) return false;
        int changed = jdbc.update("update evaluation.team_improvement_generation set task_id = ?, task_attempt = ?, "
                        + "status = 'ACTIVE', updated_at = now() where quality_run_id = ? and status = 'READY' "
                        + "and task_id is null", taskId, attempt, qualityRunId);
        if (changed == 1) {
            jdbc.update("update evaluation.quality_run_registration set status = 'RUNNING' "
                            + "where id = ? and status = 'REGISTERED'", qualityRunId);
            return true;
        }
        Integer count = jdbc.queryForObject("select count(*) from evaluation.team_improvement_generation "
                        + "where quality_run_id = ? and task_id = ? and task_attempt = ? and status in ('ACTIVE','SUCCEEDED')",
                Integer.class, qualityRunId, taskId, attempt);
        return count != null && count == 1;
    }

    @Override
    public boolean isTeamImprovementTask(UUID tenantId, UUID workspaceId, UUID taskId) {
        if (tenantId == null || workspaceId == null || taskId == null) return false;
        Integer count = jdbc.queryForObject("select count(*) from evaluation.team_improvement_generation "
                        + "where tenant_id = ? and workspace_id = ? and task_id = ?",
                Integer.class, tenantId, workspaceId, taskId);
        return count != null && count == 1;
    }

    @Override
    public boolean isTeamPreparationTask(UUID tenantId, UUID workspaceId, UUID taskId) {
        if (tenantId == null || workspaceId == null || taskId == null) return false;
        Integer count = jdbc.queryForObject("select count(*) from evaluation.team_preparation_sample s "
                        + "join evaluation.team_preparation_run r on r.id = s.run_id "
                        + "where s.task_id = ? and r.tenant_id = ? and r.workspace_id = ?",
                Integer.class, taskId, tenantId, workspaceId);
        return count != null && count == 1;
    }

    private boolean teamPreparationTaskMatches(UUID tenantId, UUID workspaceId, UUID actorId, UUID taskId,
            UUID rootTaskId, UUID qualityRunId, int attempt, UUID agentId, String agentVersion, String source,
            String runKind, String inputHash, TaskAssetBinding binding) {
        var rows = jdbc.query("select r.owner_id, r.status as run_status, r.stop_requested, r.deadline_at, "
                        + "s.status as sample_status, s.task_attempt, s.input_hash, s.agent_id, s.agent_version, "
                        + "s.capability_id, s.capability_version, s.capability_hash, s.skill_id, s.skill_version, s.skill_hash "
                        + "from evaluation.team_preparation_sample s join evaluation.team_preparation_run r on r.id = s.run_id "
                        + "where s.task_id = ? and r.quality_run_id = ? and r.tenant_id = ? and r.workspace_id = ?",
                (rs, row) -> new PreparationBinding(rs.getObject("owner_id", UUID.class), rs.getString("run_status"),
                        rs.getBoolean("stop_requested"), rs.getTimestamp("deadline_at").toInstant(),
                        rs.getString("sample_status"), rs.getInt("task_attempt"), rs.getString("input_hash"),
                        rs.getObject("agent_id", UUID.class), rs.getString("agent_version"),
                        rs.getObject("capability_id", UUID.class), rs.getString("capability_version"),
                        rs.getString("capability_hash"), rs.getObject("skill_id", UUID.class),
                        rs.getString("skill_version"), rs.getString("skill_hash")),
                taskId, qualityRunId, tenantId, workspaceId);
        if (rows.size() != 1) return false;
        var row = rows.getFirst();
        return List.of("RUNNING", "STOPPING").contains(row.runStatus()) && !row.stopRequested()
                && row.deadlineAt().isAfter(Instant.now()) && "ACTIVE".equals(row.sampleStatus())
                && row.ownerId().equals(actorId) && taskId.equals(rootTaskId) && "AGENT".equals(runKind)
                && "EVALUATION".equals(source) && attempt == row.taskAttempt() && agentId.equals(row.agentId())
                && agentVersion.equals(row.agentVersion()) && inputHash.equals(row.inputHash())
                && binding.capabilityId().equals(row.capabilityId())
                && binding.capabilityVersion().equals(row.capabilityVersion())
                && binding.capabilityHash().equals(row.capabilityHash()) && binding.skillId().equals(row.skillId())
                && binding.skillVersion().equals(row.skillVersion()) && binding.skillHash().equals(row.skillHash());
    }

    @Override
    public boolean teamPreparationSampleMatches(UUID tenantId, UUID workspaceId, UUID ownerId, UUID qualityRunId,
            UUID sampleId, UUID snapshotId, String taskKey, UUID agentId, String agentVersion, String inputHash,
            TaskAssetBinding binding, Instant deadlineAt) {
        if (tenantId == null || workspaceId == null || ownerId == null || qualityRunId == null || sampleId == null
                || snapshotId == null || taskKey == null || agentId == null || agentVersion == null || inputHash == null || binding == null
                || deadlineAt == null) return false;
        Integer count = jdbc.queryForObject("select count(*) from evaluation.team_preparation_sample s "
                        + "join evaluation.team_preparation_run r on r.id = s.run_id "
                        + "join evaluation.team_preparation_snapshot p on p.snapshot_id = s.snapshot_id "
                        + "join evaluation.quality_run_registration q on q.id = r.quality_run_id "
                        + "where s.id = ? and s.run_id = ? and s.tenant_id = ? and s.workspace_id = ? and r.owner_id = ? "
                        + "and r.quality_run_id = ? and r.status = 'RUNNING' and not r.stop_requested and r.deadline_at > now() "
                        + "and r.deadline_at = ? and s.snapshot_id = ? and s.task_key = ? and s.status = 'PENDING' and s.task_id is null "
                        + "and s.input_hash = ? and s.agent_id = ? and s.agent_version = ? "
                        + "and s.capability_id = ? and s.capability_version = ? and s.capability_hash = ? "
                        + "and s.skill_id = ? and s.skill_version = ? and s.skill_hash = ? "
                        + "and p.candidate_id = r.candidate_id and p.candidate_revision = r.candidate_revision "
                        + "and p.improvement_run_id = r.improvement_run_id and q.owner_id = r.owner_id "
                        + "and q.purpose = 'TEAM_EXPERIENCE_IMPROVEMENT' and q.source = 'EVALUATION' "
                        + "and q.status in ('REGISTERED','RUNNING')",
                Integer.class, sampleId, jdbc.queryForObject("select run_id from evaluation.team_preparation_sample where id = ?",
                        UUID.class, sampleId), tenantId, workspaceId, ownerId, qualityRunId,
                Timestamp.from(deadlineAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS)), snapshotId, taskKey, inputHash,
                agentId, agentVersion, binding.capabilityId(), binding.capabilityVersion(), binding.capabilityHash(),
                binding.skillId(), binding.skillVersion(), binding.skillHash());
        return count != null && count == 1;
    }

    @Override
    public boolean bindTeamPreparationSample(UUID tenantId, UUID workspaceId, UUID ownerId, UUID qualityRunId,
            UUID sampleId, UUID snapshotId, UUID taskId, int attempt, String taskKey, UUID agentId, String agentVersion,
            String inputHash, TaskAssetBinding binding, Instant deadlineAt) {
        if (taskId == null || attempt < 1 || !teamPreparationSampleMatches(tenantId, workspaceId, ownerId,
                qualityRunId, sampleId, snapshotId, taskKey, agentId, agentVersion, inputHash, binding, deadlineAt)) return false;
        int changed = jdbc.update("update evaluation.team_preparation_sample set task_id = ?, task_attempt = ?, status = 'ACTIVE' "
                        + "where id = ? and status = 'PENDING' and task_id is null",
                taskId, attempt, sampleId);
        if (changed == 1) return true;
        Integer count = jdbc.queryForObject("select count(*) from evaluation.team_preparation_sample "
                        + "where id = ? and task_id = ? and task_attempt = ? and status in ('ACTIVE','SUCCEEDED')",
                Integer.class, sampleId, taskId, attempt);
        return count != null && count == 1;
    }

    private record ScenarioBinding(UUID id, UUID ownerId, String manifest, String runStatus, boolean stopRequested,
                                   java.time.Instant deadlineAt, String sampleStatus, int taskAttempt,
                                   String inputHash, UUID agentId, String agentVersion, UUID capabilityId,
                                   String capabilityVersion, String capabilityHash, UUID skillId,
                                   String skillVersion, String skillHash) { }
    private record PreparationBinding(UUID ownerId, String runStatus, boolean stopRequested, Instant deadlineAt,
            String sampleStatus, int taskAttempt, String inputHash, UUID agentId, String agentVersion,
            UUID capabilityId, String capabilityVersion, String capabilityHash, UUID skillId,
            String skillVersion, String skillHash) { }
}
