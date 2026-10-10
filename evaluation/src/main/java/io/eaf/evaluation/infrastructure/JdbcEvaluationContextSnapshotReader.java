package io.eaf.evaluation.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.context.api.EvaluationContextSnapshotReader;
import io.eaf.context.api.EnterpriseContext;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.memory.api.MemoryService;
import io.eaf.memory.api.TeamExperienceService;
import io.eaf.shared.ActorContext;
import io.eaf.context.api.EvaluationContextSnapshotReader.PromptCandidateTaskBinding;
import io.eaf.shared.Hashing;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcEvaluationContextSnapshotReader implements EvaluationContextSnapshotReader {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final KnowledgeService knowledge;
    private final MemoryService memories;
    private final TeamExperienceService teamExperiences;
    private final WorkspaceAuthorization workspaces;
    private final Clock clock;

    public JdbcEvaluationContextSnapshotReader(JdbcTemplate jdbc, ObjectMapper json, KnowledgeService knowledge,
                                               MemoryService memories, TeamExperienceService teamExperiences,
                                               WorkspaceAuthorization workspaces, Clock clock) {
        this.jdbc = jdbc; this.json = json; this.knowledge = knowledge; this.memories = memories;
        this.teamExperiences = teamExperiences; this.workspaces = workspaces; this.clock = clock;
    }

    @Override
    public Optional<EnterpriseContext> readForTask(ActorContext actor, UUID workspaceId, UUID taskId, UUID snapshotId) {
        var row = load(actor, workspaceId, taskId, snapshotId);
        if (row == null) return Optional.empty();
        try {
            var baseline = parse(row.baselineJson(), row.baselineHash());
            var candidate = parse(row.candidateJson(), row.candidateHash());
            if (baseline == null || candidate == null || !baseIsCurrent(actor, workspaceId, row, baseline)
                    || candidate.items().stream().anyMatch(item -> item.expiresAt() != null && !item.expiresAt().isAfter(java.time.Instant.now(clock))))
                return Optional.empty();
            var selected = "BASELINE".equals(row.variant()) ? baseline : candidate;
            return Optional.of(selected);
        } catch (RuntimeException invalidOrRevoked) {
            return Optional.empty();
        }
    }

    @Override
    public boolean isCurrentForTask(ActorContext actor, UUID workspaceId, UUID taskId, UUID snapshotId,
                                    EnterpriseContext context) {
        var current = readForTask(actor, workspaceId, taskId, snapshotId);
        return current.isPresent() && current.get().equals(context);
    }

    @Override
    public Optional<PromptCandidateTaskBinding> promptCandidateForTask(ActorContext actor, UUID workspaceId, UUID taskId) {
        if (actor == null || actor.delegated() || workspaceId == null || taskId == null) return Optional.empty();
        var binding = jdbc.query("select p.candidate_id, p.candidate_revision from evaluation.scenario_sample s "
                        + "join evaluation.scenario_run r on r.id = s.run_id "
                        + "join evaluation.prompt_analysis_run p on p.run_id = s.run_id and p.tenant_id = r.tenant_id and p.workspace_id = r.workspace_id "
                        + "where s.task_id = ? and r.tenant_id = ? and r.workspace_id = ? and s.side = 'COMPARISON' "
                        + "and r.owner_id = ? and r.status in ('QUEUED','RUNNING','STOPPING','COMPLETED')",
                rs -> rs.next() ? new PromptCandidateTaskBinding(rs.getObject("candidate_id", UUID.class),
                        rs.getInt("candidate_revision")) : null,
                taskId, actor.tenantId(), workspaceId, actor.actorId());
        return Optional.ofNullable(binding);
    }

    private SnapshotRow load(ActorContext actor, UUID workspaceId, UUID taskId, UUID snapshotId) {
        if (actor == null || workspaceId == null || taskId == null || snapshotId == null) return null;
        return jdbc.query("select s.id as snapshot_id, s.target_type, s.target_id, s.base_version, s.owner_id, s.scope, s.candidate_content_hash, "
                        + "s.baseline_context::text, s.baseline_context_hash, s.candidate_context::text, s.candidate_context_hash, p.variant "
                        + "from evaluation.candidate_context_snapshot s join evaluation.candidate_pair_task p "
                        + "on p.snapshot_id = s.id and p.tenant_id = s.tenant_id and p.workspace_id = s.workspace_id "
                        + "where s.id = ? and s.tenant_id = ? and s.workspace_id = ? and s.invalidated_at is null "
                        + "and p.task_id = ? and p.tenant_id = ? and p.workspace_id = ? "
                        + "union all select s.id as snapshot_id, s.target_type, s.target_id, s.base_version, s.owner_id, s.scope, s.candidate_content_hash, "
                        + "s.baseline_context::text, s.baseline_context_hash, s.candidate_context::text, s.candidate_context_hash, "
                        + "case when p.side = 'BASELINE' then 'BASELINE' else 'CANDIDATE' end as variant "
                        + "from evaluation.candidate_context_snapshot s join evaluation.team_preparation_sample p "
                        + "on p.snapshot_id = s.id and p.tenant_id = s.tenant_id and p.workspace_id = s.workspace_id "
                        + "join evaluation.team_preparation_run r on r.id = p.run_id "
                        + "where s.id = ? and s.tenant_id = ? and s.workspace_id = ? and s.invalidated_at is null "
                        + "and p.task_id = ? and p.tenant_id = ? and p.workspace_id = ? and r.status in ('RUNNING','STOPPING','COMPLETED')",
                rs -> rs.next() ? new SnapshotRow(rs.getObject("snapshot_id", UUID.class), rs.getString("target_type"), rs.getObject("target_id", UUID.class),
                        rs.getString("base_version"), rs.getObject("owner_id", UUID.class), rs.getString("scope"),
                        rs.getString("candidate_content_hash"), rs.getString("baseline_context"),
                        rs.getString("baseline_context_hash"), rs.getString("candidate_context"),
                        rs.getString("candidate_context_hash"), rs.getString("variant")) : null,
                snapshotId, actor.tenantId(), workspaceId, taskId, actor.tenantId(), workspaceId,
                snapshotId, actor.tenantId(), workspaceId, taskId, actor.tenantId(), workspaceId);
    }

    private EnterpriseContext parse(String stored, String expectedHash) {
        try {
            var context = json.readValue(stored, EnterpriseContext.class);
            return Hashing.sha256(json.writeValueAsString(context)).equals(expectedHash) ? context : null;
        } catch (Exception invalid) { return null; }
    }

    private boolean baseIsCurrent(ActorContext actor, UUID workspaceId, SnapshotRow row, EnterpriseContext baseline) {
        if ("KNOWLEDGE_UPDATE".equals(row.targetType())) {
            if (row.targetId() == null || row.baseVersion() == null || baseline.items().size() != 1) return false;
            var version = Integer.parseInt(row.baseVersion());
            var publication = knowledge.getCurrentPublication(actor, workspaceId, row.targetId());
            return publication.assetVersion() == version && "PUBLISHED".equals(publication.documentStatus())
                    && publication.contentHash().equals(baseline.items().get(0).contentHash());
        }
        if ("TEAM_EXPERIENCE_UPDATE".equals(row.targetType())) {
            var binding = jdbc.query("select card_id, base_revision, base_memory_version, expected_card_version, "
                            + "scenario_key, expires_at, source_work_item_id, owner_id from evaluation.team_preparation_snapshot "
                            + "where snapshot_id = ? and tenant_id = ? and workspace_id = ?",
                    rs -> rs.next() ? new TeamSnapshotBinding(rs.getObject("card_id", UUID.class), rs.getInt("base_revision"),
                            rs.getString("base_memory_version"), rs.getLong("expected_card_version"), rs.getString("scenario_key"),
                            rs.getTimestamp("expires_at").toInstant(), rs.getObject("source_work_item_id", UUID.class),
                            rs.getObject("owner_id", UUID.class)) : null,
                    row.snapshotId(), actor.tenantId(), workspaceId);
            if (binding == null || !binding.cardId().equals(row.targetId())
                    || !binding.baseMemoryVersion().equals(row.baseVersion())
                    || !binding.ownerId().equals(row.ownerId()) || !binding.expiresAt().isAfter(java.time.Instant.now(clock))
                    || baseline.items().size() != 1) return false;
            var card = teamExperiences.get(actor, workspaceId, binding.cardId());
            if (!card.ownerId().equals(binding.ownerId()) || card.version() != binding.expectedCardVersion()
                    || card.latestRevision() != binding.baseRevision()
                    || !Integer.valueOf(binding.baseRevision()).equals(card.activeRevision()) || card.active() == null
                    || !"PUBLISHED".equals(card.active().status()) || !binding.baseMemoryVersion().equals(card.active().memoryVersion())
                    || !binding.sourceWorkItemId().equals(card.active().source().workItemId())
                    || !binding.scenarioKey().equals(card.scenarioKey())
                    || !binding.expiresAt().equals(card.active().expiresAt())) return false;
            var memory = memories.requireUsable(actor, workspaceId, binding.cardId(), binding.baseMemoryVersion());
            var item = baseline.items().getFirst();
            return "PUBLISHED".equals(memory.status()) && "TEAM".equals(memory.scope())
                    && binding.cardId().equals(item.memoryId()) && binding.baseMemoryVersion().equals(item.memoryVersion())
                    && memory.contentHash().equals(item.contentHash()) && "TEAM".equals(item.scope());
        }
        if (!"MEMORY_UPSERT".equals(row.targetType())) return false;
        if (row.targetId() == null) {
            workspaces.require(actor, workspaceId, "memory:read");
            if ("TEAM".equals(row.scope())) workspaces.require(actor, workspaceId, "memory:scope:team");
            return row.baseVersion() == null && baseline.items().isEmpty();
        }
        if (row.baseVersion() == null || baseline.items().size() != 1) return false;
        var current = memories.requireUsable(actor, workspaceId, row.targetId(), row.baseVersion());
        return current.contentHash().equals(baseline.items().get(0).contentHash());
    }

    private record SnapshotRow(UUID snapshotId, String targetType, UUID targetId, String baseVersion, UUID ownerId, String scope,
                               String candidateContentHash, String baselineJson, String baselineHash,
                               String candidateJson, String candidateHash, String variant) { }

    private record TeamSnapshotBinding(UUID cardId, int baseRevision, String baseMemoryVersion,
            long expectedCardVersion, String scenarioKey, java.time.Instant expiresAt,
            UUID sourceWorkItemId, UUID ownerId) { }
}
// 每次模型调用前复核候选实验绑定、快照摘要和正式基线授权/版本，撤权后拒绝继续使用旧正文。
