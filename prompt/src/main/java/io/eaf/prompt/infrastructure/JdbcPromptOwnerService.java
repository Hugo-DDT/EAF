package io.eaf.prompt.infrastructure;

import io.eaf.prompt.api.PromptCatalog;
import io.eaf.prompt.api.PromptOwnerService;
import io.eaf.prompt.api.PromptOwnerService.PromptCandidateSnapshot;
import io.eaf.prompt.api.PromptOwnerService.PromptRelease;
import io.eaf.prompt.api.PromptOwnerService.PromptTarget;
import io.eaf.prompt.api.PromptOwnerService.PromptWithdrawal;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcPromptOwnerService implements PromptOwnerService {
    private static final UUID ANALYSIS_BASE_PROMPT = UUID.fromString("21000000-0000-4000-8000-00000000000f");
    private static final String BASE_VERSION = "1.0.0";
    private final JdbcTemplate jdbc;
    private final PromptCatalog prompts;
    private final WorkspaceAuthorization workspaces;

    public JdbcPromptOwnerService(JdbcTemplate jdbc, PromptCatalog prompts, WorkspaceAuthorization workspaces) {
        this.jdbc = jdbc;
        this.prompts = prompts;
        this.workspaces = workspaces;
    }

    @Override
    @Transactional
    public PromptTarget registerAnalysisTarget(ActorContext actor, UUID workspaceId, String idempotencyKey) {
        requireActor(actor, workspaceId, "prompt:manage");
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200
                || idempotencyKey.chars().anyMatch(Character::isISOControl))
            throw EafException.invalid("Prompt 目标登记需要有效 Idempotency-Key。");
        var base = prompts.requirePublished(actor.tenantId(), workspaceId, ANALYSIS_BASE_PROMPT, BASE_VERSION);
        var baseHash = contentHash(base.systemTemplate(), base.userTemplate());
        var keyHash = Hashing.sha256(actor.tenantId() + "|" + workspaceId + "|" + actor.actorId() + "|" + idempotencyKey);
        var existing = targetByKey(actor, workspaceId, keyHash);
        if (existing != null) {
            if (!existing.baseHash().equals(baseHash)) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "Prompt 目标登记键已绑定其他基线。");
            return existing;
        }
        var targetId = UUID.randomUUID();
        try {
            jdbc.update("insert into prompt.owner_target(id, tenant_id, workspace_id, owner_id, base_prompt_id, base_version, base_hash, idempotency_key_hash, status) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, 'READY')",
                    targetId, actor.tenantId(), workspaceId, actor.actorId(), ANALYSIS_BASE_PROMPT, BASE_VERSION, baseHash, keyHash);
        } catch (DuplicateKeyException conflict) {
            var replay = targetByKey(actor, workspaceId, keyHash);
            if (replay == null || !replay.baseHash().equals(baseHash))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "Prompt 目标登记竞争绑定了不同基线。");
            return replay;
        }
        return getTarget(actor, workspaceId, targetId);
    }

    @Override
    @Transactional(readOnly = true)
    public PromptTarget getTarget(ActorContext actor, UUID workspaceId, UUID targetId) {
        requireActor(actor, workspaceId, "prompt:read");
        var target = target(actor.tenantId(), workspaceId, targetId, false);
        if (target == null || !target.ownerId().equals(actor.actorId())) throw EafException.notFound();
        return target;
    }

    @Override
    @Transactional(readOnly = true)
    public void assertCandidateAccessible(ActorContext actor, UUID workspaceId, UUID targetId,
            UUID candidateId, int candidateRevision, String baseHash) {
        requireActor(actor, workspaceId, "prompt:read");
        var snapshot = candidate(actor.tenantId(), workspaceId, candidateId, candidateRevision);
        if (snapshot == null || !snapshot.targetId().equals(targetId) || !snapshot.baseHash().equals(baseHash)
                || !List.of("STAGED", "RELEASED").contains(snapshot.status())) throw EafException.notFound();
    }

    @Override
    @Transactional
    public PromptCandidateSnapshot stageAnalysisCandidate(StageAnalysisCandidate command) {
        if (command == null || command.targetId() == null || command.candidateId() == null
                || command.candidateRevision() < 1 || command.expectedTargetVersion() < 1)
            throw EafException.invalid("Prompt 候选登记请求不完整。");
        requireActor(command.actor(), command.workspaceId(), "prompt:manage");
        var target = target(command.actor().tenantId(), command.workspaceId(), command.targetId(), true);
        if (target == null || !target.ownerId().equals(command.actor().actorId())) throw EafException.notFound();
        var base = prompts.requirePublished(command.actor().tenantId(), command.workspaceId(), target.basePromptId(), target.baseVersion());
        if (!target.baseHash().equals(contentHash(base.systemTemplate(), base.userTemplate())))
            throw EafException.conflict("PROMPT_BASE_CHANGED", "固定 Prompt 基线已变化。");
        var appendix = command.instructionAppendix();
        if (appendix == null || appendix.isBlank() || appendix.trim().length() > 1_000
                || appendix.chars().anyMatch(ch -> Character.isISOControl(ch) && ch != '\n' && ch != '\r' && ch != '\t'))
            throw EafException.invalid("补充说明须为 1—1000 字符的单段文本。");
        var system = base.systemTemplate() + "\n\n" + appendix.trim();
        if (system.length() > 8_000) throw EafException.invalid("候选 Prompt 超过 8,000 字符限制。");
        var hash = contentHash(system, base.userTemplate());
        var existing = candidate(command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
        if (existing != null) {
            if (!existing.targetId().equals(target.id()) || !existing.contentHash().equals(hash))
                throw EafException.conflict("PROMPT_CANDIDATE_CONFLICT", "候选修订已绑定不同 Prompt 内容。");
            return existing;
        }
        if (target.rowVersion() != command.expectedTargetVersion() || !List.of("READY", "STAGED").contains(target.status()))
            throw EafException.conflict("PROMPT_TARGET_CHANGED", "Prompt 目标状态或版本已变化。");
        jdbc.update("update prompt.analysis_candidate set status = 'SUPERSEDED' where tenant_id = ? and workspace_id = ? and target_id = ? and status = 'STAGED'",
                command.actor().tenantId(), command.workspaceId(), target.id());
        jdbc.update("insert into prompt.analysis_candidate(tenant_id, workspace_id, target_id, candidate_id, candidate_revision, base_hash, system_template, user_template, content_hash, status) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'STAGED')",
                command.actor().tenantId(), command.workspaceId(), target.id(), command.candidateId(), command.candidateRevision(),
                target.baseHash(), system, base.userTemplate(), hash);
        jdbc.update("update prompt.owner_target set status = 'STAGED', row_version = row_version + 1, updated_at = now() "
                        + "where id = ? and row_version = ? and status in ('READY','STAGED')",
                target.id(), target.rowVersion());
        return candidate(command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
    }

    @Override
    @Transactional(readOnly = true)
    public PromptCandidateSnapshot requireEvaluationCandidate(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision) {
        requireActor(actor, workspaceId, "prompt:read");
        var snapshot = candidate(actor.tenantId(), workspaceId, candidateId, candidateRevision);
        if (snapshot == null || !"STAGED".equals(snapshot.status())) throw EafException.notFound();
        var target = target(actor.tenantId(), workspaceId, snapshot.targetId(), false);
        if (target == null || !target.ownerId().equals(actor.actorId()) || !"STAGED".equals(target.status()))
            throw EafException.notFound();
        var base = prompts.requirePublished(actor.tenantId(), workspaceId, target.basePromptId(), target.baseVersion());
        if (!snapshot.baseHash().equals(target.baseHash())
                || !target.baseHash().equals(contentHash(base.systemTemplate(), base.userTemplate())))
            throw EafException.conflict("PROMPT_BASE_CHANGED", "隔离评测 Prompt 基线已变化。");
        return snapshot;
    }

    @Override
    public io.eaf.prompt.api.RenderedPrompt renderEvaluationCandidate(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision, String input) {
        var candidate = requireEvaluationCandidate(actor, workspaceId, candidateId, candidateRevision);
        if (input == null || input.length() > 8_000) throw EafException.invalid("input 超过 8,000 字符限制。");
        return new io.eaf.prompt.api.RenderedPrompt("P31_PROMPT_CANDIDATE", List.of(
                new io.eaf.prompt.api.RenderedPrompt.Message("system", candidate.systemTemplate()),
                new io.eaf.prompt.api.RenderedPrompt.Message("user", candidate.userTemplate().replace("{{input}}", input))));
    }

    @Override
    @Transactional
    public PromptRelease publishAnalysisCandidate(PublishAnalysisCandidate command) {
        if (command == null || command.targetId() == null || command.candidateId() == null
                || command.candidateRevision() < 1 || command.approvalId() == null || command.reportId() == null
                || command.reportHash() == null || !command.reportHash().matches("[0-9a-f]{64}"))
            throw EafException.invalid("Prompt 发布必须绑定精确批准和评测证据。");
        requireActor(command.actor(), command.workspaceId(), "prompt:publish");
        var prior = release(command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
        if (prior != null) {
            if (!prior.approvalId().equals(command.approvalId()) || !prior.reportId().equals(command.reportId())
                    || !prior.reportHash().equals(command.reportHash()))
                throw EafException.conflict("PROMPT_RELEASE_CONFLICT", "Prompt 候选已绑定不同的发布证据。");
            return prior;
        }
        var target = target(command.actor().tenantId(), command.workspaceId(), command.targetId(), true);
        if (target == null || !target.ownerId().equals(command.actor().actorId())) throw EafException.notFound();
        if (target.rowVersion() != command.expectedTargetVersion() || !"STAGED".equals(target.status()))
            throw EafException.conflict("PROMPT_TARGET_CHANGED", "Prompt 目标不是当前待发布版本。");
        var candidate = candidate(command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
        if (candidate == null || !candidate.targetId().equals(target.id()) || !"STAGED".equals(candidate.status()))
            throw EafException.conflict("PROMPT_CANDIDATE_CHANGED", "Prompt 候选修订已变化。");
        var base = prompts.requirePublished(command.actor().tenantId(), command.workspaceId(), target.basePromptId(), target.baseVersion());
        if (!target.baseHash().equals(contentHash(base.systemTemplate(), base.userTemplate()))
                || !target.baseHash().equals(candidate.baseHash()))
            throw EafException.conflict("PROMPT_BASE_CHANGED", "Prompt 发布基线已变化。");
        var releaseId = UUID.randomUUID();
        jdbc.update("insert into prompt.version(id, tenant_id, workspace_id, asset_version, system_template, user_template, status) "
                        + "values (?, ?, ?, '1.0.0', ?, ?, 'PUBLISHED')",
                target.id(), command.actor().tenantId(), command.workspaceId(), candidate.systemTemplate(), candidate.userTemplate());
        jdbc.update("insert into prompt.analysis_release(id, tenant_id, workspace_id, target_id, candidate_id, candidate_revision, "
                        + "prompt_id, prompt_version, content_hash, approval_id, report_id, report_hash, status) "
                        + "values (?, ?, ?, ?, ?, ?, ?, '1.0.0', ?, ?, ?, ?, 'RELEASED')",
                releaseId, command.actor().tenantId(), command.workspaceId(), target.id(), command.candidateId(),
                command.candidateRevision(), target.id(), candidate.contentHash(), command.approvalId(), command.reportId(), command.reportHash());
        jdbc.update("update prompt.analysis_candidate set status = 'RELEASED' where tenant_id = ? and workspace_id = ? and candidate_id = ? and candidate_revision = ? and status = 'STAGED'",
                command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
        jdbc.update("update prompt.owner_target set status = 'PUBLISHED', row_version = row_version + 1, updated_at = now() where id = ? and row_version = ?",
                target.id(), target.rowVersion());
        return release(command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<PromptRelease> findReleaseByOrigin(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision) {
        requireActor(actor, workspaceId, "prompt:read");
        return Optional.ofNullable(release(actor.tenantId(), workspaceId, candidateId, candidateRevision));
    }

    @Override
    @Transactional
    public PromptWithdrawal revokeAnalysisCandidate(RevokeAnalysisCandidate command) {
        if (command == null || command.candidateId() == null || command.candidateRevision() < 1)
            throw EafException.invalid("Prompt 撤回来源不完整。");
        requireActor(command.actor(), command.workspaceId(), "prompt:publish");
        var existing = withdrawal(command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
        if (existing != null) return existing;
        var release = release(command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
        if (release == null || !"RELEASED".equals(release.status())) throw EafException.notFound();
        var target = target(command.actor().tenantId(), command.workspaceId(), release.targetId(), true);
        if (target == null || !target.ownerId().equals(command.actor().actorId())) throw EafException.notFound();
        jdbc.update("update prompt.version set status = 'REVOKED' where id = ? and tenant_id = ? and workspace_id = ? and asset_version = ? and status = 'PUBLISHED'",
                release.promptId(), command.actor().tenantId(), command.workspaceId(), release.promptVersion());
        jdbc.update("update prompt.analysis_release set status = 'REVOKED' where id = ? and status = 'RELEASED'", release.id());
        jdbc.update("update prompt.analysis_candidate set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and candidate_id = ? and candidate_revision = ?",
                command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
        jdbc.update("update prompt.owner_target set status = 'WITHDRAWN', row_version = row_version + 1, updated_at = now() where id = ? and status = 'PUBLISHED'",
                target.id());
        var withdrawalId = UUID.randomUUID();
        jdbc.update("insert into prompt.analysis_withdrawal(id, tenant_id, workspace_id, release_id, candidate_id, candidate_revision, prompt_id, prompt_version, content_hash, status) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'REVOKED')",
                withdrawalId, command.actor().tenantId(), command.workspaceId(), release.id(), command.candidateId(),
                command.candidateRevision(), release.promptId(), release.promptVersion(), release.contentHash());
        return withdrawal(command.actor().tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<PromptWithdrawal> findWithdrawalByOrigin(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision) {
        requireActor(actor, workspaceId, "prompt:read");
        return Optional.ofNullable(withdrawal(actor.tenantId(), workspaceId, candidateId, candidateRevision));
    }

    private void requireActor(ActorContext actor, UUID workspaceId, String permission) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("Prompt 派生资产只允许直接 HUMAN Owner 操作。");
        workspaces.require(actor, workspaceId, "prompt:read");
        workspaces.require(actor, workspaceId, permission);
    }

    private String contentHash(String system, String user) { return Hashing.sha256(system + "\u001f" + user); }

    private PromptTarget targetByKey(ActorContext actor, UUID workspaceId, String keyHash) {
        return jdbc.query("select id, owner_id, base_prompt_id, base_version, base_hash, row_version, status, updated_at from prompt.owner_target "
                        + "where tenant_id = ? and workspace_id = ? and owner_id = ? and idempotency_key_hash = ?",
                rs -> rs.next() ? mapTarget(rs) : null, actor.tenantId(), workspaceId, actor.actorId(), keyHash);
    }

    private PromptTarget target(UUID tenantId, UUID workspaceId, UUID id, boolean lock) {
        return jdbc.query("select id, owner_id, base_prompt_id, base_version, base_hash, row_version, status, updated_at from prompt.owner_target "
                        + "where tenant_id = ? and workspace_id = ? and id = ?" + (lock ? " for update" : ""),
                rs -> rs.next() ? mapTarget(rs) : null, tenantId, workspaceId, id);
    }

    private PromptTarget mapTarget(ResultSet rs) throws SQLException {
        return new PromptTarget(rs.getObject("id", UUID.class), rs.getObject("owner_id", UUID.class),
                rs.getObject("base_prompt_id", UUID.class), rs.getString("base_version"), rs.getString("base_hash"),
                rs.getLong("row_version"), rs.getString("status"), rs.getTimestamp("updated_at").toInstant());
    }

    private PromptCandidateSnapshot candidate(UUID tenantId, UUID workspaceId, UUID candidateId, int revision) {
        return jdbc.query("select target_id, candidate_id, candidate_revision, base_hash, system_template, user_template, content_hash, status "
                        + "from prompt.analysis_candidate where tenant_id = ? and workspace_id = ? and candidate_id = ? and candidate_revision = ?",
                rs -> rs.next() ? new PromptCandidateSnapshot(rs.getObject("target_id", UUID.class),
                        rs.getObject("candidate_id", UUID.class), rs.getInt("candidate_revision"), rs.getString("base_hash"),
                        rs.getString("system_template"), rs.getString("user_template"), rs.getString("content_hash"), rs.getString("status")) : null,
                tenantId, workspaceId, candidateId, revision);
    }

    private PromptRelease release(UUID tenantId, UUID workspaceId, UUID candidateId, int revision) {
        return jdbc.query("select id, target_id, candidate_id, candidate_revision, prompt_id, prompt_version, content_hash, approval_id, report_id, report_hash, status, created_at "
                        + "from prompt.analysis_release where tenant_id = ? and workspace_id = ? and candidate_id = ? and candidate_revision = ?",
                rs -> rs.next() ? mapRelease(rs) : null, tenantId, workspaceId, candidateId, revision);
    }

    private PromptRelease mapRelease(ResultSet rs) throws SQLException {
        return new PromptRelease(rs.getObject("id", UUID.class), rs.getObject("target_id", UUID.class),
                rs.getObject("candidate_id", UUID.class), rs.getInt("candidate_revision"), rs.getObject("prompt_id", UUID.class),
                rs.getString("prompt_version"), rs.getString("content_hash"), rs.getObject("approval_id", UUID.class),
                rs.getObject("report_id", UUID.class), rs.getString("report_hash"), rs.getString("status"),
                rs.getTimestamp("created_at").toInstant());
    }

    private PromptWithdrawal withdrawal(UUID tenantId, UUID workspaceId, UUID candidateId, int revision) {
        return jdbc.query("select id, release_id, candidate_id, candidate_revision, prompt_id, prompt_version, content_hash, status, created_at "
                        + "from prompt.analysis_withdrawal where tenant_id = ? and workspace_id = ? and candidate_id = ? and candidate_revision = ?",
                rs -> rs.next() ? new PromptWithdrawal(rs.getObject("id", UUID.class), rs.getObject("release_id", UUID.class),
                        rs.getObject("candidate_id", UUID.class), rs.getInt("candidate_revision"), rs.getObject("prompt_id", UUID.class),
                        rs.getString("prompt_version"), rs.getString("content_hash"), rs.getString("status"), rs.getTimestamp("created_at").toInstant()) : null,
                tenantId, workspaceId, candidateId, revision);
    }
}
