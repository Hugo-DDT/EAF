package io.eaf.agent.infrastructure;

import io.eaf.agent.api.AgentCatalog;
import io.eaf.agent.api.AgentDefinition;
import io.eaf.agent.api.AgentOwnerService;
import io.eaf.agent.api.AgentOwnerService.AgentPromptVariant;
import io.eaf.prompt.api.PromptCatalog;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcAgentOwnerService implements AgentOwnerService {
    private static final UUID P15_AGENT = UUID.fromString("20000000-0000-4000-8000-000000000010");
    private final JdbcTemplate jdbc;
    private final AgentCatalog agents;
    private final PromptCatalog prompts;
    private final WorkspaceAuthorization workspaces;

    public JdbcAgentOwnerService(JdbcTemplate jdbc, AgentCatalog agents, PromptCatalog prompts,
            WorkspaceAuthorization workspaces) {
        this.jdbc = jdbc; this.agents = agents; this.prompts = prompts; this.workspaces = workspaces;
    }

    @Override
    @Transactional
    public AgentPromptVariant publishPromptAnalysisVariant(ActorContext actor, UUID workspaceId, UUID candidateId,
            int revision, UUID adoptionId, UUID promptId, String promptVersion, String promptHash,
            UUID approvalId, UUID reportId, String reportHash) {
        require(actor, workspaceId, "agent:write");
        workspaces.require(actor, workspaceId, "agent:publish");
        if (candidateId == null || revision < 1 || adoptionId == null || promptId == null
                || !"1.0.0".equals(promptVersion) || !hex(promptHash) || approvalId == null || reportId == null || !hex(reportHash))
            throw EafException.invalid("Agent 派生来源必须绑定精确 Prompt、批准和报告。");
        var prior = find(actor, workspaceId, candidateId, revision, adoptionId);
        if (prior != null) {
            if (!prior.promptId().equals(promptId) || !prior.promptVersion().equals(promptVersion)
                    || !prior.promptHash().equals(promptHash) || !prior.approvalId().equals(approvalId)
                    || !prior.reportId().equals(reportId) || !prior.reportHash().equals(reportHash))
                throw EafException.conflict("PROMPT_VARIANT_CONFLICT", "Agent 派生来源已绑定其他 Prompt。");
            return prior;
        }
        var base = agents.requirePublished(actor.tenantId(), workspaceId, P15_AGENT, "1.0.0");
        if (!agents.tools(actor.tenantId(), workspaceId, base.id(), base.version()).isEmpty())
            throw EafException.conflict("PROMPT_VARIANT_BASE_UNSAFE", "P15 基线 Agent 必须没有 Tool。");
        prompts.requirePublished(actor.tenantId(), workspaceId, promptId, promptVersion);
        var id = UUID.randomUUID();
        var version = "1.0.0";
        var name = "service-request-plan-p31-" + id.toString().substring(0, 8);
        try {
            jdbc.update("insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, "
                            + "model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy) "
                            + "select ?, tenant_id, workspace_id, ?, ?, ?, ?, model_profile_id, 'PUBLISHED', rag_enabled, "
                            + "response_profile, retrieval_mode, evidence_policy from agent.version "
                            + "where id = ? and tenant_id = ? and workspace_id = ? and asset_version = ? and status = 'PUBLISHED'",
                    id, name, version, promptId, promptVersion, base.id(), actor.tenantId(), workspaceId, base.version());
            if (agents.requirePublished(actor.tenantId(), workspaceId, id, version) == null) throw EafException.notFound();
            jdbc.update("insert into agent.prompt_variant_origin(tenant_id, workspace_id, candidate_id, candidate_revision, adoption_id, "
                            + "owner_id, agent_id, agent_version, base_agent_id, base_agent_version, prompt_id, prompt_version, prompt_hash, "
                            + "approval_id, report_id, report_hash, status) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PUBLISHED')",
                    actor.tenantId(), workspaceId, candidateId, revision, adoptionId, actor.actorId(), id, version, base.id(),
                    base.version(), promptId, promptVersion, promptHash, approvalId, reportId, reportHash);
        } catch (DuplicateKeyException conflict) {
            var replay = find(actor, workspaceId, candidateId, revision, adoptionId);
            if (replay == null) throw EafException.conflict("PROMPT_VARIANT_CONFLICT", "Agent 派生版本来源冲突。");
            return replay;
        }
        return find(actor, workspaceId, candidateId, revision, adoptionId);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<AgentPromptVariant> findPromptAnalysisVariant(ActorContext actor, UUID workspaceId,
            UUID candidateId, int revision, UUID adoptionId) {
        require(actor, workspaceId, "agent:read");
        return Optional.ofNullable(find(actor, workspaceId, candidateId, revision, adoptionId));
    }

    @Override
    @Transactional
    public AgentPromptVariant revokePromptAnalysisVariant(ActorContext actor, UUID workspaceId,
            UUID candidateId, int revision, UUID adoptionId) {
        require(actor, workspaceId, "agent:publish");
        var variant = find(actor, workspaceId, candidateId, revision, adoptionId);
        if (variant == null) throw EafException.notFound();
        jdbc.update("update agent.version set status = 'REVOKED' where id = ? and tenant_id = ? and workspace_id = ? "
                        + "and asset_version = ? and status = 'PUBLISHED'", variant.id(), actor.tenantId(), workspaceId, variant.version());
        jdbc.update("update agent.prompt_variant_origin set status = 'REVOKED' where tenant_id = ? and workspace_id = ? "
                        + "and candidate_id = ? and candidate_revision = ? and adoption_id = ? and status = 'PUBLISHED'",
                actor.tenantId(), workspaceId, candidateId, revision, adoptionId);
        return find(actor, workspaceId, candidateId, revision, adoptionId);
    }

    private AgentPromptVariant find(ActorContext actor, UUID workspaceId, UUID candidateId, int revision, UUID adoptionId) {
        return jdbc.query("select o.agent_id, o.agent_version, o.base_agent_id, o.base_agent_version, o.prompt_id, o.prompt_version, "
                        + "o.prompt_hash, o.approval_id, o.report_id, o.report_hash, "
                        + "o.status, a.model_profile_id, a.rag_enabled, a.response_profile, a.retrieval_mode, a.evidence_policy "
                        + "from agent.prompt_variant_origin o join agent.version a on a.id = o.agent_id and a.tenant_id = o.tenant_id "
                        + "and a.workspace_id = o.workspace_id and a.asset_version = o.agent_version "
                        + "where o.tenant_id = ? and o.workspace_id = ? and o.owner_id = ? and o.candidate_id = ? "
                        + "and o.candidate_revision = ? and o.adoption_id = ?",
                rs -> rs.next() ? new AgentPromptVariant(rs.getObject("agent_id", UUID.class), rs.getString("agent_version"),
                        rs.getObject("base_agent_id", UUID.class), rs.getString("base_agent_version"),
                        rs.getObject("prompt_id", UUID.class), rs.getString("prompt_version"),
                        rs.getString("prompt_hash"), rs.getObject("approval_id", UUID.class),
                        rs.getObject("report_id", UUID.class), rs.getString("report_hash"),
                        Hashing.sha256(String.join("|", rs.getString("model_profile_id"), rs.getString("rag_enabled"),
                                rs.getString("response_profile"), rs.getString("retrieval_mode"), rs.getString("evidence_policy"),
                                rs.getString("prompt_id"), rs.getString("prompt_version"))), rs.getString("status")) : null,
                actor.tenantId(), workspaceId, actor.actorId(), candidateId, revision, adoptionId);
    }

    private void require(ActorContext actor, UUID workspaceId, String permission) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("P31 Agent 变体只允许直接 HUMAN Owner 操作。");
        workspaces.require(actor, workspaceId, "agent:read");
        workspaces.require(actor, workspaceId, permission);
    }

    private boolean hex(String value) { return value != null && value.matches("[0-9a-f]{64}"); }
}
