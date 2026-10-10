package io.eaf.capability.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.agent.api.AgentOwnerService;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.capability.api.CapabilityToolDependency;
import io.eaf.capability.api.CapabilityToolReference;
import io.eaf.capability.api.CreateCapabilityCommand;
import io.eaf.capability.api.CreateCapabilityVersionCommand;
import io.eaf.identity.api.DelegationResourceAuthorizer;
import io.eaf.identity.api.McpCapabilityReference;
import io.eaf.prompt.api.PromptCatalog;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.skill.api.SkillDefinition;
import io.eaf.skill.api.SkillService;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.tool.api.ToolDefinition;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcCapabilityService implements CapabilityService, DelegationResourceAuthorizer {
    private static final int MAX_REFERENCE_COUNT = 16;
    private static final UUID MCP_SERVICE_REQUEST_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000012");
    private static final UUID MCP_SERVICE_REQUEST_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000010");
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final AgentCatalog agents;
    private final AgentOwnerService agentOwners;
    private final SkillService skills;
    private final PromptCatalog prompts;
    private final ToolCatalog tools;
    private final ObjectMapper json;

    public JdbcCapabilityService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces, AgentCatalog agents,
                                 AgentOwnerService agentOwners, SkillService skills, PromptCatalog prompts,
                                 ToolCatalog tools, ObjectMapper json) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.agents = agents;
        this.agentOwners = agentOwners;
        this.skills = skills;
        this.prompts = prompts;
        this.tools = tools;
        this.json = json;
    }

    @Override
    @Transactional
    public CapabilityDefinition create(CreateCapabilityCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null || command.initialVersion() == null)
            throw EafException.invalid("Capability 创建请求不完整。");
        var access = workspaces.require(command.actor(), command.workspaceId(), "capability:write");
        var name = requireText(command.name(), 120, "name");
        if (!name.matches("[a-z0-9]+(?:[.-][a-z0-9]+)*")) throw EafException.invalid("name 只能包含小写字母、数字、点和连字符。");
        var description = requireText(command.description(), 500, "description");
        var version = validateVersion(command.actor(), access.workspaceId(), command.initialVersion());
        var capabilityId = UUID.randomUUID();
        try {
            jdbc.update("insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description) values (?, ?, ?, ?, ?, ?)",
                    capabilityId, access.tenantId(), access.workspaceId(), command.actor().actorId(), name, description);
            insertVersion(capabilityId, access.tenantId(), access.workspaceId(), command.initialVersion(), version);
        } catch (DuplicateKeyException duplicate) {
            throw EafException.conflict("VERSION_CONFLICT", "Capability 名称或版本已存在。");
        }
        return load(access.tenantId(), access.workspaceId(), capabilityId, command.initialVersion().version());
    }

    @Override
    @Transactional
    public CapabilityDefinition addVersion(ActorContext actor, UUID workspaceId, UUID capabilityId,
                                           CreateCapabilityVersionCommand command) {
        var access = workspaces.require(actor, workspaceId, "capability:write");
        var ownerId = ownerId(access.tenantId(), workspaceId, capabilityId);
        if (ownerId == null) throw EafException.notFound();
        if (!ownerId.equals(actor.actorId())) throw EafException.forbidden("只有 Capability Owner 可以创建新版本。");
        var version = validateVersion(actor, workspaceId, command);
        try {
            insertVersion(capabilityId, access.tenantId(), workspaceId, command, version);
        } catch (DuplicateKeyException duplicate) {
            throw EafException.conflict("VERSION_CONFLICT", "Capability 版本已存在。");
        }
        return load(access.tenantId(), workspaceId, capabilityId, command.version());
    }

    @Override
    public List<CapabilityDefinition> list(ActorContext actor, UUID workspaceId) {
        var access = workspaces.require(actor, workspaceId, "capability:read");
        var versions = jdbc.query("select capability_id, asset_version from capability.version where tenant_id = ? and workspace_id = ? and status = 'PUBLISHED' order by capability_id, asset_version",
                (rs, row) -> new CapabilityKey(rs.getObject("capability_id", UUID.class), rs.getString("asset_version")),
                access.tenantId(), workspaceId);
        return versions.stream().map(key -> load(access.tenantId(), workspaceId, key.id(), key.version())).toList();
    }

    @Override
    public CapabilityDefinition get(ActorContext actor, UUID workspaceId, UUID capabilityId, String version) {
        var access = workspaces.require(actor, workspaceId, "capability:read");
        var capability = load(access.tenantId(), workspaceId, capabilityId, version);
        if (!"PUBLISHED".equals(capability.status()) && !capability.ownerId().equals(actor.actorId())) throw EafException.notFound();
        return capability;
    }

    @Override
    public CapabilityDefinition requirePublished(ActorContext actor, UUID workspaceId, UUID capabilityId, String version) {
        var capability = get(actor, workspaceId, capabilityId, version);
        if (!"PUBLISHED".equals(capability.status())) throw EafException.notFound();
        var skill = validateResolvedDependencies(actor, workspaceId, capability);
        return withSkillHash(capability, skill.contentHash());
    }

    @Override
    public java.util.Optional<McpCapabilityReference> resolveMcpReadonlyCapability(UUID tenantId, UUID ownerId,
            UUID delegateId, UUID workspaceId, UUID capabilityId, String capabilityVersion) {
        if (!MCP_SERVICE_REQUEST_CAPABILITY_ID.equals(capabilityId) || !"1.0.0".equals(capabilityVersion))
            return java.util.Optional.empty();
        try {
            var owner = new ActorContext(ownerId, tenantId, io.eaf.shared.ActorType.HUMAN,
                    workspaces.actions(tenantId, ownerId, workspaceId));
            var delegate = new ActorContext(delegateId, tenantId, io.eaf.shared.ActorType.AGENT,
                    workspaces.actions(tenantId, delegateId, workspaceId));
            var ownerCapability = requirePublished(owner, workspaceId, capabilityId, capabilityVersion);
            var agentCapability = requirePublished(delegate, workspaceId, capabilityId, capabilityVersion);
            if (!"PUBLISHED".equals(ownerCapability.status()) || !"PUBLISHED".equals(agentCapability.status())
                    || !MCP_SERVICE_REQUEST_AGENT_ID.equals(ownerCapability.agentId())
                    || !MCP_SERVICE_REQUEST_AGENT_ID.equals(agentCapability.agentId())
                    || !ownerCapability.contentHash().equals(agentCapability.contentHash())
                    || !java.util.Objects.equals(ownerCapability.skillContentHash(), agentCapability.skillContentHash())
                    || !ownerCapability.toolDependencies().isEmpty()
                    || !agents.tools(tenantId, workspaceId, MCP_SERVICE_REQUEST_AGENT_ID,
                            ownerCapability.agentVersion()).isEmpty()) return java.util.Optional.empty();
            return java.util.Optional.of(new McpCapabilityReference(capabilityId, capabilityVersion,
                    ownerCapability.contentHash()));
        } catch (RuntimeException unavailable) {
            return java.util.Optional.empty();
        }
    }

    @Override
    @Transactional
    public CapabilityDefinition publish(ActorContext actor, UUID workspaceId, UUID capabilityId, String version, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "capability:publish");
        var capability = load(access.tenantId(), workspaceId, capabilityId, version);
        if (!capability.ownerId().equals(actor.actorId())) throw EafException.forbidden("只有 Capability Owner 可以发布此版本。");
        if (!"DRAFT".equals(capability.status()) || capability.rowVersion() != expectedVersion)
            throw EafException.conflict("VERSION_CONFLICT", "Capability 状态或版本已变化。");
        validateResolvedDependencies(actor, workspaceId, capability);
        var changed = jdbc.update("update capability.version set status = 'PUBLISHED', row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and capability_id = ? and asset_version = ? and status = 'DRAFT' and row_version = ?",
                access.tenantId(), workspaceId, capabilityId, version, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "Capability 状态或版本已变化。");
        jdbc.update("insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id) values (?, ?, ?, ?, ?, 'PUBLISHED', ?)",
                UUID.randomUUID(), access.tenantId(), workspaceId, capabilityId, version, actor.actorId());
        return requirePublished(actor, workspaceId, capabilityId, version);
    }

    @Override
    @Transactional
    public CapabilityDefinition revoke(ActorContext actor, UUID workspaceId, UUID capabilityId, String version, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "capability:publish");
        var ownerId = ownerId(access.tenantId(), workspaceId, capabilityId);
        if (ownerId == null) throw EafException.notFound();
        if (!ownerId.equals(actor.actorId())) throw EafException.forbidden("只有 Capability Owner 可以撤回此版本。");
        var changed = jdbc.update("update capability.version set status = 'REVOKED', row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and capability_id = ? and asset_version = ? and status in ('PUBLISHED', 'DEPRECATED') and row_version = ?",
                access.tenantId(), workspaceId, capabilityId, version, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "只有当前版本的已发布 Capability 可以撤回。");
        jdbc.update("insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id) values (?, ?, ?, ?, ?, 'REVOKED', ?)",
                UUID.randomUUID(), access.tenantId(), workspaceId, capabilityId, version, actor.actorId());
        return load(access.tenantId(), workspaceId, capabilityId, version);
    }

    @Override
    @Transactional
    public PromptVariant publishPromptAnalysisVariant(ActorContext actor, UUID workspaceId, UUID candidateId,
            int revision, UUID adoptionId, UUID agentId, String agentVersion, UUID skillId, String skillVersion,
            UUID promptId, String promptVersion, String promptHash, UUID approvalId, UUID reportId, String reportHash) {
        requirePromptOwner(actor, workspaceId, "capability:write");
        workspaces.require(actor, workspaceId, "capability:publish");
        if (candidateId == null || revision < 1 || adoptionId == null || agentId == null || skillId == null
                || promptId == null || !"1.0.0".equals(agentVersion) || !"1.0.0".equals(skillVersion)
                || !"1.0.0".equals(promptVersion) || !hexHash(promptHash) || approvalId == null || reportId == null
                || !hexHash(reportHash)) throw EafException.invalid("Capability 派生来源必须绑定精确 Agent、Skill、Prompt、批准和报告。");
        var prior = findPromptVariant(actor, workspaceId, candidateId, revision, adoptionId);
        if (prior != null) {
            if (!prior.agentId().equals(agentId) || !prior.skillId().equals(skillId)
                    || !prior.promptId().equals(promptId) || !prior.promptVersion().equals(promptVersion)
                    || !prior.promptHash().equals(promptHash) || !prior.approvalId().equals(approvalId)
                    || !prior.reportId().equals(reportId) || !prior.reportHash().equals(reportHash))
                throw EafException.conflict("PROMPT_VARIANT_CONFLICT", "Capability 派生来源已绑定其他资产。");
            return prior;
        }
        var ownedAgent = agentOwners.findPromptAnalysisVariant(actor, workspaceId, candidateId, revision, adoptionId)
                .orElseThrow(EafException::notFound);
        var ownedSkill = skills.findPromptAnalysisVariant(actor, workspaceId, candidateId, revision, adoptionId)
                .orElseThrow(EafException::notFound);
        if (!ownedAgent.id().equals(agentId) || !ownedAgent.version().equals(agentVersion)
                || !ownedSkill.id().equals(skillId) || !ownedSkill.version().equals(skillVersion)
                || !ownedAgent.promptId().equals(promptId) || !ownedSkill.promptId().equals(promptId)
                || !ownedAgent.promptVersion().equals(promptVersion) || !ownedSkill.promptVersion().equals(promptVersion)
                || !"PUBLISHED".equals(ownedAgent.status())
                || !"PUBLISHED".equals(ownedSkill.status()))
            throw EafException.conflict("PROMPT_VARIANT_DEPENDENCY_CONFLICT", "Capability 只能绑定同一采用记录中的已发布 Agent 和 Skill。");
        var base = requirePublished(actor, workspaceId, MCP_SERVICE_REQUEST_CAPABILITY_ID, "1.0.0");
        if (!base.agentId().equals(MCP_SERVICE_REQUEST_AGENT_ID) || !base.toolDependencies().isEmpty()
                || !"p15-service-request-plan-v1".equals(base.evaluationRef()))
            throw EafException.conflict("PROMPT_VARIANT_BASE_UNSAFE", "P15 固定基线 Capability 必须是只读分析合同。");
        var publishedAgent = agents.requirePublished(actor.tenantId(), workspaceId, agentId, agentVersion);
        var publishedSkill = skills.requirePublished(actor, workspaceId, skillId, skillVersion);
        if (!publishedAgent.promptId().equals(promptId) || !publishedSkill.promptId().equals(promptId))
            throw EafException.conflict("PROMPT_VARIANT_DEPENDENCY_CONFLICT", "派生 Agent 与 Skill 必须绑定指定 Prompt。");
        prompts.requirePublished(actor.tenantId(), workspaceId, promptId, promptVersion);
        var id = UUID.randomUUID();
        var version = "1.0.0";
        var name = "service-request-plan-p31-" + id.toString().substring(0, 8);
        jdbc.update("insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description) "
                        + "values (?, ?, ?, ?, ?, ?)", id, actor.tenantId(), workspaceId, actor.actorId(), name,
                "P31 显式采用的只读 P15 分析 Capability。");
        jdbc.update("insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version, "
                        + "skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PUBLISHED')",
                id, actor.tenantId(), workspaceId, version, agentId, agentVersion, skillId, skillVersion, promptId,
                promptVersion, base.evaluationRef());
        jdbc.update("insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id) "
                        + "values (?, ?, ?, ?, ?, 'PUBLISHED', ?)", UUID.randomUUID(), actor.tenantId(), workspaceId, id, version, actor.actorId());
        jdbc.update("insert into capability.prompt_variant_origin(tenant_id, workspace_id, candidate_id, candidate_revision, adoption_id, "
                        + "owner_id, capability_id, capability_version, base_capability_id, base_capability_version, agent_id, agent_version, "
                        + "skill_id, skill_version, prompt_id, prompt_version, prompt_hash, approval_id, report_id, report_hash, status) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PUBLISHED')",
                actor.tenantId(), workspaceId, candidateId, revision, adoptionId, actor.actorId(), id, version, base.id(), base.version(),
                agentId, agentVersion, skillId, skillVersion, promptId, promptVersion, promptHash, approvalId, reportId, reportHash);
        return findPromptVariant(actor, workspaceId, candidateId, revision, adoptionId);
    }

    @Override
    @Transactional(readOnly = true)
    public java.util.Optional<PromptVariant> findPromptAnalysisVariant(ActorContext actor, UUID workspaceId,
            UUID candidateId, int revision, UUID adoptionId) {
        requirePromptOwner(actor, workspaceId, "capability:read");
        return java.util.Optional.ofNullable(findPromptVariant(actor, workspaceId, candidateId, revision, adoptionId));
    }

    @Override
    @Transactional
    public PromptVariant revokePromptAnalysisVariant(ActorContext actor, UUID workspaceId,
            UUID candidateId, int revision, UUID adoptionId) {
        requirePromptOwner(actor, workspaceId, "capability:publish");
        var variant = findPromptVariant(actor, workspaceId, candidateId, revision, adoptionId);
        if (variant == null) throw EafException.notFound();
        jdbc.update("update capability.version set status = 'REVOKED', row_version = row_version + 1 where tenant_id = ? "
                        + "and workspace_id = ? and capability_id = ? and asset_version = ? and status = 'PUBLISHED'",
                actor.tenantId(), workspaceId, variant.id(), variant.version());
        jdbc.update("update capability.prompt_variant_origin set status = 'REVOKED' where tenant_id = ? and workspace_id = ? "
                        + "and candidate_id = ? and candidate_revision = ? and adoption_id = ? and status = 'PUBLISHED'",
                actor.tenantId(), workspaceId, candidateId, revision, adoptionId);
        jdbc.update("insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id) "
                        + "values (?, ?, ?, ?, ?, 'REVOKED', ?) on conflict do nothing",
                UUID.randomUUID(), actor.tenantId(), workspaceId, variant.id(), variant.version(), actor.actorId());
        return findPromptVariant(actor, workspaceId, candidateId, revision, adoptionId);
    }

    @Override
    @Transactional(readOnly = true)
    public boolean isPromptAnalysisVariant(ActorContext actor, UUID workspaceId, UUID capabilityId, String version) {
        workspaces.require(actor, workspaceId, "capability:read");
        var found = jdbc.query("select candidate_id, candidate_revision, adoption_id from capability.prompt_variant_origin "
                        + "where tenant_id = ? and workspace_id = ? and capability_id = ? and capability_version = ? and status = 'PUBLISHED'",
                rs -> rs.next() ? new AdoptionKey(rs.getObject("candidate_id", UUID.class), rs.getInt("candidate_revision"),
                        rs.getObject("adoption_id", UUID.class)) : null,
                actor.tenantId(), workspaceId, capabilityId, version);
        if (found == null) return false;
        var capability = requirePublished(actor, workspaceId, capabilityId, version);
        var agent = agents.requirePublished(actor.tenantId(), workspaceId, capability.agentId(), capability.agentVersion());
        return "p15-service-request-plan-v1".equals(capability.evaluationRef())
                && capability.toolDependencies().isEmpty() && agent.promptId().equals(capability.promptId())
                && agent.promptVersion().equals(capability.promptVersion())
                && "SERVICE_REQUEST_PLAN_V1".equals(agent.responseProfile()) && agent.ragEnabled()
                && "HYBRID".equals(agent.retrievalMode()) && "NONE".equals(agent.evidencePolicy())
                && agents.tools(actor.tenantId(), workspaceId, capability.agentId(), capability.agentVersion()).isEmpty();
    }

    private PromptVariant findPromptVariant(ActorContext actor, UUID workspaceId, UUID candidateId, int revision, UUID adoptionId) {
        return jdbc.query("select o.capability_id, o.capability_version, o.base_capability_id, o.base_capability_version, "
                        + "o.agent_id, o.agent_version, o.skill_id, o.skill_version, o.prompt_id, o.prompt_version, o.status "
                        + ", o.prompt_hash, o.approval_id, o.report_id, o.report_hash "
                        + "from capability.prompt_variant_origin o where o.tenant_id = ? and o.workspace_id = ? and o.owner_id = ? "
                        + "and o.candidate_id = ? and o.candidate_revision = ? and o.adoption_id = ?",
                rs -> rs.next() ? new PromptVariant(rs.getObject("capability_id", UUID.class), rs.getString("capability_version"),
                        rs.getObject("base_capability_id", UUID.class), rs.getString("base_capability_version"),
                        rs.getObject("agent_id", UUID.class), rs.getString("agent_version"), rs.getObject("skill_id", UUID.class),
                        rs.getString("skill_version"), rs.getObject("prompt_id", UUID.class), rs.getString("prompt_version"),
                        rs.getString("prompt_hash"), rs.getObject("approval_id", UUID.class), rs.getObject("report_id", UUID.class),
                        rs.getString("report_hash"),
                        load(actor.tenantId(), workspaceId, rs.getObject("capability_id", UUID.class),
                                rs.getString("capability_version")).contentHash(), rs.getString("status")) : null,
                actor.tenantId(), workspaceId, actor.actorId(), candidateId, revision, adoptionId);
    }

    private void requirePromptOwner(ActorContext actor, UUID workspaceId, String permission) {
        if (actor == null || actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("P31 Capability 变体只允许直接 HUMAN Owner 操作。");
        workspaces.require(actor, workspaceId, "capability:read");
        workspaces.require(actor, workspaceId, permission);
    }

    private boolean hexHash(String value) { return value != null && value.matches("[0-9a-f]{64}"); }

    private record AdoptionKey(UUID candidateId, int revision, UUID adoptionId) { }

    private ValidatedVersion validateVersion(ActorContext actor, UUID workspaceId, CreateCapabilityVersionCommand command) {
        if (command == null) throw EafException.invalid("Capability 版本不能为空。");
        var version = requireText(command.version(), 40, "version");
        if (!version.equals(command.version())) throw EafException.invalid("version 不能包含首尾空格。");
        var agentVersion = requireText(command.agentVersion(), 40, "agentVersion");
        var skillVersion = requireText(command.skillVersion(), 40, "skillVersion");
        var promptVersion = requireText(command.promptVersion(), 40, "promptVersion");
        var evaluationRef = requireText(command.evaluationRef(), 120, "evaluationRef");
        if (command.agentId() == null || command.skillId() == null || command.promptId() == null)
            throw EafException.invalid("Agent、Skill 和 Prompt 引用不能为空。");

        workspaces.require(actor, workspaceId, "agent:read");
        var agent = agents.requirePublished(actor.tenantId(), workspaceId, command.agentId(), agentVersion);
        var skill = skills.requirePublished(actor, workspaceId, command.skillId(), skillVersion);
        if (!agent.promptId().equals(command.promptId()) || !agent.promptVersion().equals(promptVersion)
                || !skill.promptId().equals(command.promptId()) || !skill.promptVersion().equals(promptVersion))
            throw EafException.conflict("CAPABILITY_PROMPT_CONFLICT", "Capability、Agent 与 Skill 必须固定到同一个已发布 Prompt 版本。");
        prompts.requirePublished(actor.tenantId(), workspaceId, command.promptId(), promptVersion);

        var references = validateToolReferences(actor, workspaceId, command.toolDependencies());
        validateAgentAndSkillToolSubset(actor.tenantId(), workspaceId, agent.id(), agent.version(), skill, references);
        return new ValidatedVersion(agent.id(), agent.version(), skill.id(), skill.version(), command.promptId(), promptVersion,
                evaluationRef, List.copyOf(references));
    }

    private List<StoredToolDependency> validateToolReferences(ActorContext actor, UUID workspaceId, List<CapabilityToolReference> references) {
        if (references == null || references.isEmpty() || references.size() > MAX_REFERENCE_COUNT)
            throw EafException.invalid("toolDependencies 必须包含 1 到 16 个已发布 Tool。");
        workspaces.require(actor, workspaceId, "tool:read");
        var names = new HashSet<String>();
        var dependencies = new ArrayList<StoredToolDependency>();
        for (var reference : references) {
            if (reference == null) throw EafException.invalid("Tool 依赖不能为空。");
            var name = requireText(reference.name(), 120, "tool name");
            var version = requireText(reference.version(), 40, "tool version");
            if (!names.add(name)) throw EafException.conflict("CAPABILITY_TOOL_CONFLICT", "同一 Capability 不能绑定同名 Tool 的多个版本。");
            ToolDefinition tool = tools.requirePublished(actor.tenantId(), workspaceId, name, version);
            dependencies.add(new StoredToolDependency(name, version, canonical(parseSchema(tool.inputSchema())),
                    canonical(parseSchema(tool.outputSchema()))));
        }
        dependencies.sort(Comparator.comparing(StoredToolDependency::name));
        return dependencies;
    }

    private void validateAgentAndSkillToolSubset(UUID tenantId, UUID workspaceId, UUID agentId, String agentVersion,
                                                 SkillDefinition skill, List<StoredToolDependency> dependencies) {
        var available = agents.tools(tenantId, workspaceId, agentId, agentVersion).stream()
                .map(binding -> binding.name() + "@" + binding.version()).collect(java.util.stream.Collectors.toSet());
        for (var dependency : dependencies) {
            if (!available.contains(dependency.name() + "@" + dependency.version()))
                throw EafException.conflict("CAPABILITY_TOOL_CONFLICT", "Capability Tool 必须已在固定 Agent 允许列表内。");
        }
        var selected = dependencies.stream().map(dependency -> dependency.name() + "@" + dependency.version())
                .collect(java.util.stream.Collectors.toSet());
        for (var dependency : skill.toolDependencies()) {
            if (!selected.contains(dependency.name() + "@" + dependency.version()))
                throw EafException.conflict("CAPABILITY_TOOL_CONFLICT", "Capability 必须包含 Skill 的全部固定 Tool 依赖。");
        }
    }

    private SkillDefinition validateResolvedDependencies(ActorContext actor, UUID workspaceId, CapabilityDefinition capability) {
        workspaces.require(actor, workspaceId, "agent:read");
        var agent = agents.requirePublished(actor.tenantId(), workspaceId, capability.agentId(), capability.agentVersion());
        var skill = skills.requirePublished(actor, workspaceId, capability.skillId(), capability.skillVersion());
        if (!agent.promptId().equals(capability.promptId()) || !agent.promptVersion().equals(capability.promptVersion())
                || !skill.promptId().equals(capability.promptId()) || !skill.promptVersion().equals(capability.promptVersion()))
            throw EafException.conflict("CAPABILITY_PROMPT_CONFLICT", "固定 Agent、Skill 与 Prompt 依赖已不一致。");
        prompts.requirePublished(actor.tenantId(), workspaceId, capability.promptId(), capability.promptVersion());
        workspaces.require(actor, workspaceId, "tool:read");
        var dependencies = loadStoredDependencies(actor.tenantId(), workspaceId, capability.id(), capability.version());
        validateAgentAndSkillToolSubset(actor.tenantId(), workspaceId, agent.id(), agent.version(), skill, dependencies);
        for (var dependency : dependencies) {
            ToolDefinition current = tools.requirePublished(actor.tenantId(), workspaceId, dependency.name(), dependency.version());
            if (!parseSchema(dependency.inputSchema()).equals(parseSchema(current.inputSchema()))
                    || !parseSchema(dependency.outputSchema()).equals(parseSchema(current.outputSchema())))
                throw EafException.conflict("CAPABILITY_TOOL_SCHEMA_CONFLICT", "固定 Tool Schema 与 Capability 快照不一致。");
        }
        return skill;
    }

    private void insertVersion(UUID capabilityId, UUID tenantId, UUID workspaceId,
                               CreateCapabilityVersionCommand command, ValidatedVersion version) {
        jdbc.update("insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version, skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'DRAFT')",
                capabilityId, tenantId, workspaceId, command.version(), version.agentId(), version.agentVersion(),
                version.skillId(), version.skillVersion(), version.promptId(), version.promptVersion(), version.evaluationRef());
        for (var dependency : version.toolDependencies()) {
            jdbc.update("insert into capability.tool_dependency(tenant_id, workspace_id, capability_id, capability_version, tool_name, tool_version, input_schema, output_schema) "
                            + "values (?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)",
                    tenantId, workspaceId, capabilityId, command.version(), dependency.name(), dependency.version(),
                    dependency.inputSchema(), dependency.outputSchema());
        }
    }

    private CapabilityDefinition load(UUID tenantId, UUID workspaceId, UUID capabilityId, String version) {
        var row = jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.name, d.description, "
                        + "v.asset_version, v.agent_id, v.agent_version, v.skill_id, v.skill_version, v.prompt_id, v.prompt_version, v.evaluation_ref, v.status, v.row_version "
                        + "from capability.definition d join capability.version v on v.capability_id = d.id and v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id "
                        + "where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and v.asset_version = ?",
                rs -> rs.next() ? new CapabilityRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("name"),
                        rs.getString("description"), rs.getString("asset_version"), rs.getObject("agent_id", UUID.class),
                        rs.getString("agent_version"), rs.getObject("skill_id", UUID.class), rs.getString("skill_version"),
                        rs.getObject("prompt_id", UUID.class), rs.getString("prompt_version"), rs.getString("evaluation_ref"),
                        rs.getString("status"), rs.getLong("row_version")) : null,
                tenantId, workspaceId, capabilityId, version);
        if (row == null) throw EafException.notFound();
        var dependencies = loadStoredDependencies(tenantId, workspaceId, capabilityId, version).stream()
                .map(dependency -> new CapabilityToolDependency(dependency.name(), dependency.version(),
                        Hashing.sha256(canonical(parseSchema(dependency.inputSchema()))),
                        Hashing.sha256(canonical(parseSchema(dependency.outputSchema()))))).toList();
        var contentHash = contentHash(row, dependencies);
        return new CapabilityDefinition(row.id(), row.tenantId(), row.workspaceId(), row.ownerId(), row.name(), row.description(),
                row.version(), row.agentId(), row.agentVersion(), row.skillId(), row.skillVersion(), null,
                row.promptId(), row.promptVersion(), dependencies, row.evaluationRef(), row.status(), row.rowVersion(), contentHash);
    }

    private CapabilityDefinition withSkillHash(CapabilityDefinition capability, String skillHash) {
        return new CapabilityDefinition(capability.id(), capability.tenantId(), capability.workspaceId(), capability.ownerId(),
                capability.name(), capability.description(), capability.version(), capability.agentId(), capability.agentVersion(),
                capability.skillId(), capability.skillVersion(), skillHash, capability.promptId(), capability.promptVersion(),
                capability.toolDependencies(), capability.evaluationRef(), capability.status(), capability.rowVersion(), capability.contentHash());
    }

    private List<StoredToolDependency> loadStoredDependencies(UUID tenantId, UUID workspaceId, UUID capabilityId, String version) {
        return jdbc.query("select tool_name, tool_version, input_schema::text input_schema, output_schema::text output_schema "
                        + "from capability.tool_dependency where tenant_id = ? and workspace_id = ? and capability_id = ? and capability_version = ? order by tool_name",
                (rs, row) -> new StoredToolDependency(rs.getString("tool_name"), rs.getString("tool_version"),
                        rs.getString("input_schema"), rs.getString("output_schema")), tenantId, workspaceId, capabilityId, version);
    }

    private UUID ownerId(UUID tenantId, UUID workspaceId, UUID capabilityId) {
        return jdbc.query("select owner_id from capability.definition where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getObject("owner_id", UUID.class) : null, capabilityId, tenantId, workspaceId);
    }

    private JsonNode parseSchema(String source) {
        try {
            var schema = json.readTree(source);
            if (schema == null || !schema.isObject() || !"object".equals(schema.path("type").asText()))
                throw new IllegalStateException("Tool Schema 不是 JSON object。");
            return schema;
        } catch (JsonProcessingException e) { throw new IllegalStateException("Tool Schema 无法解析。", e); }
    }

    // ponytail: Skill 与 Agent 版本不可变，引用 ID/版本即可固定其内容；若引入可变资产，需在 Manifest 中保存独立内容摘要。
    private String contentHash(CapabilityRow row, List<CapabilityToolDependency> dependencies) {
        try {
            return Hashing.sha256(json.writeValueAsString(new CapabilityContent(row.name(), row.description(), row.version(),
                    row.agentId(), row.agentVersion(), row.skillId(), row.skillVersion(), row.promptId(), row.promptVersion(),
                    dependencies, row.evaluationRef())));
        } catch (JsonProcessingException e) { throw new IllegalStateException("Capability 内容摘要无法生成。", e); }
    }

    private String canonical(JsonNode node) {
        try { return json.writeValueAsString(node); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Schema 无法序列化。", e); }
    }

    private String requireText(String value, int maxLength, String field) {
        if (value == null || value.isBlank() || value.length() > maxLength) throw EafException.invalid(field + " 缺失或超过长度限制。");
        return value.trim();
    }

    private record StoredToolDependency(String name, String version, String inputSchema, String outputSchema) { }
    private record ValidatedVersion(UUID agentId, String agentVersion, UUID skillId, String skillVersion,
                                    UUID promptId, String promptVersion, String evaluationRef,
                                    List<StoredToolDependency> toolDependencies) { }
    private record CapabilityKey(UUID id, String version) { }
    private record CapabilityRow(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String name, String description,
                                 String version, UUID agentId, String agentVersion, UUID skillId, String skillVersion,
                                 UUID promptId, String promptVersion, String evaluationRef, String status, long rowVersion) { }
    private record CapabilityContent(String name, String description, String version, UUID agentId, String agentVersion,
                                     UUID skillId, String skillVersion, UUID promptId, String promptVersion,
                                     List<CapabilityToolDependency> dependencies, String evaluationRef) { }
}
// 本文件负责实现 JdbcCapabilityService.java 相关代码。
