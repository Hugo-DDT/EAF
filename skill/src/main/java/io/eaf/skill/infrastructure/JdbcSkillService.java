package io.eaf.skill.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.prompt.api.PromptCatalog;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.skill.api.CreateSkillCommand;
import io.eaf.skill.api.CreateSkillVersionCommand;
import io.eaf.skill.api.SkillDefinition;
import io.eaf.skill.api.SkillService;
import io.eaf.skill.api.SkillToolDependency;
import io.eaf.skill.api.SkillToolReference;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.tool.api.ToolDefinition;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcSkillService implements SkillService {
    private static final int MAX_SCHEMA_LENGTH = 16_384;
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final PromptCatalog prompts;
    private final ToolCatalog tools;
    private final ObjectMapper json;

    public JdbcSkillService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces, PromptCatalog prompts,
                            ToolCatalog tools, ObjectMapper json) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.prompts = prompts;
        this.tools = tools;
        this.json = json;
    }

    @Override
    @Transactional
    public SkillDefinition create(CreateSkillCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null || command.initialVersion() == null)
            throw EafException.invalid("Skill 创建请求不完整。");
        var access = workspaces.require(command.actor(), command.workspaceId(), "skill:write");
        var name = requireText(command.name(), 120, "name");
        if (!name.matches("[a-z0-9]+(?:[.-][a-z0-9]+)*")) throw EafException.invalid("name 只能包含小写字母、数字、点和连字符。");
        var description = requireText(command.description(), 500, "description");
        var version = validateVersion(command.actor(), access.workspaceId(), command.initialVersion());
        var skillId = UUID.randomUUID();
        try {
            jdbc.update("insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description) values (?, ?, ?, ?, ?, ?)",
                    skillId, access.tenantId(), access.workspaceId(), command.actor().actorId(), name, description);
            insertVersion(skillId, access.tenantId(), access.workspaceId(), command.initialVersion(), version);
        } catch (DuplicateKeyException e) {
            throw EafException.conflict("VERSION_CONFLICT", "Skill 名称或版本已存在。");
        }
        return load(command.actor().tenantId(), access.workspaceId(), skillId, command.initialVersion().version());
    }

    @Override
    @Transactional
    public SkillDefinition addVersion(ActorContext actor, UUID workspaceId, UUID skillId, CreateSkillVersionCommand command) {
        var access = workspaces.require(actor, workspaceId, "skill:write");
        var ownerId = jdbc.query("select owner_id from skill.definition where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getObject("owner_id", UUID.class) : null, skillId, access.tenantId(), workspaceId);
        if (ownerId == null) throw EafException.notFound();
        if (!ownerId.equals(actor.actorId())) throw EafException.forbidden("只有 Skill Owner 可以创建新版本。");
        var version = validateVersion(actor, workspaceId, command);
        try {
            insertVersion(skillId, access.tenantId(), workspaceId, command, version);
        } catch (DuplicateKeyException e) {
            throw EafException.conflict("VERSION_CONFLICT", "Skill 版本已存在。");
        }
        return load(access.tenantId(), workspaceId, skillId, command.version());
    }

    @Override
    public List<SkillDefinition> list(ActorContext actor, UUID workspaceId) {
        var access = workspaces.require(actor, workspaceId, "skill:read");
        var versions = jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.name, d.description, v.asset_version "
                        + "from skill.definition d join skill.version v on v.skill_id = d.id and v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id "
                        + "where d.tenant_id = ? and d.workspace_id = ? and v.status = 'PUBLISHED' order by d.name, v.asset_version",
                (rs, row) -> new SkillKey(rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                        rs.getObject("id", UUID.class), rs.getString("asset_version")), access.tenantId(), workspaceId);
        return versions.stream().map(key -> load(key.tenantId(), key.workspaceId(), key.skillId(), key.version())).toList();
    }

    @Override
    public SkillDefinition get(ActorContext actor, UUID workspaceId, UUID skillId, String version) {
        var access = workspaces.require(actor, workspaceId, "skill:read");
        var skill = load(access.tenantId(), workspaceId, skillId, version);
        if (!"PUBLISHED".equals(skill.status()) && !skill.ownerId().equals(actor.actorId())) throw EafException.notFound();
        return skill;
    }

    @Override
    public SkillDefinition requirePublished(ActorContext actor, UUID workspaceId, UUID skillId, String version) {
        var skill = get(actor, workspaceId, skillId, version);
        if (!"PUBLISHED".equals(skill.status())) throw EafException.notFound();
        validateResolvedDependencies(actor, workspaceId, skill);
        return skill;
    }

    @Override
    @Transactional
    public SkillDefinition publish(ActorContext actor, UUID workspaceId, UUID skillId, String version, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "skill:publish");
        var skill = load(access.tenantId(), workspaceId, skillId, version);
        if (!skill.ownerId().equals(actor.actorId())) throw EafException.forbidden("只有 Skill Owner 可以发布此版本。");
        if (!"DRAFT".equals(skill.status()) || skill.rowVersion() != expectedVersion)
            throw EafException.conflict("VERSION_CONFLICT", "Skill 状态或版本已变化。");
        validateResolvedDependencies(actor, workspaceId, skill);
        var changed = jdbc.update("update skill.version set status = 'PUBLISHED', row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and skill_id = ? and asset_version = ? and status = 'DRAFT' and row_version = ?",
                access.tenantId(), workspaceId, skillId, version, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "Skill 状态或版本已变化。");
        jdbc.update("insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id) values (?, ?, ?, ?, ?, 'PUBLISHED', ?)",
                UUID.randomUUID(), access.tenantId(), workspaceId, skillId, version, actor.actorId());
        return load(access.tenantId(), workspaceId, skillId, version);
    }

    @Override
    @Transactional
    public SkillDefinition revoke(ActorContext actor, UUID workspaceId, UUID skillId, String version, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "skill:publish");
        var ownerId = jdbc.query("select owner_id from skill.definition where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getObject("owner_id", UUID.class) : null, skillId, access.tenantId(), workspaceId);
        if (ownerId == null) throw EafException.notFound();
        if (!ownerId.equals(actor.actorId())) throw EafException.forbidden("只有 Skill Owner 可以撤回此版本。");
        var changed = jdbc.update("update skill.version set status = 'REVOKED', row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and skill_id = ? and asset_version = ? and status in ('PUBLISHED', 'DEPRECATED') and row_version = ?",
                access.tenantId(), workspaceId, skillId, version, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "只有当前版本的已发布 Skill 可以撤回。");
        jdbc.update("insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id) values (?, ?, ?, ?, ?, 'REVOKED', ?)",
                UUID.randomUUID(), access.tenantId(), workspaceId, skillId, version, actor.actorId());
        return load(access.tenantId(), workspaceId, skillId, version);
    }

    private ValidatedVersion validateVersion(ActorContext actor, UUID workspaceId, CreateSkillVersionCommand command) {
        if (command == null) throw EafException.invalid("Skill 版本不能为空。");
        var versionName = requireText(command.version(), 40, "version");
        var evaluationRef = requireText(command.evaluationRef(), 120, "evaluationRef");
        if (!versionName.equals(command.version())) throw EafException.invalid("version 不能包含首尾空格。");
        var input = parseSchema(command.inputSchema(), "inputSchema");
        var output = parseSchema(command.outputSchema(), "outputSchema");
        if (command.promptId() == null) throw EafException.invalid("promptId 不能为空。");
        workspaces.require(actor, workspaceId, "agent:read");
        var promptVersion = requireText(command.promptVersion(), 40, "promptVersion");
        if (!promptVersion.equals(command.promptVersion())) throw EafException.invalid("promptVersion 不能包含首尾空格。");
        var prompt = prompts.requirePublished(actor.tenantId(), workspaceId, command.promptId(), promptVersion);
        if (!prompt.userTemplate().contains("{{input}}"))
            throw EafException.conflict("SKILL_PROMPT_SCHEMA_CONFLICT", "Prompt 必须使用固定的 {{input}} 输入变量。");

        if (command.toolDependencies() == null || command.toolDependencies().isEmpty() || command.toolDependencies().size() > 16)
            throw EafException.invalid("toolDependencies 必须包含 1 到 16 个已发布 Tool。");
        workspaces.require(actor, workspaceId, "tool:read");
        var seenNames = new java.util.HashSet<String>();
        var dependencies = new ArrayList<StoredToolDependency>();
        for (var reference : command.toolDependencies()) {
            if (reference == null) throw EafException.invalid("Tool 依赖不能为空。");
            var name = requireText(reference.name(), 120, "tool name");
            var toolVersion = requireText(reference.version(), 40, "tool version");
            if (!seenNames.add(name)) throw EafException.conflict("SKILL_TOOL_SCHEMA_CONFLICT", "同一 Skill 不能绑定同名 Tool 的多个版本。");
            var tool = tools.requirePublished(actor.tenantId(), workspaceId, name, toolVersion);
            var toolInput = parseToolSchema(tool.inputSchema());
            var toolOutput = parseToolSchema(tool.outputSchema());
            dependencies.add(new StoredToolDependency(name, toolVersion, canonical(toolInput), canonical(toolOutput)));
        }
        dependencies.sort(Comparator.comparing(StoredToolDependency::name));
        return new ValidatedVersion(canonical(input), canonical(output), command.promptId(), promptVersion,
                evaluationRef, List.copyOf(dependencies));
    }

    private void validateResolvedDependencies(ActorContext actor, UUID workspaceId, SkillDefinition skill) {
        workspaces.require(actor, workspaceId, "agent:read");
        var prompt = prompts.requirePublished(actor.tenantId(), workspaceId, skill.promptId(), skill.promptVersion());
        if (!prompt.userTemplate().contains("{{input}}"))
            throw EafException.conflict("SKILL_PROMPT_SCHEMA_CONFLICT", "Prompt 不再满足 Skill 输入约束。");
        workspaces.require(actor, workspaceId, "tool:read");
        for (var dependency : loadStoredDependencies(actor.tenantId(), workspaceId, skill.id(), skill.version())) {
            ToolDefinition current = tools.requirePublished(actor.tenantId(), workspaceId, dependency.name(), dependency.version());
            // Schema 是 JSON 对象，字段顺序不改变结构含义，因此按节点结构比较而非比较序列化文本。
            if (!parseToolSchema(dependency.inputSchema()).equals(parseToolSchema(current.inputSchema()))
                    || !parseToolSchema(dependency.outputSchema()).equals(parseToolSchema(current.outputSchema())))
                throw EafException.conflict("SKILL_TOOL_SCHEMA_CONFLICT", "固定 Tool 版本的 Schema 与 Skill 发布快照不一致。");
        }
    }

    private void insertVersion(UUID skillId, UUID tenantId, UUID workspaceId, CreateSkillVersionCommand command,
                               ValidatedVersion version) {
        jdbc.update("insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema, prompt_id, prompt_version, evaluation_ref, status) "
                        + "values (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, 'DRAFT')",
                skillId, tenantId, workspaceId, command.version(), version.inputSchema(), version.outputSchema(),
                version.promptId(), version.promptVersion(), version.evaluationRef());
        for (var dependency : version.toolDependencies()) {
            jdbc.update("insert into skill.tool_dependency(tenant_id, workspace_id, skill_id, skill_version, tool_name, tool_version, input_schema, output_schema) "
                            + "values (?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)",
                    tenantId, workspaceId, skillId, command.version(), dependency.name(), dependency.version(),
                    dependency.inputSchema(), dependency.outputSchema());
        }
    }

    private SkillDefinition load(UUID tenantId, UUID workspaceId, UUID skillId, String version) {
        var row = jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.name, d.description, "
                        + "v.asset_version, v.input_schema::text input_schema, v.output_schema::text output_schema, "
                        + "v.prompt_id, v.prompt_version, v.evaluation_ref, v.status, v.row_version "
                        + "from skill.definition d join skill.version v on v.skill_id = d.id and v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id "
                        + "where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and v.asset_version = ?",
                rs -> rs.next() ? new VersionRow(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("name"),
                        rs.getString("description"), rs.getString("asset_version"), rs.getString("input_schema"),
                        rs.getString("output_schema"), rs.getObject("prompt_id", UUID.class), rs.getString("prompt_version"),
                        rs.getString("evaluation_ref"), rs.getString("status"), rs.getLong("row_version")) : null,
                tenantId, workspaceId, skillId, version);
        if (row == null) throw EafException.notFound();
        var dependencies = loadStoredDependencies(tenantId, workspaceId, skillId, version).stream()
                .map(dependency -> new SkillToolDependency(dependency.name(), dependency.version(),
                        schemaHash(dependency.inputSchema()), schemaHash(dependency.outputSchema()))).toList();
        var contentHash = contentHash(row, dependencies);
        return new SkillDefinition(row.id(), row.tenantId(), row.workspaceId(), row.ownerId(), row.name(), row.description(),
                row.version(), row.inputSchema(), row.outputSchema(), row.promptId(), row.promptVersion(), dependencies,
                row.evaluationRef(), row.status(), row.rowVersion(), contentHash);
    }

    private List<StoredToolDependency> loadStoredDependencies(UUID tenantId, UUID workspaceId, UUID skillId, String version) {
        return jdbc.query("select tool_name, tool_version, input_schema::text input_schema, output_schema::text output_schema "
                        + "from skill.tool_dependency where tenant_id = ? and workspace_id = ? and skill_id = ? and skill_version = ? order by tool_name",
                (rs, row) -> new StoredToolDependency(rs.getString("tool_name"), rs.getString("tool_version"),
                        rs.getString("input_schema"), rs.getString("output_schema")), tenantId, workspaceId, skillId, version);
    }

    private JsonNode parseSchema(String source, String field) {
        if (source == null || source.length() > MAX_SCHEMA_LENGTH) throw EafException.invalid(field + " 缺失或超过长度限制。");
        try {
            var schema = json.readTree(source);
            if (schema == null || !schema.isObject() || !"object".equals(schema.path("type").asText())
                    || !schema.path("properties").isObject() || schema.path("properties").isEmpty())
                throw EafException.invalid(field + " 必须是包含 properties 的 JSON object Schema。");
            var required = schema.path("required");
            if (!required.isMissingNode() && !required.isArray()) throw EafException.invalid(field + ".required 必须是数组。");
            if (required.isArray()) {
                var seen = new java.util.HashSet<String>();
                for (var property : required) {
                    if (!property.isTextual() || !schema.path("properties").has(property.asText()) || !seen.add(property.asText()))
                        throw EafException.invalid(field + ".required 必须是唯一且已声明的属性。");
                }
            }
            return schema;
        } catch (JsonProcessingException e) {
            throw EafException.invalid(field + " 必须是有效 JSON Schema。");
        }
    }

    private JsonNode parseToolSchema(String source) {
        try {
            var schema = json.readTree(source);
            if (schema == null || !schema.isObject() || !"object".equals(schema.path("type").asText()))
                throw new IllegalStateException("Tool Schema 不是 JSON object。");
            return schema;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Tool Schema 无法解析。", e);
        }
    }

    // ponytail: 仅校验 object/properties/required；需要复杂 JSON Schema 语义时再扩展。
    private String canonical(JsonNode node) {
        try { return json.writeValueAsString(node); }
        catch (JsonProcessingException e) { throw new IllegalStateException("JSON Schema 无法序列化。", e); }
    }

    private String schemaHash(String schema) {
        return Hashing.sha256(canonical(parseToolSchema(schema)));
    }

    private String contentHash(VersionRow row, List<SkillToolDependency> dependencies) {
        try {
            return Hashing.sha256(json.writeValueAsString(new SkillContent(row.name(), row.description(), row.version(),
                    canonical(parseSchema(row.inputSchema(), "inputSchema")), canonical(parseSchema(row.outputSchema(), "outputSchema")),
                    row.promptId(), row.promptVersion(), dependencies, row.evaluationRef())));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Skill 内容摘要无法生成。", e);
        }
    }

    private String requireText(String value, int maxLength, String field) {
        if (value == null || value.isBlank() || value.length() > maxLength) throw EafException.invalid(field + " 缺失或超过长度限制。");
        return value.trim();
    }

    private record StoredToolDependency(String name, String version, String inputSchema, String outputSchema) { }
    private record ValidatedVersion(String inputSchema, String outputSchema, UUID promptId, String promptVersion,
                                    String evaluationRef, List<StoredToolDependency> toolDependencies) { }
    private record SkillKey(UUID tenantId, UUID workspaceId, UUID skillId, String version) { }
    private record VersionRow(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String name, String description,
                              String version, String inputSchema, String outputSchema, UUID promptId, String promptVersion,
                              String evaluationRef, String status, long rowVersion) { }
    private record SkillContent(String name, String description, String version, String inputSchema, String outputSchema,
                               UUID promptId, String promptVersion, List<SkillToolDependency> toolDependencies,
                               String evaluationRef) { }
}
