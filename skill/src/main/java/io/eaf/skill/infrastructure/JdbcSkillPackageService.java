package io.eaf.skill.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.skill.api.SkillPackageService;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcSkillPackageService implements SkillPackageService {
    private static final TypeReference<Map<String, String>> FILE_MAP = new TypeReference<>() { };
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final ObjectMapper json;
    private final DeclarativeCapabilityPackageCodec codec = new DeclarativeCapabilityPackageCodec();

    public JdbcSkillPackageService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces, ObjectMapper json) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.json = json;
    }

    @Override
    public byte[] export(ActorContext actor, UUID workspaceId, ExportSource source) {
        requireExportAccess(actor, workspaceId, source);
        return codec.export(source);
    }

    @Override
    public DeclarationPackage describe(ActorContext actor, UUID workspaceId, ExportSource source) {
        requireExportAccess(actor, workspaceId, source);
        var rendered = codec.render(source);
        return new DeclarationPackage(packageSource(source), PROFILE, rendered.packageHash(), codec.describe(rendered));
    }

    @Override
    public DeclarationFile readDeclarationFile(ActorContext actor, UUID workspaceId, ExportSource source,
                                               String expectedPackageHash, String relativePath) {
        requireExportAccess(actor, workspaceId, source);
        if (expectedPackageHash == null || !expectedPackageHash.matches("[0-9a-f]{64}"))
            throw EafException.invalid("packageHash 格式无效。");
        var rendered = codec.render(source);
        if (!expectedPackageHash.equals(rendered.packageHash()))
            throw EafException.conflict("PACKAGE_VERSION_CHANGED", "声明包版本已变化，请重新发现当前声明。");
        var content = codec.readFile(rendered, relativePath);
        var info = codec.describe(rendered).stream().filter(file -> file.path().equals(relativePath)).findFirst()
                .orElseThrow(() -> EafException.invalid("声明文件路径无效。"));
        return new DeclarationFile(info, content);
    }

    private void requireExportAccess(ActorContext actor, UUID workspaceId, ExportSource source) {
        workspaces.require(actor, workspaceId, "skill:read");
        if (source == null || !workspaceId.equals(source.sourceWorkspaceId()))
            throw EafException.invalid("导出来源 Workspace 必须与请求 Workspace 一致。");
    }

    private static PackageSource packageSource(ExportSource source) {
        var capability = source.capability();
        var skill = source.skill();
        return new PackageSource(source.sourceWorkspaceId(), capability.id(), capability.version(), capability.contentHash(),
                skill.id(), skill.version(), skill.contentHash());
    }

    @Override
    public PackageValidation validate(ActorContext actor, UUID workspaceId, byte[] zip) {
        workspaces.require(actor, workspaceId, "skill:write");
        return codec.validate(zip).validation();
    }

    @Override
    @Transactional
    public ImportResult importPackage(ActorContext actor, UUID workspaceId, String idempotencyKey, byte[] zip) {
        var access = workspaces.require(actor, workspaceId, "skill:write");
        var key = requireKey(idempotencyKey);
        // 导入每次都重新解析提交的字节；不接受预检时生成的任何服务端状态。
        var decoded = codec.validate(zip);
        var item = decoded.validation();
        var keyHash = Hashing.sha256(key);
        var requestHash = Hashing.sha256(item.profile() + "\n" + item.packageHash());
        var id = UUID.randomUUID();
        var filesJson = writeJson(new TreeMap<>(decoded.files()));
        var changed = jdbc.update("insert into skill.declarative_package(id, tenant_id, workspace_id, owner_id, profile, format_version, template_version, name, package_hash, source_workspace_id, capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash, manifest_json, files_json, idempotency_key_hash, request_hash) "
                        + "values (?, ?, ?, ?, ?, '1', '1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?) "
                        + "on conflict (tenant_id, workspace_id, owner_id, idempotency_key_hash) do nothing",
                id, access.tenantId(), access.workspaceId(), actor.actorId(), item.profile(), item.name(), item.packageHash(),
                item.source().workspaceId(), item.source().capabilityId(), item.source().capabilityVersion(), item.source().capabilityHash(),
                item.source().skillId(), item.source().skillVersion(), item.source().skillHash(), decoded.manifestJson(), filesJson,
                keyHash, requestHash);
        if (changed == 1) return new ImportResult(load(access.tenantId(), workspaceId, id, actor.actorId()).view(), true);
        var existing = findByKey(access.tenantId(), workspaceId, actor.actorId(), keyHash);
        if (existing == null) throw new IllegalStateException("Package idempotency conflict row was not found.");
        if (!existing.requestHash().equals(requestHash))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "相同 Idempotency-Key 已用于不同声明包。");
        return new ImportResult(existing.view(), false);
    }

    @Override
    public PackagePage list(ActorContext actor, UUID workspaceId, int limit, int offset) {
        var access = workspaces.require(actor, workspaceId, "skill:read");
        if (limit < 1 || limit > 50 || offset < 0) throw EafException.invalid("limit 必须为 1-50，offset 必须为非负整数。");
        if (offset > Integer.MAX_VALUE - limit) throw EafException.invalid("offset 超过可分页范围。");
        var rows = jdbc.query("select " + COLUMNS + " from skill.declarative_package where tenant_id = ? and workspace_id = ? and owner_id = ? and status = 'STORED' "
                        + "order by created_at desc, id limit ? offset ?", (rs, rowNumber) -> row(rs),
                access.tenantId(), workspaceId, actor.actorId(), limit + 1, offset);
        var hasNext = rows.size() > limit;
        var items = rows.stream().limit(limit).map(PackageRow::view).toList();
        return new PackagePage(items, hasNext ? offset + limit : null);
    }

    @Override
    public PackageView get(ActorContext actor, UUID workspaceId, UUID packageId) {
        var access = workspaces.require(actor, workspaceId, "skill:read");
        return load(access.tenantId(), workspaceId, packageId, actor.actorId()).view();
    }

    @Override
    public PackageDownload download(ActorContext actor, UUID workspaceId, UUID packageId) {
        var access = workspaces.require(actor, workspaceId, "skill:read");
        var row = load(access.tenantId(), workspaceId, packageId, actor.actorId());
        if (!"STORED".equals(row.status())) throw EafException.notFound();
        var content = codec.download(row.capabilityId(), readFiles(row.filesJson()));
        return new PackageDownload("eaf-skill-package-" + row.id() + ".zip", content);
    }

    @Override
    @Transactional
    public PackageView archive(ActorContext actor, UUID workspaceId, UUID packageId, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "skill:write");
        if (expectedVersion < 1) throw EafException.invalid("expectedVersion 必须为正整数。");
        var current = load(access.tenantId(), workspaceId, packageId, actor.actorId());
        if ("ARCHIVED".equals(current.status())) return current.view();
        var changed = jdbc.update("update skill.declarative_package set status = 'ARCHIVED', archived_at = now(), row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and owner_id = ? and id = ? and status = 'STORED' and row_version = ?",
                access.tenantId(), workspaceId, actor.actorId(), packageId, expectedVersion);
        if (changed != 1) {
            current = load(access.tenantId(), workspaceId, packageId, actor.actorId());
            if (!"ARCHIVED".equals(current.status())) throw EafException.conflict("VERSION_CONFLICT", "声明包状态或版本已变化。");
        }
        return load(access.tenantId(), workspaceId, packageId, actor.actorId()).view();
    }

    private static final String COLUMNS = "id, owner_id, profile, name, package_hash, source_workspace_id, capability_id, capability_version, capability_hash, skill_id, skill_version, skill_hash, status, row_version, files_json::text files_json, request_hash, created_at, archived_at";

    private PackageRow load(UUID tenantId, UUID workspaceId, UUID packageId, UUID ownerId) {
        var row = jdbc.query("select " + COLUMNS + " from skill.declarative_package where tenant_id = ? and workspace_id = ? and owner_id = ? and id = ?",
                rs -> rs.next() ? row(rs) : null, tenantId, workspaceId, ownerId, packageId);
        if (row == null) throw EafException.notFound();
        return row;
    }

    private PackageRow findByKey(UUID tenantId, UUID workspaceId, UUID ownerId, String keyHash) {
        return jdbc.query("select " + COLUMNS + " from skill.declarative_package where tenant_id = ? and workspace_id = ? and owner_id = ? and idempotency_key_hash = ?",
                rs -> rs.next() ? row(rs) : null, tenantId, workspaceId, ownerId, keyHash);
    }

    private PackageRow row(ResultSet rs) throws SQLException {
        return new PackageRow(rs.getObject("id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("profile"),
                rs.getString("name"), rs.getString("package_hash"), rs.getObject("source_workspace_id", UUID.class),
                rs.getObject("capability_id", UUID.class), rs.getString("capability_version"), rs.getString("capability_hash"),
                rs.getObject("skill_id", UUID.class), rs.getString("skill_version"), rs.getString("skill_hash"),
                rs.getString("status"), rs.getLong("row_version"), rs.getString("files_json"), rs.getString("request_hash"),
                instant(rs.getTimestamp("created_at")), instant(rs.getTimestamp("archived_at")));
    }

    private Instant instant(Timestamp value) { return value == null ? null : value.toInstant(); }

    private String writeJson(Object value) {
        try { return json.writeValueAsString(value); }
        catch (JsonProcessingException impossible) { throw new IllegalStateException("声明包文件索引无法序列化。", impossible); }
    }

    private Map<String, String> readFiles(String source) {
        try { return json.readValue(source, FILE_MAP); }
        catch (JsonProcessingException corrupt) { throw new IllegalStateException("已存声明包文件索引损坏。", corrupt); }
    }

    private String requireKey(String value) {
        if (value == null || value.isBlank() || value.length() > 128 || !value.equals(value.trim()))
            throw EafException.invalid("Idempotency-Key 必须为 1-128 个非空字符且无首尾空格。");
        return value;
    }

    private record PackageRow(UUID id, UUID ownerId, String profile, String name, String packageHash,
                              UUID sourceWorkspaceId, UUID capabilityId, String capabilityVersion, String capabilityHash,
                              UUID skillId, String skillVersion, String skillHash, String status, long rowVersion,
                              String filesJson, String requestHash, Instant createdAt, Instant archivedAt) {
        PackageSource source() {
            return new PackageSource(sourceWorkspaceId, capabilityId, capabilityVersion, capabilityHash, skillId, skillVersion, skillHash);
        }
        PackageView view() {
            var files = "ARCHIVED".equals(status) ? List.<String>of() : List.of(
                    "SKILL.md", "eaf-package.json", "references/capability.json", "references/skill.json", "references/usage.md");
            return new PackageView(id, ownerId, profile, name, packageHash, source(), status, rowVersion, files, createdAt, archivedAt);
        }
    }
}
