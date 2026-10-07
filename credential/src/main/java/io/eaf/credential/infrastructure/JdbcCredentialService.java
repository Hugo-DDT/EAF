package io.eaf.credential.infrastructure;

import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.credential.api.CredentialAdministration;
import io.eaf.credential.api.CredentialRegistration;
import io.eaf.credential.api.CredentialRequest;
import io.eaf.credential.api.CredentialResolutionPort;
import io.eaf.credential.api.ResolvedCredential;
import io.eaf.secret.api.SecretResolver;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Credential 持有授权元数据和版本；每次解析现查状态，再委托 Secret 后端取值。 */
@Service
public class JdbcCredentialService implements CredentialResolutionPort, CredentialAdministration {
    private static final Pattern REF = Pattern.compile("[a-z0-9][a-z0-9._-]{0,119}");
    private static final Pattern TOKEN = Pattern.compile("[a-z][a-z0-9._:-]{0,159}");
    private static final Pattern SECRET_REF = Pattern.compile("(?:env|vault|aws-sm|azure-kv)://[A-Za-z0-9._:/-]{1,240}");
    private final JdbcTemplate jdbc;
    private final SecretResolver secrets;
    private final WorkspaceAuthorization workspaces;
    private final AuditPort audit;

    public JdbcCredentialService(JdbcTemplate jdbc, SecretResolver secrets,
                                 WorkspaceAuthorization workspaces, AuditPort audit) {
        this.jdbc = jdbc;
        this.secrets = secrets;
        this.workspaces = workspaces;
        this.audit = audit;
    }

    @Override
    public ResolvedCredential resolve(CredentialRequest request) {
        if (request == null || request.tenantId() == null || request.workspaceId() == null || !validRef(request.credentialRef()))
            throw unavailable();
        var binding = jdbc.query("select b.id, b.audience, b.allowed_uses, b.permissions, b.status, b.current_version, "
                        + "v.secret_ref, v.status as version_status, v.valid_from, v.expires_at "
                        + "from credential.binding b join credential.secret_version v "
                        + "on v.binding_id = b.id and v.version = b.current_version "
                        + "where b.scope_type = 'WORKSPACE' and b.tenant_id = ? and b.workspace_id = ? and b.credential_ref = ?",
                rs -> rs.next() ? map(rs) : null, request.tenantId(), request.workspaceId(), request.credentialRef());
        if (binding == null || !"ACTIVE".equals(binding.status()) || !"ACTIVE".equals(binding.versionStatus())
                || binding.validFrom().isAfter(Instant.now())
                || binding.expiresAt() != null && !binding.expiresAt().isAfter(Instant.now()))
            throw unavailable();
        requirePurpose(binding, request.audience(), request.use(), request.permission());
        return resolveSecret(request.credentialRef(), binding.version(), binding.secretRef());
    }

    @Override
    public ResolvedCredential resolveSystem(String credentialRef, String audience, String use, String permission) {
        if (!validRef(credentialRef)) throw unavailable();
        var binding = jdbc.query("select b.id, b.audience, b.allowed_uses, b.permissions, b.status, b.current_version, "
                        + "v.secret_ref, v.status as version_status, v.valid_from, v.expires_at "
                        + "from credential.binding b join credential.secret_version v "
                        + "on v.binding_id = b.id and v.version = b.current_version "
                        + "where b.scope_type = 'SYSTEM' and b.tenant_id is null and b.workspace_id is null and b.credential_ref = ?",
                rs -> rs.next() ? map(rs) : null, credentialRef);
        if (binding == null || !"ACTIVE".equals(binding.status()) || !"ACTIVE".equals(binding.versionStatus())
                || binding.validFrom().isAfter(Instant.now())
                || binding.expiresAt() != null && !binding.expiresAt().isAfter(Instant.now()))
            throw unavailable();
        requirePurpose(binding, audience, use, permission);
        return resolveSecret(credentialRef, binding.version(), binding.secretRef());
    }

    @Override
    @Transactional
    public long register(ActorContext actor, CredentialRegistration registration) {
        requireManager(actor, registration == null ? null : registration.workspaceId());
        validateRegistration(registration);
        try {
            var id = UUID.randomUUID();
            jdbc.update("insert into credential.binding(id, scope_type, tenant_id, workspace_id, owner_id, credential_ref, audience, allowed_uses, permissions, status, current_version) "
                            + "values (?, 'WORKSPACE', ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', 1)",
                    id, actor.tenantId(), registration.workspaceId(), actor.actorId(), registration.credentialRef(),
                    registration.audience(), registration.allowedUses().toArray(String[]::new), registration.permissions().toArray(String[]::new));
            jdbc.update("insert into credential.secret_version(binding_id, version, secret_ref, status, valid_from, expires_at) values (?, 1, ?, 'ACTIVE', ?, ?)",
                    id, registration.secretRef(), timestamp(Instant.now()), timestamp(registration.expiresAt()));
        } catch (org.springframework.dao.DuplicateKeyException duplicate) {
            throw EafException.conflict("CREDENTIAL_ALREADY_REGISTERED", "该作用域的凭据引用已登记。");
        }
        appendAudit(actor, registration.workspaceId(), registration.credentialRef(), 1, "credential.register");
        return 1;
    }

    @Override
    @Transactional
    public long rotate(ActorContext actor, UUID workspaceId, String credentialRef, String secretRef,
                       long expectedVersion, Instant expiresAt) {
        requireManager(actor, workspaceId);
        if (!validRef(credentialRef) || !validSecretRef(secretRef) || expectedVersion < 1
                || expiresAt != null && !expiresAt.isAfter(Instant.now()))
            throw EafException.invalid("凭据轮换参数无效。");
        var binding = jdbc.query("select id, current_version, status from credential.binding where scope_type = 'WORKSPACE' "
                        + "and tenant_id = ? and workspace_id = ? and credential_ref = ? for update",
                rs -> rs.next() ? new BindingRow(rs.getObject("id", UUID.class), rs.getLong("current_version"), rs.getString("status")) : null,
                actor.tenantId(), workspaceId, credentialRef);
        if (binding == null || !"ACTIVE".equals(binding.status())) throw unavailable();
        if (binding.version() != expectedVersion)
            throw EafException.conflict("CREDENTIAL_VERSION_CONFLICT", "凭据版本已变化，请重新读取当前版本。");
        var newVersion = Math.addExact(expectedVersion, 1);
        jdbc.update("update credential.secret_version set status = 'REVOKED' where binding_id = ? and version = ? and status = 'ACTIVE'",
                binding.id(), expectedVersion);
        jdbc.update("insert into credential.secret_version(binding_id, version, secret_ref, status, valid_from, expires_at) values (?, ?, ?, 'ACTIVE', ?, ?)",
                binding.id(), newVersion, secretRef, timestamp(Instant.now()), timestamp(expiresAt));
        jdbc.update("update credential.binding set current_version = ?, updated_at = now() where id = ?",
                newVersion, binding.id());
        appendAudit(actor, workspaceId, credentialRef, newVersion, "credential.rotate");
        return newVersion;
    }

    @Override
    @Transactional
    public void disable(ActorContext actor, UUID workspaceId, String credentialRef, long expectedVersion) {
        requireManager(actor, workspaceId);
        if (!validRef(credentialRef) || expectedVersion < 1) throw EafException.invalid("凭据停用参数无效。");
        var binding = jdbc.query("select id, current_version, status from credential.binding where scope_type = 'WORKSPACE' "
                        + "and tenant_id = ? and workspace_id = ? and credential_ref = ? for update",
                rs -> rs.next() ? new BindingRow(rs.getObject("id", UUID.class), rs.getLong("current_version"), rs.getString("status")) : null,
                actor.tenantId(), workspaceId, credentialRef);
        if (binding == null || !"ACTIVE".equals(binding.status())) throw unavailable();
        if (binding.version() != expectedVersion)
            throw EafException.conflict("CREDENTIAL_VERSION_CONFLICT", "凭据版本已变化，请重新读取当前版本。");
        jdbc.update("update credential.secret_version set status = 'REVOKED' where binding_id = ? and status = 'ACTIVE'", binding.id());
        jdbc.update("update credential.binding set status = 'DISABLED', updated_at = now() where id = ?", binding.id());
        appendAudit(actor, workspaceId, credentialRef, expectedVersion, "credential.disable");
    }

    private void requireManager(ActorContext actor, UUID workspaceId) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated() || workspaceId == null)
            throw EafException.forbidden("凭据管理仅允许当前 Workspace 的直接 HUMAN 管理身份。");
        workspaces.require(actor, workspaceId, "credential:manage");
    }

    private void validateRegistration(CredentialRegistration registration) {
        if (registration == null || !validRef(registration.credentialRef()) || !validSecretRef(registration.secretRef())
                || registration.audience() == null || !TOKEN.matcher(registration.audience()).matches()
                || registration.allowedUses().isEmpty() || registration.allowedUses().size() > 16
                || registration.allowedUses().stream().anyMatch(use -> !TOKEN.matcher(use).matches())
                || registration.permissions().isEmpty() || registration.permissions().size() > 32
                || registration.permissions().stream().anyMatch(permission -> !TOKEN.matcher(permission).matches())
                || registration.expiresAt() != null && !registration.expiresAt().isAfter(Instant.now()))
            throw EafException.invalid("凭据的引用、受众、用途、权限或有效期无效。");
    }

    private void requirePurpose(CredentialRow binding, String audience, String use, String permission) {
        if (audience == null || use == null || permission == null || !audience.equals(binding.audience())
                || !binding.uses().contains(use) || !binding.permissions().contains(permission))
            throw EafException.conflict("CREDENTIAL_SCOPE_DENIED", "凭据的受众、用途或权限不匹配。");
    }

    private ResolvedCredential resolveSecret(String ref, long version, String secretRef) {
        try {
            var value = secrets.resolve(secretRef).revealForOutboundRequest();
            if (value == null || value.isBlank() || value.length() > 4096) throw new IllegalStateException();
            return new ResolvedCredential(ref, version, value);
        } catch (RuntimeException unavailable) {
            // 不附加后端异常 cause，避免 Provider/SDK 或 Secret 实现把秘密带入日志。
            throw unavailable();
        }
    }

    private CredentialRow map(ResultSet rs) throws SQLException {
        return new CredentialRow(rs.getString("audience"), strings(rs.getArray("allowed_uses")),
                strings(rs.getArray("permissions")), rs.getString("status"), rs.getLong("current_version"),
                rs.getString("secret_ref"), rs.getString("version_status"),
                rs.getTimestamp("valid_from").toInstant(), rs.getTimestamp("expires_at") == null ? null : rs.getTimestamp("expires_at").toInstant());
    }

    private Set<String> strings(java.sql.Array value) throws SQLException {
        var raw = value == null ? null : (Object[]) value.getArray();
        if (raw == null) return Set.of();
        var result = new java.util.HashSet<String>();
        for (var item : raw) if (item instanceof String text) result.add(text);
        return Set.copyOf(result);
    }

    private void appendAudit(ActorContext actor, UUID workspaceId, String ref, long version, String action) {
        var payload = "{\"credentialRef\":\"" + ref + "\",\"version\":" + version + "}";
        audit.append(new AuditFact(UUID.randomUUID().toString(), actor.tenantId(), workspaceId, actor.actorId(), null,
                action, "SUCCEEDED", payload, null));
    }

    private boolean validRef(String ref) { return ref != null && REF.matcher(ref).matches(); }
    private boolean validSecretRef(String ref) { return ref != null && SECRET_REF.matcher(ref).matches(); }
    private java.sql.Timestamp timestamp(Instant instant) { return instant == null ? null : java.sql.Timestamp.from(instant); }
    private EafException unavailable() { return EafException.conflict("CREDENTIAL_UNAVAILABLE", "凭据不可用或不在当前作用域。"); }

    private record CredentialRow(String audience, Set<String> uses, Set<String> permissions, String status,
                                 long version, String secretRef, String versionStatus, Instant validFrom, Instant expiresAt) { }
    private record BindingRow(UUID id, long version, String status) { }
}
