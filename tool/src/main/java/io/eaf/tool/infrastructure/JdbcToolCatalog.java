package io.eaf.tool.infrastructure;

import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.tool.api.ToolDefinition;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcToolCatalog implements ToolCatalog {
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;

    public JdbcToolCatalog(JdbcTemplate jdbc, WorkspaceAuthorization workspaces) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
    }

    @Override
    public List<ToolDefinition> list(ActorContext actor, UUID workspaceId) {
        workspaces.require(actor, workspaceId, "tool:read");
        return jdbc.query("select tenant_id, workspace_id, name, asset_version, description, input_schema::text, output_schema::text, permission_action, effect, binding_ref, status from tool.version where tenant_id = ? and workspace_id = ? and status = 'PUBLISHED' order by name, asset_version",
                (rs, row) -> map(rs), actor.tenantId(), workspaceId);
    }

    @Override
    public ToolDefinition requirePublished(UUID tenantId, UUID workspaceId, String name, String version) {
        var result = jdbc.query("select tenant_id, workspace_id, name, asset_version, description, input_schema::text, output_schema::text, permission_action, effect, binding_ref, status from tool.version where tenant_id = ? and workspace_id = ? and name = ? and asset_version = ?",
                rs -> rs.next() ? map(rs) : null, tenantId, workspaceId, name, version);
        if (result == null || !"PUBLISHED".equals(result.status())) throw EafException.notFound();
        return result;
    }

    private ToolDefinition map(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new ToolDefinition(rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                rs.getString("name"), rs.getString("asset_version"), rs.getString("description"),
                rs.getString("input_schema"), rs.getString("output_schema"), rs.getString("permission_action"),
                rs.getString("effect"), rs.getString("binding_ref"), rs.getString("status"));
    }
}
// 本文件负责实现 EAF 的 JdbcToolCatalog.java 相关代码。
