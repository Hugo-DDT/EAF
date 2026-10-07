package io.eaf.agent.infrastructure;

import io.eaf.agent.api.RemoteAgentCatalog;
import io.eaf.agent.api.RemoteAgentRegistration;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcRemoteAgentCatalog implements RemoteAgentCatalog {
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;

    public JdbcRemoteAgentCatalog(JdbcTemplate jdbc, WorkspaceAuthorization workspaces) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
    }

    @Override
    public RemoteAgentRegistration requireActive(ActorContext actor, UUID workspaceId, String agentKey) {
        // Agent 登记是 Workspace 私有配置，未授权调用者不能通过 key 探测其存在性。
        var access = workspaces.require(actor, workspaceId, "agent:read");
        var registrations = jdbc.query("select id, tenant_id, workspace_id, owner_id, agent_key, agent_version, "
                        + "capability_id, capability_version, connector_id, peer_skill_id, allowed_input_fields, "
                        + "allowed_output_fields, delegation_actions, resource_scope "
                        + "from agent.remote_registration where tenant_id = ? and workspace_id = ? "
                        + "and agent_key = ? and status = 'ACTIVE'",
                (rs, row) -> map(rs), access.tenantId(), access.workspaceId(), agentKey);
        if (registrations.isEmpty()) throw EafException.notFound();
        return registrations.getFirst();
    }

    // 只返回登记快照；Capability、Connector 和委托有效性由各自公共 API 再核验。
    private static RemoteAgentRegistration map(ResultSet rs) throws SQLException {
        return new RemoteAgentRegistration(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class),
                rs.getString("agent_key"), rs.getString("agent_version"),
                rs.getObject("capability_id", UUID.class), rs.getString("capability_version"),
                rs.getObject("connector_id", UUID.class), rs.getString("peer_skill_id"),
                strings(rs.getArray("allowed_input_fields")), strings(rs.getArray("allowed_output_fields")),
                strings(rs.getArray("delegation_actions")), rs.getString("resource_scope"));
    }

    private static List<String> strings(Array value) throws SQLException {
        if (value == null) return List.of();
        // PostgreSQL 数组连接资源要及时释放，避免长驻协议入口耗尽连接。
        try {
            return Arrays.asList((String[]) value.getArray());
        } finally {
            value.free();
        }
    }
}
