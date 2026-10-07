package io.eaf.agent.infrastructure;

import io.eaf.agent.api.AgentCatalog;
import io.eaf.agent.api.AgentDefinition;
import io.eaf.agent.api.ToolBinding;
import io.eaf.shared.EafException;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcAgentCatalog implements AgentCatalog {
    private final JdbcTemplate jdbc;

    public JdbcAgentCatalog(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public AgentDefinition requirePublished(UUID tenantId, UUID workspaceId, UUID agentId, String version) {
        var result = jdbc.query("select id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy from agent.version where id = ? and tenant_id = ? and workspace_id = ? and asset_version = ?",
                rs -> rs.next() ? map(rs) : null, agentId, tenantId, workspaceId, version);
        if (result == null || !"PUBLISHED".equals(result.status())) throw EafException.notFound();
        return result;
    }

    @Override
    public List<AgentDefinition> list(UUID tenantId, UUID workspaceId) {
        return jdbc.query("select id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy from agent.version where tenant_id = ? and workspace_id = ? and status = 'PUBLISHED' order by id, asset_version",
                (rs, row) -> map(rs), tenantId, workspaceId);
    }

    @Override
    public List<ToolBinding> tools(UUID tenantId, UUID workspaceId, UUID agentId, String version) {
        return jdbc.query("select tool_name, tool_version from agent.tool_binding where tenant_id = ? and workspace_id = ? and agent_id = ? and agent_version = ? order by tool_name, tool_version",
                (rs, row) -> new ToolBinding(rs.getString("tool_name"), rs.getString("tool_version")),
                tenantId, workspaceId, agentId, version);
    }

    private AgentDefinition map(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new AgentDefinition(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class), rs.getString("name"), rs.getString("asset_version"), rs.getObject("prompt_id", UUID.class), rs.getString("prompt_version"), rs.getObject("model_profile_id", UUID.class), rs.getString("status"), rs.getBoolean("rag_enabled"), rs.getString("response_profile"), rs.getString("retrieval_mode"), rs.getString("evidence_policy"));
    }
}
// 本文件负责实现 EAF 的 JdbcAgentCatalog.java 相关代码。
