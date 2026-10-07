package io.eaf.prompt.infrastructure;

import io.eaf.prompt.api.PromptCatalog;
import io.eaf.prompt.api.PromptVersion;
import io.eaf.prompt.api.RenderedPrompt;
import io.eaf.shared.EafException;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcPromptCatalog implements PromptCatalog {
    private final JdbcTemplate jdbc;

    public JdbcPromptCatalog(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public PromptVersion requirePublished(UUID tenantId, UUID workspaceId, UUID promptId, String version) {
        var result = jdbc.query("select id, tenant_id, workspace_id, asset_version, system_template, user_template, status from prompt.version where id = ? and tenant_id = ? and workspace_id = ? and asset_version = ?",
                rs -> rs.next() ? new PromptVersion(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class), rs.getString("asset_version"), rs.getString("system_template"), rs.getString("user_template"), rs.getString("status")) : null,
                promptId, tenantId, workspaceId, version);
        if (result == null || !"PUBLISHED".equals(result.status())) throw EafException.notFound();
        return result;
    }

    @Override
    public RenderedPrompt render(UUID tenantId, UUID workspaceId, UUID promptId, String version, String input) {
        var prompt = requirePublished(tenantId, workspaceId, promptId, version);
        if (input.length() > 8_000) throw EafException.invalid("input 超过 8,000 字符限制。");
        return new RenderedPrompt(version, List.of(
                new RenderedPrompt.Message("system", prompt.systemTemplate()),
                new RenderedPrompt.Message("user", prompt.userTemplate().replace("{{input}}", input))));
    }
}
// 本文件负责实现 EAF 的 JdbcPromptCatalog.java 相关代码。
