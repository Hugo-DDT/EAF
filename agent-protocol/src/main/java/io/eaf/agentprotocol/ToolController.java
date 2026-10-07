package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.tool.api.ToolDefinition;
import java.util.List;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/tools")
public class ToolController {
    private final ToolCatalog tools;
    private final ObjectMapper json;

    public ToolController(ToolCatalog tools, ObjectMapper json) { this.tools = tools; this.json = json; }

    @GetMapping
    ToolList list(@PathVariable UUID workspaceId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        return new ToolList(tools.list(actor, workspaceId).stream().map(t -> ToolView.of(t, json)).toList(), null);
    }

    record ToolList(List<ToolView> items, String nextCursor) { }
    record ToolView(String name, String version, String description, JsonNode inputSchema, JsonNode outputSchema,
                    String permissionAction, String effect, String bindingRef, String status) {
        static ToolView of(ToolDefinition t, ObjectMapper json) {
            try { return new ToolView(t.name(), t.version(), t.description(), json.readTree(t.inputSchema()), json.readTree(t.outputSchema()), t.permissionAction(), t.effect(), t.bindingRef(), t.status()); }
            catch (Exception e) { throw new IllegalStateException("工具声明无法解析。", e); }
        }
    }
}
// 本文件负责实现 EAF 的 ToolController.java 相关代码。
