package io.eaf.agentprotocol;

import io.eaf.agent.api.AgentCatalog;
import io.eaf.agent.api.AgentDefinition;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.List;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/agents")
public class AgentController {
    private final WorkspaceAuthorization workspaces;
    private final AgentCatalog agents;
    public AgentController(WorkspaceAuthorization workspaces, AgentCatalog agents) { this.workspaces = workspaces; this.agents = agents; }

    @GetMapping
    AgentList list(@PathVariable UUID workspaceId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "agent:read");
        return new AgentList(agents.list(actor.tenantId(), access.workspaceId()).stream().map(AgentView::of).toList());
    }
    record AgentList(List<AgentView> items, String nextCursor) { AgentList(List<AgentView> items) { this(items, null); } }
    // ragEnabled 是不可变 Agent 版本的运行契约，决定是否保存并校验 Context 快照。
    record AgentView(UUID id, String name, String version, UUID promptId, String promptVersion, UUID modelProfileId,
                     String status, boolean ragEnabled, String responseProfile, String retrievalMode, String evidencePolicy) {
        static AgentView of(AgentDefinition a) { return new AgentView(a.id(), a.name(), a.version(), a.promptId(), a.promptVersion(), a.modelProfileId(), a.status(), a.ragEnabled(), a.responseProfile(), a.retrievalMode(), a.evidencePolicy()); }
    }
}
// 本文件负责实现 EAF 的 AgentController.java 相关代码。
