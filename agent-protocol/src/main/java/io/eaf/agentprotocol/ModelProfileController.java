package io.eaf.agentprotocol;

import io.eaf.agent.api.AgentCatalog;
import io.eaf.capability.api.CapabilityService;
import io.eaf.model.api.ModelProfileAvailability;
import io.eaf.model.api.ModelProfileCatalog;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/model-profiles")
public class ModelProfileController {
    private static final UUID P15_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000012");
    private static final UUID P15_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000010");
    private final CapabilityService capabilities;
    private final AgentCatalog agents;
    private final ModelProfileCatalog profiles;
    private final WorkspaceAuthorization workspaces;

    public ModelProfileController(CapabilityService capabilities, AgentCatalog agents,
                                  ModelProfileCatalog profiles, WorkspaceAuthorization workspaces) {
        this.capabilities = capabilities;
        this.agents = agents;
        this.profiles = profiles;
        this.workspaces = workspaces;
    }

    @GetMapping
    public List<ModelProfileAvailability> list(@PathVariable UUID workspaceId,
            @RequestParam UUID capabilityId, @RequestParam String capabilityVersion,
            Authentication authentication) {
        ActorContext actor = ApiSupport.actor(authentication);
        if (actor.type() != io.eaf.shared.ActorType.HUMAN || actor.delegated()
                || !P15_CAPABILITY_ID.equals(capabilityId)
                || !Set.of("1.0.0", "1.1.0").contains(capabilityVersion))
            throw EafException.notFound();
        workspaces.require(actor, workspaceId, "capability:read");
        workspaces.require(actor, workspaceId, "agent:read");
        var capability = capabilities.requirePublished(actor, workspaceId, capabilityId, capabilityVersion);
        if (!P15_AGENT_ID.equals(capability.agentId())) throw EafException.notFound();
        var agent = agents.requirePublished(actor.tenantId(), workspaceId, capability.agentId(), capability.agentVersion());
        if (!"SERVICE_REQUEST_PLAN_V1".equals(agent.responseProfile()) || !agents.tools(actor.tenantId(), workspaceId,
                agent.id(), agent.version()).isEmpty()) throw EafException.notFound();
        return profiles.listSelectable(agent.modelProfileId());
    }
}
