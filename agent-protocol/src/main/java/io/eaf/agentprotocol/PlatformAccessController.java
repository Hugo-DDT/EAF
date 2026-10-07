package io.eaf.agentprotocol;

import io.eaf.agentprotocol.PlatformAccessApplicationService.DiscoveryRequest;
import java.util.Set;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/discovery")
public class PlatformAccessController {
    private static final Set<String> ALLOWED_QUERY = Set.of("kind", "query", "limit", "offset");
    private final PlatformAccessApplicationService access;

    public PlatformAccessController(PlatformAccessApplicationService access) {
        this.access = access;
    }

    @GetMapping
    PlatformAccessApplicationService.DiscoveryPage search(@PathVariable UUID workspaceId,
            @RequestParam MultiValueMap<String, String> query, Authentication authentication) {
        if (query.keySet().stream().anyMatch(key -> !ALLOWED_QUERY.contains(key)))
            throw io.eaf.shared.EafException.invalid("目录查询包含未允许字段。");
        var request = new DiscoveryRequest(single(query, "kind"), single(query, "query"),
                PlatformAccessApplicationService.optionalQueryInteger(single(query, "limit"), "limit"),
                PlatformAccessApplicationService.optionalQueryInteger(single(query, "offset"), "offset"));
        return access.search(ApiSupport.actor(authentication), workspaceId, request);
    }

    @GetMapping("/skills/{skillId}/versions/{version}")
    PlatformAccessApplicationService.SkillUsageDetail skill(@PathVariable UUID workspaceId,
            @PathVariable UUID skillId, @PathVariable String version, Authentication authentication) {
        return access.getSkill(ApiSupport.actor(authentication), workspaceId, skillId, version);
    }

    @GetMapping("/capabilities/{capabilityId}/versions/{version}")
    PlatformAccessApplicationService.CapabilityUsageDetail capability(@PathVariable UUID workspaceId,
            @PathVariable UUID capabilityId, @PathVariable String version, Authentication authentication) {
        return access.getCapability(ApiSupport.actor(authentication), workspaceId, capabilityId, version);
    }

    private static String single(MultiValueMap<String, String> values, String name) {
        var all = values.get(name);
        if (all == null) return null;
        if (all.size() != 1) throw io.eaf.shared.EafException.invalid(name + " 只能提供一次。");
        return all.getFirst();
    }
}
