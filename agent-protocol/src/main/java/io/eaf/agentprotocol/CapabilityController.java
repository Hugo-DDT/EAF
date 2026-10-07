package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.capability.api.CapabilityToolDependency;
import io.eaf.capability.api.CapabilityToolReference;
import io.eaf.capability.api.CreateCapabilityCommand;
import io.eaf.capability.api.CreateCapabilityVersionCommand;
import io.eaf.shared.EafException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/capabilities")
public class CapabilityController {
    private final CapabilityService capabilities;
    private final ObjectMapper json;

    public CapabilityController(CapabilityService capabilities, ObjectMapper json) {
        this.capabilities = capabilities;
        this.json = json;
    }

    @GetMapping
    CapabilityList list(@PathVariable UUID workspaceId, Authentication authentication) {
        return new CapabilityList(capabilities.list(ApiSupport.actor(authentication), workspaceId).stream().map(CapabilityView::of).toList(), null);
    }

    // Actor、Owner、状态与解析权限只由认证上下文和 Capability 域服务决定。
    @PostMapping
    ResponseEntity<CapabilityView> create(@PathVariable UUID workspaceId, @RequestBody Map<String, Object> rawBody,
                                          Authentication authentication) {
        var body = convert(rawBody, CreateBody.class,
                Set.of("name", "description", "version", "agentId", "agentVersion", "skillId", "skillVersion",
                        "promptId", "promptVersion", "toolDependencies", "evaluationRef"));
        var actor = ApiSupport.actor(authentication);
        var definition = capabilities.create(new CreateCapabilityCommand(actor, workspaceId, body.name(), body.description(), version(
                body.version(), body.agentId(), body.agentVersion(), body.skillId(), body.skillVersion(), body.promptId(),
                body.promptVersion(), body.toolDependencies(), body.evaluationRef())));
        return ResponseEntity.status(201).body(CapabilityView.of(definition));
    }

    @PostMapping("/{capabilityId}/versions")
    CapabilityView addVersion(@PathVariable UUID workspaceId, @PathVariable UUID capabilityId,
                              @RequestBody Map<String, Object> rawBody, Authentication authentication) {
        var body = convert(rawBody, VersionBody.class,
                Set.of("version", "agentId", "agentVersion", "skillId", "skillVersion", "promptId", "promptVersion",
                        "toolDependencies", "evaluationRef"));
        return CapabilityView.of(capabilities.addVersion(ApiSupport.actor(authentication), workspaceId, capabilityId,
                version(body.version(), body.agentId(), body.agentVersion(), body.skillId(), body.skillVersion(),
                        body.promptId(), body.promptVersion(), body.toolDependencies(), body.evaluationRef())));
    }

    @GetMapping("/{capabilityId}/versions/{version}")
    CapabilityView get(@PathVariable UUID workspaceId, @PathVariable UUID capabilityId, @PathVariable String version,
                       Authentication authentication) {
        return CapabilityView.of(capabilities.get(ApiSupport.actor(authentication), workspaceId, capabilityId, version));
    }

    @PostMapping("/{capabilityId}/versions/{version}/publish")
    CapabilityView publish(@PathVariable UUID workspaceId, @PathVariable UUID capabilityId, @PathVariable String version,
                           @RequestParam long expectedVersion, Authentication authentication) {
        return CapabilityView.of(capabilities.publish(ApiSupport.actor(authentication), workspaceId, capabilityId, version, expectedVersion));
    }

    @PostMapping("/{capabilityId}/versions/{version}/revoke")
    CapabilityView revoke(@PathVariable UUID workspaceId, @PathVariable UUID capabilityId, @PathVariable String version,
                          @RequestParam long expectedVersion, Authentication authentication) {
        return CapabilityView.of(capabilities.revoke(ApiSupport.actor(authentication), workspaceId, capabilityId, version, expectedVersion));
    }

    private <T> T convert(Map<String, Object> rawBody, Class<T> type, Set<String> allowed) {
        if (rawBody == null) throw EafException.invalid("请求体不能为空。");
        if (rawBody.keySet().stream().anyMatch(key -> !allowed.contains(key))) throw EafException.invalid("请求包含未允许字段。");
        try { return json.convertValue(rawBody, type); }
        catch (IllegalArgumentException e) { throw EafException.invalid("请求字段类型无效。"); }
    }

    private CreateCapabilityVersionCommand version(String version, UUID agentId, String agentVersion,
                                                   UUID skillId, String skillVersion, UUID promptId,
                                                   String promptVersion, List<CapabilityToolReference> tools,
                                                   String evaluationRef) {
        return new CreateCapabilityVersionCommand(version, agentId, agentVersion, skillId, skillVersion, promptId,
                promptVersion, tools, evaluationRef);
    }

    record CapabilityList(List<CapabilityView> items, String nextCursor) { }
    record CreateBody(String name, String description, String version, UUID agentId, String agentVersion,
                      UUID skillId, String skillVersion, UUID promptId, String promptVersion,
                      List<CapabilityToolReference> toolDependencies, String evaluationRef) { }
    record VersionBody(String version, UUID agentId, String agentVersion, UUID skillId, String skillVersion,
                       UUID promptId, String promptVersion, List<CapabilityToolReference> toolDependencies,
                       String evaluationRef) { }
    record CapabilityView(UUID id, UUID ownerId, String name, String description, String version,
                          UUID agentId, String agentVersion, UUID skillId, String skillVersion,
                          String skillContentHash, UUID promptId, String promptVersion,
                          List<CapabilityToolDependency> toolDependencies, String evaluationRef,
                          String status, long rowVersion, String contentHash) {
        static CapabilityView of(CapabilityDefinition capability) {
            return new CapabilityView(capability.id(), capability.ownerId(), capability.name(), capability.description(),
                    capability.version(), capability.agentId(), capability.agentVersion(), capability.skillId(),
                    capability.skillVersion(), capability.skillContentHash(), capability.promptId(), capability.promptVersion(),
                    capability.toolDependencies(), capability.evaluationRef(), capability.status(), capability.rowVersion(),
                    capability.contentHash());
        }
    }
}
// 本文件负责实现 CapabilityController.java 相关代码。
