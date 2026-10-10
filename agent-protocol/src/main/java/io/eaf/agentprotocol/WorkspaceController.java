package io.eaf.agentprotocol;

import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceCatalog;
import io.eaf.workspace.api.WorkspaceCreateResult;
import io.eaf.workspace.api.WorkspaceKind;
import io.eaf.workspace.api.WorkspaceMembershipAdministration;
import io.eaf.workspace.api.WorkspacePage;
import io.eaf.workspace.api.WorkspaceSummary;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WorkspaceController {
    private static final Set<String> GRANTABLE_ACTIONS = Set.of("context:read", "knowledge:read", "knowledge:write",
            "knowledge:publish", "knowledge:source:manage", "workspace:members:manage", "memory:read",
            "prompt:read", "prompt:manage", "prompt:publish");
    private final WorkspaceCatalog catalog;
    private final WorkspaceMembershipAdministration memberships;

    public WorkspaceController(WorkspaceCatalog catalog, WorkspaceMembershipAdministration memberships) {
        this.catalog = catalog;
        this.memberships = memberships;
    }

    @PutMapping("/api/v1/workspaces/{workspaceId}")
    ResponseEntity<WorkspaceSummary> create(@PathVariable UUID workspaceId, @RequestBody Map<String, Object> body,
                                            Authentication authentication) {
        requireFields(body, Set.of("name", "kind", "parentWorkspaceId"));
        var name = string(body, "name");
        var kind = enumKind(string(body, "kind"));
        var parent = optionalUuid(body, "parentWorkspaceId");
        WorkspaceCreateResult result = catalog.create(ApiSupport.actor(authentication), workspaceId, name, kind, parent);
        return ResponseEntity.status(result.created() ? 201 : 200).body(result.workspace());
    }

    @GetMapping("/api/v1/workspaces")
    WorkspacePage list(@RequestParam MultiValueMap<String, String> query, Authentication authentication) {
        if (query.keySet().stream().anyMatch(key -> !Set.of("kind", "limit", "offset").contains(key)))
            throw EafException.invalid("Workspace 列表查询包含未允许字段。");
        var kindText = single(query, "kind");
        var kind = kindText == null ? null : enumKind(kindText);
        var limit = PlatformAccessApplicationService.optionalQueryInteger(single(query, "limit"), "limit");
        var offset = PlatformAccessApplicationService.optionalQueryInteger(single(query, "offset"), "offset");
        return catalog.list(ApiSupport.actor(authentication), kind, limit == null ? 20 : limit, offset == null ? 0 : offset);
    }

    @PutMapping("/api/v1/workspaces/{workspaceId}/members/{subjectId}/grants")
    Map<String, Integer> grant(@PathVariable UUID workspaceId, @PathVariable UUID subjectId,
                              @RequestBody Map<String, Object> body, Authentication authentication) {
        var actions = actions(body);
        return Map.of("changed", memberships.grant(ApiSupport.actor(authentication), workspaceId, subjectId, actions));
    }

    @PostMapping("/api/v1/workspaces/{workspaceId}/members/{subjectId}/grants/revoke")
    Map<String, Integer> revoke(@PathVariable UUID workspaceId, @PathVariable UUID subjectId,
                                @RequestBody Map<String, Object> body, Authentication authentication) {
        var actions = actions(body);
        return Map.of("changed", memberships.revoke(ApiSupport.actor(authentication), workspaceId, subjectId, actions));
    }

    private static Set<String> actions(Map<String, Object> body) {
        requireFields(body, Set.of("actions"));
        if (!(body.get("actions") instanceof java.util.List<?> values) || values.isEmpty() || values.size() > GRANTABLE_ACTIONS.size())
            throw EafException.invalid("actions 必须是有限的知识协作授权列表。");
        var actions = new LinkedHashSet<String>();
        for (var value : values) {
            if (!(value instanceof String action) || !GRANTABLE_ACTIONS.contains(action) || !actions.add(action))
                throw EafException.invalid("actions 包含重复或未允许的动作。");
        }
        return Set.copyOf(actions);
    }

    private static void requireFields(Map<String, Object> body, Set<String> allowed) {
        if (body == null || body.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("Workspace 请求包含未允许字段。");
    }

    private static String string(Map<String, Object> body, String name) {
        if (!(body.get(name) instanceof String value) || value.isBlank()) throw EafException.invalid(name + " 必须是非空字符串。");
        return value;
    }

    private static UUID optionalUuid(Map<String, Object> body, String name) {
        var value = body.get(name);
        if (value == null) return null;
        if (!(value instanceof String text)) throw EafException.invalid(name + " 必须是 UUID。");
        try { return UUID.fromString(text); }
        catch (IllegalArgumentException malformed) { throw EafException.invalid(name + " 必须是 UUID。"); }
    }

    private static WorkspaceKind enumKind(String value) {
        try { return WorkspaceKind.valueOf(value); }
        catch (IllegalArgumentException malformed) { throw EafException.invalid("kind 必须是 PERSONAL、PROJECT、DEPARTMENT 或 ENTERPRISE。"); }
    }

    private static String single(MultiValueMap<String, String> query, String name) {
        var values = query.get(name);
        if (values == null) return null;
        if (values.size() != 1) throw EafException.invalid(name + " 只能提供一次。");
        return values.getFirst();
    }
}
