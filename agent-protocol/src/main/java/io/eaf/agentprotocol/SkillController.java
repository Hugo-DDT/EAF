package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.EafException;
import io.eaf.skill.api.CreateSkillCommand;
import io.eaf.skill.api.CreateSkillVersionCommand;
import io.eaf.skill.api.SkillDefinition;
import io.eaf.skill.api.SkillService;
import io.eaf.skill.api.SkillToolDependency;
import io.eaf.skill.api.SkillToolReference;
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
@RequestMapping("/api/v1/workspaces/{workspaceId}/skills")
public class SkillController {
    private final SkillService skills;
    private final ObjectMapper json;

    public SkillController(SkillService skills, ObjectMapper json) {
        this.skills = skills;
        this.json = json;
    }

    @GetMapping
    SkillList list(@PathVariable UUID workspaceId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        return new SkillList(skills.list(actor, workspaceId).stream().map(this::view).toList(), null);
    }

    // 只接受 Skill 自身内容；Owner、状态和权限字段均由服务端决定。
    @PostMapping
    ResponseEntity<SkillView> create(@PathVariable UUID workspaceId, @RequestBody Map<String, Object> rawBody,
                                     Authentication authentication) {
        var body = convert(rawBody, CreateBody.class,
                Set.of("name", "description", "version", "inputSchema", "outputSchema", "promptId", "promptVersion", "toolDependencies", "evaluationRef"));
        var actor = ApiSupport.actor(authentication);
        var definition = skills.create(new CreateSkillCommand(actor, workspaceId, body.name(), body.description(),
                version(body.version(), body.inputSchema(), body.outputSchema(), body.promptId(), body.promptVersion(),
                        body.toolDependencies(), body.evaluationRef())));
        return ResponseEntity.status(201).body(view(definition));
    }

    @PostMapping("/{skillId}/versions")
    SkillView addVersion(@PathVariable UUID workspaceId, @PathVariable UUID skillId,
                         @RequestBody Map<String, Object> rawBody, Authentication authentication) {
        var body = convert(rawBody, VersionBody.class,
                Set.of("version", "inputSchema", "outputSchema", "promptId", "promptVersion", "toolDependencies", "evaluationRef"));
        return view(skills.addVersion(ApiSupport.actor(authentication), workspaceId, skillId,
                version(body.version(), body.inputSchema(), body.outputSchema(), body.promptId(), body.promptVersion(),
                        body.toolDependencies(), body.evaluationRef())));
    }

    @GetMapping("/{skillId}/versions/{version}")
    SkillView get(@PathVariable UUID workspaceId, @PathVariable UUID skillId, @PathVariable String version,
                  Authentication authentication) {
        return view(skills.get(ApiSupport.actor(authentication), workspaceId, skillId, version));
    }

    @PostMapping("/{skillId}/versions/{version}/publish")
    SkillView publish(@PathVariable UUID workspaceId, @PathVariable UUID skillId, @PathVariable String version,
                      @RequestParam long expectedVersion, Authentication authentication) {
        return view(skills.publish(ApiSupport.actor(authentication), workspaceId, skillId, version, expectedVersion));
    }

    @PostMapping("/{skillId}/versions/{version}/revoke")
    SkillView revoke(@PathVariable UUID workspaceId, @PathVariable UUID skillId, @PathVariable String version,
                     @RequestParam long expectedVersion, Authentication authentication) {
        return view(skills.revoke(ApiSupport.actor(authentication), workspaceId, skillId, version, expectedVersion));
    }

    private <T> T convert(Map<String, Object> rawBody, Class<T> type, Set<String> allowed) {
        if (rawBody == null) throw EafException.invalid("请求体不能为空。");
        if (rawBody.keySet().stream().anyMatch(key -> !allowed.contains(key))) throw EafException.invalid("请求包含未允许字段。");
        try { return json.convertValue(rawBody, type); }
        catch (IllegalArgumentException e) { throw EafException.invalid("请求字段类型无效。"); }
    }

    private CreateSkillVersionCommand version(String version, JsonNode inputSchema, JsonNode outputSchema,
                                              UUID promptId, String promptVersion,
                                              List<SkillToolReference> toolDependencies, String evaluationRef) {
        return new CreateSkillVersionCommand(version, tree(inputSchema), tree(outputSchema), promptId,
                promptVersion, toolDependencies, evaluationRef);
    }

    private String tree(JsonNode value) { return value == null ? null : value.toString(); }

    private SkillView view(SkillDefinition skill) {
        try {
            return new SkillView(skill.id(), skill.ownerId(), skill.name(), skill.description(), skill.version(),
                    json.readTree(skill.inputSchema()), json.readTree(skill.outputSchema()), skill.promptId(),
                    skill.promptVersion(), skill.toolDependencies(), skill.evaluationRef(), skill.status(),
                    skill.rowVersion(), skill.contentHash());
        } catch (Exception e) {
            throw new IllegalStateException("已保存的 Skill Schema 无法解析。", e);
        }
    }

    record SkillList(List<SkillView> items, String nextCursor) { }
    record CreateBody(String name, String description, String version, JsonNode inputSchema, JsonNode outputSchema,
                      UUID promptId, String promptVersion, List<SkillToolReference> toolDependencies, String evaluationRef) { }
    record VersionBody(String version, JsonNode inputSchema, JsonNode outputSchema, UUID promptId, String promptVersion,
                       List<SkillToolReference> toolDependencies, String evaluationRef) { }
    record SkillView(UUID id, UUID ownerId, String name, String description, String version, JsonNode inputSchema,
                     JsonNode outputSchema, UUID promptId, String promptVersion, List<SkillToolDependency> toolDependencies,
                     String evaluationRef, String status, long rowVersion, String contentHash) { }
}
// REST 适配只转换 JSON；发布、依赖和权限检查均由 Skill 模块负责。
