package io.eaf.agentprotocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.capability.api.CapabilityService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.skill.api.SkillPackageService;
import io.eaf.skill.api.SkillPackageService.AssetProjection;
import io.eaf.skill.api.SkillPackageService.ExportSource;
import io.modelcontextprotocol.server.McpServerFeatures.SyncResourceSpecification;
import io.modelcontextprotocol.server.McpServerFeatures.SyncResourceTemplateSpecification;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceResult;
import io.modelcontextprotocol.spec.McpSchema.Resource;
import io.modelcontextprotocol.spec.McpSchema.ResourceTemplate;
import io.modelcontextprotocol.spec.McpSchema.TextResourceContents;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** MCP 固定 Resources 只暴露 P25 声明；资产正文在读取时按当前身份生成。 */
@Component
@ConditionalOnProperty(name = "eaf.agent-protocol.declarative-resources-enabled", havingValue = "true")
public final class McpDeclarativeResources {
    static final String INDEX_URI = "eaf://declarations/index";
    private static final String INDEX_META = "io.eaf/declarations";
    private static final String TEMPLATE_PREFIX = "eaf://declarations/workspaces/{workspaceId}/capabilities/{capabilityId}/versions/{version}/packages/{packageHash}/";
    private static final List<String> FILES = List.of("SKILL.md", "eaf-package.json", "references/capability.json",
            "references/skill.json", "references/usage.md");

    private final McpTaskTools tools;
    private final PlatformAccessApplicationService access;
    private final CapabilityService capabilities;
    private final SkillPackageService packages;
    private final ObjectMapper json;

    public McpDeclarativeResources(McpTaskTools tools, PlatformAccessApplicationService access,
                                   CapabilityService capabilities, SkillPackageService packages, ObjectMapper json) {
        this.tools = tools;
        this.access = access;
        this.capabilities = capabilities;
        this.packages = packages;
        this.json = json;
    }

    List<SyncResourceSpecification> resourceSpecifications() {
        var resource = Resource.builder(INDEX_URI, "eaf-declarations-index")
                .description("读取时指定 workspaceId；内容按当前身份生成。")
                .mimeType("application/json").build();
        return List.of(new SyncResourceSpecification(resource, this::readIndex));
    }

    List<SyncResourceTemplateSpecification> templateSpecifications() {
        return FILES.stream().map(path -> {
            var mimeType = mimeType(path);
            var template = ResourceTemplate.builder(TEMPLATE_PREFIX + path, "eaf-declaration-" + path.replace('/', '-'))
                    .description("读取指定发布版本的固定声明文件；读取时重新检查身份、状态和摘要。")
                    .mimeType(mimeType).build();
            return new SyncResourceTemplateSpecification(template, this::readFile);
        }).toList();
    }

    private ReadResourceResult readIndex(McpSyncServerExchange exchange, ReadResourceRequest request) {
        if (!INDEX_URI.equals(request.uri())) throw EafException.invalid("声明目录 URI 无效。");
        var args = indexArguments(request.meta());
        var actor = tools.actor(exchange);
        var page = access.search(actor, args.workspaceId(), new PlatformAccessApplicationService.DiscoveryRequest(
                "CAPABILITY", args.query(), 10, args.offset()));
        var items = new ArrayList<IndexItem>();
        for (var found : page.items()) {
            try {
                var source = source(actor, args.workspaceId(), found.id(), found.version());
                var declaration = packages.describe(actor, args.workspaceId(), source);
                var cap = source.capability();
                var skill = source.skill();
                var files = declaration.files().stream().map(info -> new IndexFile(info.path(),
                        fileUri(args.workspaceId(), cap.id(), cap.version(), declaration.packageHash(), info.path()),
                        info.mimeType(), info.sizeBytes(), info.sha256())).toList();
                items.add(new IndexItem(cap.name(), cap.description(), reference(cap), reference(skill),
                        declaration.packageHash(), fileUri(args.workspaceId(), cap.id(), cap.version(),
                        declaration.packageHash(), "SKILL.md"), files));
            } catch (EafException unavailable) {
                // 目录只省略当前不可见或不符合 P25 固定声明合同的项；其他故障应暴露给调用方。
                if (!canOmitFromIndex(unavailable)) throw unavailable;
            }
        }
        var result = new DeclarationIndex("EAF_DECLARATIVE_RESOURCE_INDEX_V1", SkillPackageService.PROFILE,
                args.workspaceId().toString(), items, page.nextOffset());
        return text(request.uri(), "application/json", write(result), Map.of(INDEX_META,
                Map.of("workspaceId", args.workspaceId().toString(), "itemCount", items.size())));
    }

    private ReadResourceResult readFile(McpSyncServerExchange exchange, ReadResourceRequest request) {
        var uri = parseFileUri(request.uri());
        var actor = tools.actor(exchange);
        var source = source(actor, uri.workspaceId(), uri.capabilityId(), uri.version());
        var file = packages.readDeclarationFile(actor, uri.workspaceId(), source, uri.packageHash(), uri.path());
        var info = file.info();
        return text(request.uri(), info.mimeType(), new String(file.content(), java.nio.charset.StandardCharsets.UTF_8),
                Map.of(INDEX_META, Map.of("packageHash", uri.packageHash(), "path", info.path(),
                        "sizeBytes", info.sizeBytes(), "sha256", info.sha256())));
    }

    private ExportSource source(ActorContext actor, UUID workspaceId, UUID capabilityId, String version) {
        var current = capabilities.requirePublished(actor, workspaceId, capabilityId, version);
        var listed = access.getCapability(actor, workspaceId, capabilityId, version);
        if (!current.contentHash().equals(listed.contentHash())) throw EafException.notFound();
        var capability = new PlatformAccessApplicationService.CapabilityUsageDetail(listed.kind(), listed.id(),
                listed.name(), listed.description(), listed.version(), listed.contentHash(),
                new PlatformAccessApplicationService.SkillVersionReference(current.skillId(), current.skillVersion(),
                        current.skillContentHash()), listed.taskInput(), listed.usageNotes());
        var skill = access.getSkill(actor, workspaceId, capability.skill().id(), capability.skill().version());
        if (!skill.contentHash().equals(capability.skill().contentHash())) throw EafException.notFound();
        return new ExportSource(workspaceId,
                new AssetProjection(capability.id(), capability.name(), capability.description(), capability.version(),
                        capability.contentHash(), json.valueToTree(capability)),
                new AssetProjection(skill.id(), skill.name(), skill.description(), skill.version(), skill.contentHash(),
                        json.valueToTree(skill)));
    }

    private IndexArguments indexArguments(Map<String, Object> meta) {
        var raw = meta == null ? null : meta.get(INDEX_META);
        if (!(raw instanceof Map<?, ?> values) || values.keySet().stream()
                .anyMatch(key -> !(key instanceof String name) || !Set.of("workspaceId", "query", "offset").contains(name)))
            throw EafException.invalid("声明目录读取需要 io.eaf/declarations 中的 workspaceId、query 和 offset 参数。");
        var workspace = values.get("workspaceId");
        if (!(workspace instanceof String text)) throw EafException.invalid("workspaceId 必须是 UUID。");
        final UUID workspaceId;
        try {
            workspaceId = UUID.fromString(text);
            if (!workspaceId.toString().equals(text)) throw new IllegalArgumentException();
        } catch (IllegalArgumentException malformed) {
            throw EafException.invalid("workspaceId 必须是标准 UUID。");
        }
        var query = values.get("query");
        if (query != null && !(query instanceof String)) throw EafException.invalid("query 必须是字符串。");
        var queryText = (String) query;
        if (queryText != null && queryText.length() > 200) throw EafException.invalid("query 最多 200 个字符。");
        return new IndexArguments(workspaceId, queryText,
                PlatformAccessApplicationService.optionalInteger(castStringMap(values), "offset"));
    }

    private Map<String, Object> castStringMap(Map<?, ?> values) {
        var result = new java.util.HashMap<String, Object>();
        values.forEach((key, value) -> result.put((String) key, value));
        return result;
    }

    private FileUri parseFileUri(String value) {
        try {
            if (value == null || value.indexOf('%') >= 0 || value.indexOf('\\') >= 0) throw new IllegalArgumentException();
            var uri = URI.create(value);
            if (!"eaf".equals(uri.getScheme()) || !"declarations".equals(uri.getHost())
                    || uri.getPort() != -1 || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                    || uri.getRawFragment() != null || !value.startsWith("eaf://declarations/"))
                throw new IllegalArgumentException();
            var segments = uri.getRawPath().substring(1).split("/", -1);
            if (segments.length < 9 || !List.of(segments[0], segments[2], segments[4], segments[6])
                    .equals(List.of("workspaces", "capabilities", "versions", "packages")))
                throw new IllegalArgumentException();
            var path = String.join("/", java.util.Arrays.copyOfRange(segments, 8, segments.length));
            if (!FILES.contains(path) || !uuid(segments[1]).toString().equals(segments[1])
                    || !uuid(segments[3]).toString().equals(segments[3])
                    || !segments[5].matches("[A-Za-z0-9][A-Za-z0-9.+_-]{0,39}")
                    || !segments[7].matches("[0-9a-f]{64}")) throw new IllegalArgumentException();
            return new FileUri(uuid(segments[1]), uuid(segments[3]), segments[5], segments[7], path);
        } catch (IllegalArgumentException malformed) {
            throw EafException.invalid("声明文件 URI 不符合固定路径格式。");
        }
    }

    private UUID uuid(String value) {
        return UUID.fromString(value);
    }

    private String fileUri(UUID workspaceId, UUID capabilityId, String version, String packageHash, String path) {
        return "eaf://declarations/workspaces/" + workspaceId + "/capabilities/" + capabilityId
                + "/versions/" + version + "/packages/" + packageHash + "/" + path;
    }

    private ReadResourceResult text(String uri, String mimeType, String body, Map<String, Object> meta) {
        return new ReadResourceResult(List.of(new TextResourceContents(uri, mimeType, body, meta)));
    }

    private String write(Object value) {
        try { return json.writeValueAsString(value); }
        catch (JsonProcessingException impossible) { throw new IllegalStateException("声明目录无法序列化。", impossible); }
    }

    private static boolean canOmitFromIndex(EafException error) {
        return error.status() == 403 || error.status() == 404 || error.code().startsWith("PACKAGE_");
    }

    private static String mimeType(String path) {
        return path.endsWith(".md") ? "text/markdown" : "application/json";
    }

    private static AssetReference reference(AssetProjection asset) {
        return new AssetReference(asset.id().toString(), asset.version(), asset.contentHash());
    }

    private record IndexArguments(UUID workspaceId, String query, Integer offset) { }
    private record FileUri(UUID workspaceId, UUID capabilityId, String version, String packageHash, String path) { }
    private record AssetReference(String id, String version, String contentHash) { }
    private record IndexFile(String path, String uri, String mimeType, int sizeBytes, String sha256) { }
    private record IndexItem(String name, String description, AssetReference capability, AssetReference skill,
                             String packageHash, String entryUri, List<IndexFile> files) { }
    private record DeclarationIndex(String format, String profile, String workspaceId,
                                   List<IndexItem> items, Integer nextOffset) { }
}
