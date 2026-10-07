package io.eaf.skill.infrastructure;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.skill.api.SkillPackageService;
import io.eaf.skill.api.SkillPackageService.AssetProjection;
import io.eaf.skill.api.SkillPackageService.ExportSource;
import io.eaf.skill.api.SkillPackageService.PackageSource;
import io.eaf.skill.api.SkillPackageService.PackageValidation;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/** 固定 profile 的 ZIP/JSON 编解码；所有包内容只当 UTF-8 声明数据处理。 */
final class DeclarativeCapabilityPackageCodec {
    static final int MAX_ZIP_BYTES = 1_048_576;
    static final int MAX_TOTAL_BYTES = 1_048_576;
    static final int MAX_FILE_BYTES = 262_144;
    private static final String FORMAT_VERSION = "1";
    private static final String TEMPLATE_VERSION = "1";
    private static final String MANIFEST = "eaf-package.json";
    private static final List<String> CONTENT_PATHS = List.of(
            "SKILL.md", "references/capability.json", "references/skill.json", "references/usage.md");
    private static final List<String> ZIP_ORDER = List.of(
            "SKILL.md", MANIFEST, "references/capability.json", "references/skill.json", "references/usage.md");
    private static final List<String> LIMITATIONS = List.of(
            "来源未签名；packageHash 只校验内容一致性。",
            "导入不创建、发布或执行 EAF Skill；运行前必须通过原服务在线复核精确版本与权限。",
            "包不包含服务地址或凭据，归档不能收回已复制到包外的副本。");
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);

    byte[] export(ExportSource source) {
        if (source == null || source.sourceWorkspaceId() == null || source.capability() == null || source.skill() == null)
            throw EafException.invalid("声明包导出来源不完整。");
        var capability = source.capability();
        var skill = source.skill();
        requireAsset(capability, "CAPABILITY");
        requireAsset(skill, "SKILL");
        if (!capability.contentHash().matches("[0-9a-f]{64}") || !skill.contentHash().matches("[0-9a-f]{64}"))
            throw unsupported("Capability 或 Skill 缺少可核验的 contentHash。");
        var capUsage = normalize(capability.usageProjection());
        var skillUsage = normalize(skill.usageProjection());
        validateCapabilityProjection(capUsage, capability);
        validateSkillProjection(skillUsage, skill);
        var skillReference = capUsage.path("skill");
        if (!skillReference.path("id").asText().equals(skill.id().toString())
                || !skillReference.path("version").asText().equals(skill.version())
                || !skillReference.path("contentHash").asText().equals(skill.contentHash()))
            throw EafException.conflict("PACKAGE_REFERENCE_INVALID", "Capability 固定绑定的 Skill 版本或摘要已不一致。");

        var capBytes = write(capUsage);
        var skillBytes = write(skillUsage);
        var root = rootName(capability.id());
        var files = new TreeMap<String, String>();
        files.put("references/capability.json", text(capBytes));
        files.put("references/skill.json", text(skillBytes));
        files.put("references/usage.md", renderUsage(source.sourceWorkspaceId(), capability, skill));
        files.put("SKILL.md", renderSkill(root, capability, skill));
        var fileInfos = CONTENT_PATHS.stream().map(path -> fileInfo(path, bytes(files.get(path)))).toList();
        var base = manifestBase(source.sourceWorkspaceId(), capability, skill, fileInfos);
        var packageHash = Hashing.sha256(text(write(base)));
        var manifest = (ObjectNode) base.deepCopy();
        manifest.put("packageHash", packageHash);
        files.put(MANIFEST, text(write(normalize(manifest))));
        return zip(root, files);
    }

    Decoded validate(byte[] zip) {
        if (zip == null || zip.length == 0) throw EafException.invalid("声明包 ZIP 不能为空。");
        if (zip.length > MAX_ZIP_BYTES) throw packageError("PACKAGE_TOO_LARGE", "声明包超过 1 MiB 限制。");
        try {
            var root = (String) null;
            var files = new TreeMap<String, String>();
            var seen = new HashSet<String>();
            var seenFolded = new HashSet<String>();
            var total = 0;
            var entries = 0;
            try (var input = new ZipInputStream(new ByteArrayInputStream(zip), StandardCharsets.UTF_8)) {
                ZipEntry entry;
                while ((entry = input.getNextEntry()) != null) {
                    if (++entries > 7) throw packageError("PACKAGE_FORMAT_UNSUPPORTED", "声明包条目数量超限。");
                    var name = entry.getName();
                    if (name == null || name.isBlank() || name.indexOf('\0') >= 0 || name.indexOf('\\') >= 0
                            || name.startsWith("/") || name.matches("^[A-Za-z]:.*"))
                        throw packageError("PACKAGE_PATH_INVALID", "声明包包含无效路径。");
                    var lower = name.toLowerCase(Locale.ROOT);
                    if (!seen.add(name) || !seenFolded.add(lower))
                        throw packageError("PACKAGE_PATH_INVALID", "声明包包含重复或大小写冲突路径。");
                    var segments = name.split("/", -1);
                    for (var segment : segments) {
                        if (segment.equals("..") || segment.equals(".") || segment.isEmpty() && !entry.isDirectory())
                            throw packageError("PACKAGE_PATH_INVALID", "声明包包含路径穿越或空路径段。");
                    }
                    if (root == null) {
                        if (segments.length < 2 || !segments[0].matches("eaf-[0-9a-f]{32}"))
                            throw packageError("PACKAGE_FORMAT_UNSUPPORTED", "声明包根目录不符合 EAF profile。");
                        root = segments[0];
                    }
                    if (!segments[0].equals(root)) throw packageError("PACKAGE_PATH_INVALID", "声明包只能包含一个根目录。");
                    var relative = name.substring(root.length() + 1);
                    if (entry.isDirectory()) {
                        if (!Set.of("", "references/").contains(relative))
                            throw packageError("PACKAGE_PATH_INVALID", "声明包包含未允许的目录项。");
                        if (input.readNBytes(1).length != 0)
                            throw packageError("PACKAGE_FORMAT_UNSUPPORTED", "声明包目录项不能包含数据。");
                    } else {
                        if (!CONTENT_PATHS.contains(relative) && !MANIFEST.equals(relative))
                            throw packageError("PACKAGE_FORMAT_UNSUPPORTED", "声明包包含未允许的文件。");
                        var content = input.readNBytes(MAX_FILE_BYTES + 1);
                        if (content.length > MAX_FILE_BYTES)
                            throw packageError("PACKAGE_TOO_LARGE", "声明包单文件超过 256 KiB 限制。");
                        total += content.length;
                        if (total > MAX_TOTAL_BYTES) throw packageError("PACKAGE_TOO_LARGE", "声明包解压内容超过 1 MiB 限制。");
                        files.put(relative, decode(content));
                    }
                    input.closeEntry();
                }
            }
            if (files.size() != 5 || !files.keySet().containsAll(CONTENT_PATHS) || !files.containsKey(MANIFEST))
                throw packageError("PACKAGE_FORMAT_UNSUPPORTED", "声明包必须恰好包含五个固定文件。");
            var manifestBytes = bytes(files.get(MANIFEST));
            var manifest = object(readJson(manifestBytes), "manifest");
            requireFields(manifest, Set.of("profile", "formatVersion", "templateVersion", "sourceWorkspaceId",
                    "capability", "skill", "files", "packageHash"), "manifest");
            if (!SkillPackageService.PROFILE.equals(string(manifest, "profile"))
                    || !FORMAT_VERSION.equals(string(manifest, "formatVersion"))
                    || !TEMPLATE_VERSION.equals(string(manifest, "templateVersion")))
                throw unsupported("不支持此 EAF 声明包版本。");
            var sourceWorkspaceId = uuid(string(manifest, "sourceWorkspaceId"), "sourceWorkspaceId");
            var capability = object(manifest.path("capability"), "capability");
            var skill = object(manifest.path("skill"), "skill");
            requireFields(capability, Set.of("id", "version", "contentHash"), "manifest.capability");
            requireFields(skill, Set.of("id", "version", "contentHash"), "manifest.skill");
            var capId = uuid(string(capability, "id"), "capability.id");
            var skillId = uuid(string(skill, "id"), "skill.id");
            var capVersion = version(string(capability, "version"), "capability.version");
            var skillVersion = version(string(skill, "version"), "skill.version");
            var capHash = hash(string(capability, "contentHash"), "capability.contentHash");
            var skillHash = hash(string(skill, "contentHash"), "skill.contentHash");
            if (!root.equals(rootName(capId))) throw packageError("PACKAGE_PATH_INVALID", "ZIP 根目录与 manifest Capability 不匹配。");

            var fileMetadata = validateFileMetadata(manifest.path("files"), files);
            var canonicalBase = (ObjectNode) manifest.deepCopy();
            canonicalBase.remove("packageHash");
            var expectedHash = Hashing.sha256(text(write(normalize(canonicalBase))));
            if (!expectedHash.equals(hash(string(manifest, "packageHash"), "packageHash")))
                throw packageError("PACKAGE_CONTENT_MISMATCH", "manifest packageHash 校验失败。");
            if (!java.util.Arrays.equals(manifestBytes, write(normalize(manifest))))
                throw packageError("PACKAGE_CONTENT_MISMATCH", "manifest JSON 不符合规范化格式。");

            var capabilityProjection = object(readJson(bytes(files.get("references/capability.json"))), "capability.json");
            var skillProjection = object(readJson(bytes(files.get("references/skill.json"))), "skill.json");
            validateImportedCapability(capabilityProjection, capId, capVersion, capHash, skillId, skillVersion, skillHash);
            validateImportedSkill(skillProjection, skillId, skillVersion, skillHash);
            if (!java.util.Arrays.equals(bytes(files.get("references/capability.json")), write(normalize(capabilityProjection)))
                    || !java.util.Arrays.equals(bytes(files.get("references/skill.json")), write(normalize(skillProjection))))
                throw packageError("PACKAGE_CONTENT_MISMATCH", "资源 JSON 不符合规范化格式。");
            var capabilityAsset = new AssetProjection(capId, string(capabilityProjection, "name"),
                    string(capabilityProjection, "description"), capVersion, capHash, capabilityProjection);
            var skillAsset = new AssetProjection(skillId, string(skillProjection, "name"),
                    string(skillProjection, "description"), skillVersion, skillHash, skillProjection);
            if (!files.get("SKILL.md").equals(renderSkill(root, capabilityAsset, skillAsset))
                    || !files.get("references/usage.md").equals(renderUsage(sourceWorkspaceId, capabilityAsset, skillAsset)))
                throw packageError("PACKAGE_CONTENT_MISMATCH", "SKILL.md 或 usage.md 不符合固定模板。");
            var source = new PackageSource(sourceWorkspaceId, capId, capVersion, capHash, skillId, skillVersion, skillHash);
            var name = string(capabilityProjection, "name");
            var result = new PackageValidation(SkillPackageService.PROFILE, FORMAT_VERSION, expectedHash, name, source,
                    List.copyOf(fileMetadata), "VALID", false, "UNVERIFIED", LIMITATIONS);
            return new Decoded(result, text(write(normalize(manifest))), Map.copyOf(files), root);
        } catch (EafException known) {
            throw known;
        } catch (Exception invalid) {
            throw packageError("PACKAGE_FORMAT_UNSUPPORTED", "声明包 ZIP 或固定文件格式无效。");
        }
    }

    byte[] download(Decoded decoded) {
        return zip(decoded.root(), new TreeMap<>(decoded.files()));
    }

    byte[] download(UUID capabilityId, Map<String, String> files) {
        return zip(rootName(capabilityId), new TreeMap<>(files));
    }

    record Decoded(PackageValidation validation, String manifestJson, Map<String, String> files, String root) { }

    private List<String> validateFileMetadata(JsonNode value, Map<String, String> files) throws IOException {
        if (!value.isArray() || value.size() != 4) throw packageError("PACKAGE_FORMAT_UNSUPPORTED", "manifest.files 必须列出四个资源文件。");
        var seen = new ArrayList<String>();
        for (var item : value) {
            var info = object(item, "manifest.files[]");
            requireFields(info, Set.of("path", "bytes", "sha256"), "manifest.files[]");
            var path = string(info, "path");
            if (!CONTENT_PATHS.contains(path) || !files.containsKey(path))
                throw packageError("PACKAGE_REFERENCE_INVALID", "manifest 引用了未允许或缺失的文件。");
            var content = bytes(files.get(path));
            if (!info.path("bytes").canConvertToInt() || info.path("bytes").intValue() != content.length
                    || !hash(string(info, "sha256"), "files.sha256").equals(Hashing.sha256(text(content))))
                throw packageError("PACKAGE_CONTENT_MISMATCH", "声明包文件长度或摘要不匹配。");
            seen.add(path);
        }
        var sorted = seen.stream().sorted().toList();
        if (seen.size() != 4 || !seen.equals(sorted) || !seen.equals(CONTENT_PATHS.stream().sorted().toList()))
            throw packageError("PACKAGE_FORMAT_UNSUPPORTED", "manifest.files 必须按路径排序并恰好包含固定资源。");
        return sorted;
    }

    private ObjectNode manifestBase(UUID sourceWorkspace, AssetProjection capability, AssetProjection skill,
                                    List<ObjectNode> fileInfo) {
        var manifest = JSON.createObjectNode();
        manifest.put("profile", SkillPackageService.PROFILE);
        manifest.put("formatVersion", FORMAT_VERSION);
        manifest.put("templateVersion", TEMPLATE_VERSION);
        manifest.put("sourceWorkspaceId", sourceWorkspace.toString());
        var cap = JSON.createObjectNode();
        cap.put("id", capability.id().toString());
        cap.put("version", capability.version());
        cap.put("contentHash", capability.contentHash());
        manifest.set("capability", cap);
        var skillRef = JSON.createObjectNode();
        skillRef.put("id", skill.id().toString());
        skillRef.put("version", skill.version());
        skillRef.put("contentHash", skill.contentHash());
        manifest.set("skill", skillRef);
        var files = JSON.createArrayNode();
        fileInfo.stream().sorted(Comparator.comparing(info -> info.path("path").asText())).forEach(files::add);
        manifest.set("files", files);
        return (ObjectNode) normalize(manifest);
    }

    private ObjectNode fileInfo(String path, byte[] content) {
        var info = JSON.createObjectNode();
        info.put("path", path);
        info.put("bytes", content.length);
        info.put("sha256", Hashing.sha256(text(content)));
        return info;
    }

    private String renderSkill(String root, AssetProjection capability, AssetProjection skill) {
        var description = capability.description();
        if (description == null || description.isBlank() || description.length() > 1_024)
            throw unsupported("Capability 描述为空或超过 Agent Skills 的 1,024 字符限制。");
        return "---\nname: " + root + "\ndescription: " + quote(description)
                + "\ncompatibility: \"Requires an EAF MCP server configured by the user.\""
                + "\nmetadata:\n  eaf-profile: \"" + SkillPackageService.PROFILE + "\""
                + "\n  capability-id: \"" + capability.id() + "\""
                + "\n  capability-version: \"" + capability.version() + "\""
                + "\n  skill-id: \"" + skill.id() + "\""
                + "\n  skill-version: \"" + skill.version() + "\"\n---\n\n"
                + description + "\n\nRead `references/capability.json` and `references/skill.json` for the exact published contracts.\n"
                + "Follow `references/usage.md` to configure and verify the original EAF service before creating a task.\n";
    }

    private String renderUsage(UUID workspace, AssetProjection capability, AssetProjection skill) {
        return "# EAF capability package usage\n\n"
                + "This package is an unsigned, read-only reference. Configure the original EAF service URL, credentials, and Workspace in trusted client settings; this package contains none of them.\n\n"
                + "1. Negotiate MCP protocol `2025-11-25` and confirm the existing tools with `tools/list`.\n"
                + "2. Read Capability `" + capability.id() + "` version `" + capability.version() + "` and Skill `"
                + skill.id() + "` version `" + skill.version() + "` in source Workspace `" + workspace + "`.\n"
                + "3. Stop if either asset is unavailable, withdrawn, bound differently, or has a missing or different contentHash. Never substitute `latest`, a same-name asset, or another Workspace.\n"
                + "4. For user-provided input, call `eaf.tasks.create` with the exact Capability ID/version, an input string of at most 8,000 characters, and a stable idempotencyKey. Keep retries on that key.\n"
                + "5. Query the returned Task ID with bounded polling. If creation is uncertain, replay the same key; if stopping a known Task, call `eaf.tasks.cancel` with its latest expectedVersion.\n\n"
                + "Skill input/output schemas describe asset data; they are not the Task request envelope. This package does not authorize tools or perform confirmation, approval, registration, or other dedicated business actions.\n";
    }

    private byte[] zip(String root, Map<String, String> files) {
        try {
            var out = new ByteArrayOutputStream();
            try (var zip = new ZipOutputStream(out, StandardCharsets.UTF_8)) {
                for (var path : ZIP_ORDER) {
                    var content = files.get(path);
                    if (content == null) throw new IllegalStateException("missing package file");
                    var entry = new ZipEntry(root + "/" + path);
                    entry.setTime(0L);
                    zip.putNextEntry(entry);
                    zip.write(bytes(content));
                    zip.closeEntry();
                }
            }
            var result = out.toByteArray();
            if (result.length > MAX_ZIP_BYTES) throw packageError("PACKAGE_TOO_LARGE", "生成的声明包超过 1 MiB 限制。");
            return result;
        } catch (EafException known) {
            throw known;
        } catch (IOException impossible) {
            throw new IllegalStateException("声明包 ZIP 无法生成。", impossible);
        }
    }

    private void requireAsset(AssetProjection asset, String kind) {
        if (asset.id() == null || asset.name() == null || asset.name().isBlank() || asset.description() == null
                || asset.version() == null || !asset.version().matches("[A-Za-z0-9][A-Za-z0-9.+_-]{0,39}")
                || asset.usageProjection() == null || !asset.usageProjection().isObject())
            throw unsupported(kind + " 使用投影缺少声明包所需字段。");
    }

    private void validateCapabilityProjection(JsonNode node, AssetProjection asset) {
        requireFields(node, Set.of("kind", "id", "name", "description", "version", "contentHash", "skill", "taskInput", "usageNotes"), "capability");
        requireMetadata(node, "CAPABILITY", asset);
        var skillRef = object(node.path("skill"), "capability.skill");
        requireFields(skillRef, Set.of("id", "version", "contentHash"), "capability.skill");
        uuid(string(skillRef, "id"), "capability.skill.id");
        version(string(skillRef, "version"), "capability.skill.version");
        hash(string(skillRef, "contentHash"), "capability.skill.contentHash");
        var taskInput = object(node.path("taskInput"), "capability.taskInput");
        requireFields(taskInput, Set.of("type", "maxLength"), "capability.taskInput");
        if (!"string".equals(string(taskInput, "type")) || taskInput.path("maxLength").asInt(-1) != 8_000)
            throw unsupported("Capability Task 输入合同不符合固定 profile。");
        stringArray(node.path("usageNotes"), "capability.usageNotes", 20);
    }

    private void validateSkillProjection(JsonNode node, AssetProjection asset) {
        requireFields(node, Set.of("kind", "id", "name", "description", "version", "contentHash", "inputSchema", "outputSchema", "toolDependencies", "usageNotes"), "skill");
        requireMetadata(node, "SKILL", asset);
        validateSchema(node.path("inputSchema"), "skill.inputSchema");
        validateSchema(node.path("outputSchema"), "skill.outputSchema");
        var dependencies = node.path("toolDependencies");
        if (!dependencies.isArray() || dependencies.size() > 16) throw unsupported("Skill Tool 依赖数量不符合固定 profile。");
        var names = new HashSet<String>();
        for (var dependency : dependencies) {
            var object = object(dependency, "skill.toolDependencies[]");
            requireFields(object, Set.of("name", "version"), "skill.toolDependencies[]");
            var name = string(object, "name");
            if (!name.matches("[a-z0-9]+(?:[.-][a-z0-9]+)*") || !names.add(name)) throw unsupported("Skill Tool 依赖名称无效或重复。");
            version(string(object, "version"), "skill.toolDependencies.version");
        }
        stringArray(node.path("usageNotes"), "skill.usageNotes", 20);
    }

    private void validateImportedCapability(JsonNode node, UUID capId, String capVersion, String capHash,
                                            UUID skillId, String skillVersion, String skillHash) {
        requireFields(node, Set.of("kind", "id", "name", "description", "version", "contentHash", "skill", "taskInput", "usageNotes"), "capability");
        var cap = new AssetProjection(capId, string(node, "name"), string(node, "description"), capVersion, capHash, node);
        validateCapabilityProjection(node, cap);
        var reference = node.path("skill");
        if (!skillId.toString().equals(string(reference, "id")) || !skillVersion.equals(string(reference, "version"))
                || !skillHash.equals(string(reference, "contentHash")))
            throw packageError("PACKAGE_REFERENCE_INVALID", "Capability 与 Skill 版本绑定不一致。");
    }

    private void validateImportedSkill(JsonNode node, UUID skillId, String version, String contentHash) {
        requireFields(node, Set.of("kind", "id", "name", "description", "version", "contentHash", "inputSchema", "outputSchema", "toolDependencies", "usageNotes"), "skill");
        validateSkillProjection(node, new AssetProjection(skillId, string(node, "name"), string(node, "description"), version, contentHash, node));
    }

    private void requireMetadata(JsonNode node, String kind, AssetProjection asset) {
        if (!kind.equals(string(node, "kind")) || !asset.id().toString().equals(string(node, "id"))
                || !asset.name().equals(string(node, "name")) || !asset.description().equals(string(node, "description"))
                || !asset.version().equals(string(node, "version")) || !asset.contentHash().equals(hash(string(node, "contentHash"), "contentHash")))
            throw packageError("PACKAGE_REFERENCE_INVALID", "资源摘要与 manifest 引用不一致。");
    }

    private void validateSchema(JsonNode schema, String field) {
        if (!schema.isObject() || !"object".equals(schema.path("type").asText())
                || !schema.path("properties").isObject() || schema.path("properties").isEmpty()
                || write(schema).length > 16_384 || depth(schema, 0) > 32)
            throw packageError("PACKAGE_REFERENCE_INVALID", field + " 必须是受限的 JSON object Schema。");
        var required = schema.path("required");
        if (!required.isMissingNode() && !required.isArray()) throw packageError("PACKAGE_REFERENCE_INVALID", field + ".required 必须是数组。");
        if (required.isArray()) {
            var seen = new HashSet<String>();
            for (var item : required) {
                if (!item.isTextual() || !schema.path("properties").has(item.asText()) || !seen.add(item.asText()))
                    throw packageError("PACKAGE_REFERENCE_INVALID", field + ".required 只能引用唯一的已声明属性。");
            }
        }
    }

    private int depth(JsonNode node, int level) {
        if (!node.isContainerNode()) return level;
        var max = level;
        for (var child : node) max = Math.max(max, depth(child, level + 1));
        return max;
    }

    private List<String> stringArray(JsonNode value, String field, int max) {
        if (!value.isArray() || value.size() > max) throw unsupported(field + " 必须是受限字符串数组。");
        var result = new ArrayList<String>();
        for (var item : value) {
            if (!item.isTextual() || item.asText().isBlank() || item.asText().length() > 1_000)
                throw unsupported(field + " 包含无效文本。");
            result.add(item.asText());
        }
        return result;
    }

    private JsonNode readJson(byte[] content) throws IOException {
        var node = JSON.readTree(content);
        if (node == null) throw packageError("PACKAGE_FORMAT_UNSUPPORTED", "声明包 JSON 不能为空。");
        return node;
    }

    private ObjectNode object(JsonNode node, String field) {
        if (node == null || !node.isObject()) throw packageError("PACKAGE_FORMAT_UNSUPPORTED", field + " 必须是 JSON 对象。");
        return (ObjectNode) node;
    }

    private void requireFields(JsonNode node, Set<String> expected, String field) {
        var actual = new HashSet<String>();
        node.fieldNames().forEachRemaining(actual::add);
        if (!actual.equals(expected)) throw packageError("PACKAGE_FORMAT_UNSUPPORTED", field + " 字段不符合固定 profile。");
    }

    private String string(JsonNode node, String field) {
        var value = node.path(field);
        if (!value.isTextual() || value.asText().isBlank()) throw packageError("PACKAGE_FORMAT_UNSUPPORTED", field + " 必须是非空字符串。");
        return value.asText();
    }

    private UUID uuid(String value, String field) {
        try {
            var parsed = UUID.fromString(value);
            if (!parsed.toString().equals(value)) throw new IllegalArgumentException();
            return parsed;
        } catch (IllegalArgumentException malformed) {
            throw packageError("PACKAGE_REFERENCE_INVALID", field + " 必须是标准 UUID。");
        }
    }

    private String version(String value, String field) {
        if (!value.matches("[A-Za-z0-9][A-Za-z0-9.+_-]{0,39}"))
            throw packageError("PACKAGE_REFERENCE_INVALID", field + " 版本格式无效。");
        return value;
    }

    private String hash(String value, String field) {
        if (!value.matches("[0-9a-f]{64}")) throw packageError("PACKAGE_REFERENCE_INVALID", field + " 必须是 SHA-256 摘要。");
        return value;
    }

    private JsonNode normalize(JsonNode node) {
        if (node == null) return JSON.getNodeFactory().nullNode();
        if (node.isObject()) {
            var sorted = new TreeMap<String, JsonNode>();
            node.fields().forEachRemaining(entry -> sorted.put(entry.getKey(), entry.getValue()));
            var result = JSON.createObjectNode();
            sorted.forEach((key, value) -> result.set(key, normalize(value)));
            return result;
        }
        if (node.isArray()) {
            var result = JSON.createArrayNode();
            node.forEach(value -> result.add(normalize(value)));
            return result;
        }
        return node.deepCopy();
    }

    private byte[] write(JsonNode node) {
        try { return JSON.writeValueAsBytes(normalize(node)); }
        catch (IOException impossible) { throw new IllegalStateException("声明包 JSON 无法序列化。", impossible); }
    }

    private String quote(String value) {
        try { return JSON.writeValueAsString(value); }
        catch (IOException impossible) { throw new IllegalStateException("YAML frontmatter 转义失败。", impossible); }
    }

    private byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private String text(byte[] value) { return new String(value, StandardCharsets.UTF_8); }

    private String decode(byte[] value) throws IOException {
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(value)).toString();
    }

    private String rootName(UUID capabilityId) { return "eaf-" + capabilityId.toString().replace("-", ""); }

    private EafException unsupported(String message) {
        return packageError("PACKAGE_FORMAT_UNSUPPORTED", message);
    }

    private EafException packageError(String code, String message) {
        return new EafException(400, code, message);
    }
}
