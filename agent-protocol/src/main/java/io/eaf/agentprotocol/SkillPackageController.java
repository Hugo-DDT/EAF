package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.capability.api.CapabilityService;
import io.eaf.shared.EafException;
import io.eaf.skill.api.SkillPackageService;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}")
public class SkillPackageController {
    private static final MediaType ZIP = MediaType.parseMediaType("application/zip");
    private static final Set<String> PAGE_PARAMS = Set.of("limit", "offset");
    private final SkillPackageService packages;
    private final CapabilityService capabilities;
    private final PlatformAccessApplicationService access;
    private final ObjectMapper json;

    public SkillPackageController(SkillPackageService packages, CapabilityService capabilities,
                                  PlatformAccessApplicationService access, ObjectMapper json) {
        this.packages = packages;
        this.capabilities = capabilities;
        this.access = access;
        this.json = json;
    }

    @GetMapping("/capabilities/{capabilityId}/versions/{version}/package")
    ResponseEntity<byte[]> export(@PathVariable UUID workspaceId, @PathVariable UUID capabilityId,
                                  @PathVariable String version, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        // 导出需要当前 Owner API 补出的 Skill 内容摘要；普通目录详情仍保持只读投影原权限。
        var published = capabilities.requirePublished(actor, workspaceId, capabilityId, version);
        var listed = access.getCapability(actor, workspaceId, capabilityId, version);
        var capability = new PlatformAccessApplicationService.CapabilityUsageDetail(listed.kind(), listed.id(),
                listed.name(), listed.description(), listed.version(), listed.contentHash(),
                new PlatformAccessApplicationService.SkillVersionReference(published.skillId(), published.skillVersion(),
                        published.skillContentHash()), listed.taskInput(), listed.usageNotes());
        var skillReference = capability.skill();
        var skill = access.getSkill(actor, workspaceId, skillReference.id(), skillReference.version());
        if (!skill.contentHash().equals(skillReference.contentHash()))
            throw EafException.conflict("PACKAGE_REFERENCE_INVALID", "Capability 绑定的 Skill 当前摘要不匹配。");
        var source = new SkillPackageService.ExportSource(workspaceId,
                new SkillPackageService.AssetProjection(capability.id(), capability.name(), capability.description(),
                        capability.version(), capability.contentHash(), json.valueToTree(capability)),
                new SkillPackageService.AssetProjection(skill.id(), skill.name(), skill.description(), skill.version(),
                        skill.contentHash(), json.valueToTree(skill)));
        var content = packages.export(actor, workspaceId, source);
        var filename = "eaf-capability-" + capabilityId + "-" + capability.version().replaceAll("[^A-Za-z0-9._-]", "_") + ".zip";
        return ResponseEntity.ok().contentType(ZIP).header(HttpHeaders.CONTENT_DISPOSITION,
                        ContentDisposition.attachment().filename(filename).build().toString())
                .header("X-Content-Type-Options", "nosniff").body(content);
    }

    @PostMapping(path = "/skill-packages/validate", consumes = "application/zip")
    SkillPackageService.PackageValidation validate(@PathVariable UUID workspaceId, HttpServletRequest request,
                                                   Authentication authentication) throws IOException {
        return packages.validate(ApiSupport.actor(authentication), workspaceId, readZip(request));
    }

    @PostMapping(path = "/skill-packages", consumes = "application/zip")
    ResponseEntity<SkillPackageService.PackageView> importPackage(@PathVariable UUID workspaceId,
            @RequestHeader("Idempotency-Key") String key, HttpServletRequest request,
            Authentication authentication) throws IOException {
        var result = packages.importPackage(ApiSupport.actor(authentication), workspaceId, key, readZip(request));
        return ResponseEntity.status(result.created() ? 201 : 200).body(result.packageView());
    }

    @GetMapping("/skill-packages")
    SkillPackageService.PackagePage list(@PathVariable UUID workspaceId,
            @RequestParam MultiValueMap<String, String> query, Authentication authentication) {
        if (query.keySet().stream().anyMatch(key -> !PAGE_PARAMS.contains(key)))
            throw EafException.invalid("声明包列表只接受 limit 和 offset。");
        var limit = integer(query, "limit");
        var offset = integer(query, "offset");
        return packages.list(ApiSupport.actor(authentication), workspaceId,
                limit == null ? 20 : limit, offset == null ? 0 : offset);
    }

    @GetMapping("/skill-packages/{packageId}")
    SkillPackageService.PackageView get(@PathVariable UUID workspaceId, @PathVariable UUID packageId,
                                        Authentication authentication) {
        return packages.get(ApiSupport.actor(authentication), workspaceId, packageId);
    }

    @GetMapping("/skill-packages/{packageId}/download")
    ResponseEntity<byte[]> download(@PathVariable UUID workspaceId, @PathVariable UUID packageId,
                                    Authentication authentication) {
        var file = packages.download(ApiSupport.actor(authentication), workspaceId, packageId);
        return ResponseEntity.ok().contentType(ZIP).header(HttpHeaders.CONTENT_DISPOSITION,
                        ContentDisposition.attachment().filename(file.fileName()).build().toString())
                .header("X-Content-Type-Options", "nosniff").body(file.content());
    }

    @PostMapping("/skill-packages/{packageId}/archive")
    SkillPackageService.PackageView archive(@PathVariable UUID workspaceId, @PathVariable UUID packageId,
            @RequestBody java.util.Map<String, Object> body, Authentication authentication) {
        if (body == null || !body.keySet().equals(Set.of("expectedVersion"))
                || !(body.get("expectedVersion") instanceof Number number))
            throw EafException.invalid("archive 请求只接受整数 expectedVersion。");
        final long expectedVersion;
        try {
            expectedVersion = new java.math.BigDecimal(number.toString()).longValueExact();
        } catch (NumberFormatException | ArithmeticException invalid) {
            throw EafException.invalid("archive 请求只接受整数 expectedVersion。");
        }
        return packages.archive(ApiSupport.actor(authentication), workspaceId, packageId, expectedVersion);
    }

    private byte[] readZip(HttpServletRequest request) throws IOException {
        var contentType = request.getContentType();
        if (contentType == null || !MediaType.parseMediaType(contentType).isCompatibleWith(ZIP))
            throw new EafException(415, "UNSUPPORTED_MEDIA_TYPE", "声明包请求必须使用 application/zip。");
        var length = request.getContentLengthLong();
        if (length > DeclarativeCapabilityPackageLimits.MAX_ZIP_BYTES)
            throw new EafException(413, "PACKAGE_TOO_LARGE", "声明包超过 1 MiB 限制。");
        var bytes = request.getInputStream().readNBytes(DeclarativeCapabilityPackageLimits.MAX_ZIP_BYTES + 1);
        if (bytes.length > DeclarativeCapabilityPackageLimits.MAX_ZIP_BYTES)
            throw new EafException(413, "PACKAGE_TOO_LARGE", "声明包超过 1 MiB 限制。");
        return bytes;
    }

    private Integer integer(MultiValueMap<String, String> query, String name) {
        var values = query.get(name);
        if (values == null) return null;
        if (values.size() != 1) throw EafException.invalid(name + " 只能提供一次。");
        return PlatformAccessApplicationService.optionalQueryInteger(values.getFirst(), name);
    }

    private static final class DeclarativeCapabilityPackageLimits {
        private static final int MAX_ZIP_BYTES = 1_048_576;
    }
}
