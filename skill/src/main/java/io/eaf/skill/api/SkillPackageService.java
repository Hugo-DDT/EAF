package io.eaf.skill.api;

import com.fasterxml.jackson.databind.JsonNode;
import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** EAF 声明包只是私有只读资料；它不会成为可执行 Skill。 */
public interface SkillPackageService {
    String PROFILE = "EAF_DECLARATIVE_CAPABILITY_V1";

    byte[] export(ActorContext actor, UUID workspaceId, ExportSource source);
    DeclarationPackage describe(ActorContext actor, UUID workspaceId, ExportSource source);
    DeclarationFile readDeclarationFile(ActorContext actor, UUID workspaceId, ExportSource source,
                                        String expectedPackageHash, String relativePath);
    PackageValidation validate(ActorContext actor, UUID workspaceId, byte[] zip);
    ImportResult importPackage(ActorContext actor, UUID workspaceId, String idempotencyKey, byte[] zip);
    PackagePage list(ActorContext actor, UUID workspaceId, int limit, int offset);
    PackageView get(ActorContext actor, UUID workspaceId, UUID packageId);
    PackageDownload download(ActorContext actor, UUID workspaceId, UUID packageId);
    PackageView archive(ActorContext actor, UUID workspaceId, UUID packageId, long expectedVersion);

    record AssetProjection(UUID id, String name, String description, String version, String contentHash,
                           JsonNode usageProjection) { }
    record ExportSource(UUID sourceWorkspaceId, AssetProjection capability, AssetProjection skill) { }
    record PackageSource(UUID workspaceId, UUID capabilityId, String capabilityVersion, String capabilityHash,
                         UUID skillId, String skillVersion, String skillHash) { }
    record PackageValidation(String profile, String formatVersion, String packageHash, String name,
                             PackageSource source, List<String> files, String status, boolean executable,
                             String trust, List<String> limitations) { }
    record PackageView(UUID id, UUID ownerId, String profile, String name, String packageHash,
                       PackageSource source, String status, long rowVersion, List<String> files,
                       Instant createdAt, Instant archivedAt) { }
    record PackagePage(List<PackageView> items, Integer nextOffset) { }
    record PackageDownload(String fileName, byte[] content) { }
    record ImportResult(PackageView packageView, boolean created) { }
    record DeclarationFileInfo(String path, String mimeType, int sizeBytes, String sha256) { }
    record DeclarationPackage(PackageSource source, String profile, String packageHash,
                              List<DeclarationFileInfo> files) { }
    record DeclarationFile(DeclarationFileInfo info, byte[] content) {
        public DeclarationFile { content = content.clone(); }
        @Override public byte[] content() { return content.clone(); }
    }
}
