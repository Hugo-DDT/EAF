package io.eaf.context.api;

import java.util.List;
import java.util.UUID;

/** 实际带入 prepare 模型输入的团队经验版本与 Memory 来源。 */
public record TeamExperienceUsage(List<Included> included) {
    public TeamExperienceUsage { included = included == null ? List.of() : List.copyOf(included); }
    public record Included(UUID cardId, int revision, UUID memoryId, String memoryVersion, String contentHash) { }
}
