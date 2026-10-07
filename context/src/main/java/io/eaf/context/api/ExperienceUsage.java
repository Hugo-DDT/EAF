package io.eaf.context.api;

import java.util.List;
import java.util.UUID;

/** 服务端根据实际快照生成的个人经验带入/引用明细。 */
public record ExperienceUsage(List<IncludedExperience> included, List<String> citedCitationIds,
                             int omittedByLimit, int omittedByBudget) {
    public ExperienceUsage {
        included = included == null ? List.of() : List.copyOf(included);
        citedCitationIds = citedCitationIds == null ? List.of() : List.copyOf(citedCitationIds);
    }
    public record IncludedExperience(UUID cardId, int revision, String memoryVersion, String citationId,
                                     String applicability) { }
}
