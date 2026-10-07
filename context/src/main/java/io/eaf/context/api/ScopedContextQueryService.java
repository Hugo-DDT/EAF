package io.eaf.context.api;

import io.eaf.shared.ActorContext;
import java.util.List;
import java.util.UUID;

public interface ScopedContextQueryService {
    ScopedContextResult query(ActorContext actor, UUID targetWorkspaceId, String query,
                              Integer topK, Integer tokenBudget, List<UUID> sourceWorkspaceIds);
}
