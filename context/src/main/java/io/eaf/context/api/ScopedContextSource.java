package io.eaf.context.api;

import java.util.UUID;

public record ScopedContextSource(UUID workspaceId, String name, String kind, int returnedItemCount) { }
