package io.eaf.workspace.api;

import java.util.UUID;

public record ContextSourceSummary(UUID workspaceId, String name, WorkspaceKind kind,
                                   boolean selected) { }
