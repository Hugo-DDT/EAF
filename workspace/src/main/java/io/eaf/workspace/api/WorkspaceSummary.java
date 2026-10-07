package io.eaf.workspace.api;

import java.util.UUID;

public record WorkspaceSummary(UUID workspaceId, String name, WorkspaceKind kind,
                               UUID ownerId, UUID parentWorkspaceId, String parentName) { }
