package io.eaf.workspace.api;

import java.util.UUID;

public record WorkspaceProfile(UUID tenantId, UUID workspaceId, WorkspaceKind kind,
                               UUID ownerId, UUID parentWorkspaceId) { }
