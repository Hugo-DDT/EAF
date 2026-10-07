package io.eaf.workspace.api;

import java.util.UUID;

public record WorkspaceAccess(UUID workspaceId, UUID tenantId, String name) { }
// 本文件负责实现 EAF 的 WorkspaceAccess.java 相关代码。
