package io.eaf.tool.api;

import java.util.UUID;

public record ToolDefinition(UUID tenantId, UUID workspaceId, String name, String version,
                             String description, String inputSchema, String outputSchema,
                             String permissionAction, String effect, String bindingRef,
                             String status) { }
// 本文件负责实现 EAF 的 ToolDefinition.java 相关代码。
