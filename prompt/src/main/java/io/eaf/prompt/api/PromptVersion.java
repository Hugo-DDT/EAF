package io.eaf.prompt.api;

import java.util.UUID;

public record PromptVersion(UUID id, UUID tenantId, UUID workspaceId, String version,
                            String systemTemplate, String userTemplate, String status) { }
// 本文件负责实现 EAF 的 PromptVersion.java 相关代码。
