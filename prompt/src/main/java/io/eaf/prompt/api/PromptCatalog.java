package io.eaf.prompt.api;

import java.util.UUID;

public interface PromptCatalog {
    PromptVersion requirePublished(UUID tenantId, UUID workspaceId, UUID promptId, String version);
    RenderedPrompt render(UUID tenantId, UUID workspaceId, UUID promptId, String version, String input);
}
// 本文件负责实现 EAF 的 PromptCatalog.java 相关代码。
