package io.eaf.agent.api;

import java.util.UUID;

// Agent 版本在发布时固定 RAG 开关，Runtime 据此决定是否读取授权 Context。
public record AgentDefinition(UUID id, UUID tenantId, UUID workspaceId, String name,
                              String version, UUID promptId, String promptVersion,
                              UUID modelProfileId, String status, boolean ragEnabled,
                              String responseProfile, String retrievalMode, String evidencePolicy) { }
// 本文件负责实现 EAF 的 AgentDefinition.java 相关代码。
