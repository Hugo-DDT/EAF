package io.eaf.capability.api;

import java.util.List;
import java.util.UUID;

public record CreateCapabilityVersionCommand(String version, UUID agentId, String agentVersion,
                                             UUID skillId, String skillVersion, UUID promptId,
                                             String promptVersion, List<CapabilityToolReference> toolDependencies,
                                             String evaluationRef) { }
// 本文件负责实现 CreateCapabilityVersionCommand.java 相关代码。
