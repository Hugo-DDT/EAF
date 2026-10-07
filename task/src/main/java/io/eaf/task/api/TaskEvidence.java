package io.eaf.task.api;

import java.util.UUID;

public record TaskEvidence(TaskSnapshot snapshot, UUID promptId, String promptVersion, UUID qualityRunId) { }
// 本文件负责实现 EAF 的 TaskEvidence.java 相关代码。
