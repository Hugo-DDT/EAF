package io.eaf.observability.api;

import java.time.Instant;
import java.util.UUID;

public record TraceObservation(String traceId, UUID taskId, UUID runId, String phase,
                               long elapsedMs, String outcome, String errorCode, Instant occurredAt) { }
// 本文件负责实现 EAF 的 TraceObservation.java 相关代码。
