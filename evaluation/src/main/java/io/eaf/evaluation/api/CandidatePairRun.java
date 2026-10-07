package io.eaf.evaluation.api;

import java.util.UUID;

/** 相同输入在基线与候选隔离快照上的 Task 运行结果。 */
public record CandidatePairRun(UUID id, UUID snapshotId, UUID baselineTaskId, UUID candidateTaskId,
                               String baselineStatus, String baselineErrorCode,
                               String candidateStatus, String candidateErrorCode) { }
