package io.eaf.evaluation.api;

import io.eaf.shared.ActorContext;
import io.eaf.model.api.ModelProfileRef;
import io.eaf.model.api.ModelProfileSelection;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface ScenarioEvaluationService {
    List<ScenarioDatasetSummary> listDatasets(ActorContext actor, UUID workspaceId);
    ScenarioRunResult createRun(ActorContext actor, UUID workspaceId, ScenarioRunRequest request, String idempotencyKey);
    ScenarioRunReport getRun(ActorContext actor, UUID workspaceId, UUID runId);
    List<ScenarioSampleView> listSamples(ActorContext actor, UUID workspaceId, UUID runId, int limit);
    ScenarioSampleEvidence getEvidence(ActorContext actor, UUID workspaceId, UUID runId, UUID sampleId);
    ScenarioRunReport stopRun(ActorContext actor, UUID workspaceId, UUID runId, long expectedVersion);
    ScenarioReviewResult review(ActorContext actor, UUID workspaceId, UUID runId, UUID sampleId,
                                ScenarioReviewCommand command, String idempotencyKey);
    List<ScenarioReview> listReviews(ActorContext actor, UUID workspaceId, UUID runId, UUID sampleId);

    /** 一个定时 tick 最多推进一个样本；所有模型工作仍由 TaskDispatcher 执行。 */
    void dispatchNext();

    record ScenarioDatasetSummary(String datasetKey, String version, String sceneKey, String scoringVersion,
                                  int devCases, int heldOutCases, String manifestHash) { }

    record ScenarioRunRequest(String datasetKey, String datasetVersion, String split, String mode,
                              CapabilityVersion baseline, CapabilityVersion comparison, Instant deadlineAt,
                              String experiment, ModelProfileRef baselineModelProfileRef,
                              ModelProfileRef comparisonModelProfileRef) {
        public ScenarioRunRequest(String datasetKey, String datasetVersion, String split, String mode,
                                  CapabilityVersion baseline, CapabilityVersion comparison, Instant deadlineAt) {
            this(datasetKey, datasetVersion, split, mode, baseline, comparison, deadlineAt, null, null, null);
        }
    }

    record CapabilityVersion(UUID capabilityId, String version) { }

    record ScenarioRunResult(ScenarioRunReport report, boolean created) { }

    record ScenarioRunReport(UUID runId, String status, long version, boolean partial, String stopReason,
                             int plannedSamples, int completedSamples, int failedSamples, int notRunSamples,
                             boolean comparable, boolean sameAsset, String comparisonReason,
                             ScenarioMetric execution, ScenarioMetric structure, ScenarioMetric category,
                             ScenarioMetric outcome, ScenarioMetric citationBinding,
                             ScenarioMetric evidenceHit, ScenarioMetric readOnlySafety,
                             int improvedPairs, int regressedPairs, int unchangedPairs, int unevaluablePairs,
                             String manifestHash, Instant createdAt, Instant updatedAt, Instant deadlineAt,
                             ScenarioUsageSummary usage, ScenarioTimingSummary timing,
                             ScenarioOptimizationSummary optimization,
                             ScenarioModelProfileEvidence modelProfileEvidence) {
        public ScenarioRunReport(UUID runId, String status, long version, boolean partial, String stopReason,
                                 int plannedSamples, int completedSamples, int failedSamples, int notRunSamples,
                                 boolean comparable, boolean sameAsset, String comparisonReason,
                                 ScenarioMetric execution, ScenarioMetric structure, ScenarioMetric category,
                                 ScenarioMetric outcome, ScenarioMetric citationBinding, ScenarioMetric evidenceHit,
                                 ScenarioMetric readOnlySafety, int improvedPairs, int regressedPairs,
                                 int unchangedPairs, int unevaluablePairs, String manifestHash, Instant createdAt,
                                 Instant updatedAt, Instant deadlineAt, ScenarioUsageSummary usage,
                                 ScenarioTimingSummary timing, ScenarioOptimizationSummary optimization) {
            this(runId, status, version, partial, stopReason, plannedSamples, completedSamples, failedSamples,
                    notRunSamples, comparable, sameAsset, comparisonReason, execution, structure, category, outcome,
                    citationBinding, evidenceHit, readOnlySafety, improvedPairs, regressedPairs, unchangedPairs,
                    unevaluablePairs, manifestHash, createdAt, updatedAt, deadlineAt, usage, timing, optimization, null);
        }
    }

    record ScenarioModelProfileEvidence(String status, String experiment, String reasonCode,
                                        boolean configurationComparable, ModelProfileSelection baseline,
                                        ModelProfileSelection comparison) { }

    record ScenarioOptimizationSummary(int schemaVersion, String experiment, String manifestHash, Instant asOf,
                                       String status, List<String> reasonCodes, boolean conditionsComparable,
                                       List<String> conditionsReasons, ScenarioOptimizationSide baseline,
                                       ScenarioOptimizationSide comparison, List<ScenarioOptimizationPair> pairs,
                                       ScenarioOptimizationPairedSummary pairedSummary) { }

    record ScenarioOptimizationSide(String capabilityVersion, int plannedSamples, int completedSamples,
                                    int failedSamples, int notRunSamples, ScenarioMetric execution,
                                    ScenarioMetric structure, ScenarioMetric category, ScenarioMetric outcome,
                                    ScenarioMetric citationBinding, ScenarioMetric evidenceHit,
                                    ScenarioMetric readOnlySafety, int reviewedSamples, int selfReviewedSamples,
                                    int unreviewedSamples, int preparedRequests, int recordedTasks,
                                    int missingTasks, int matchedGenerationCalls, int preparedWithoutUsage,
                                    int generationCallsWithoutPresentation, long contextChars, long contextUtf8Bytes, long messageChars,
                                    long messageUtf8Bytes, long fullContextChars, long fullContextUtf8Bytes,
                                    long fullMessageChars, long fullMessageUtf8Bytes, long contextReductionChars,
                                    long contextReductionUtf8Bytes, long messageReductionChars,
                                    long messageReductionUtf8Bytes, int chatCalls,
                                    int embeddingCalls, List<String> modelIdentities, List<String> usageEvidence,
                                    ScenarioUsageSummary usage,
                                    ScenarioOptimizationTiming timing) { }

    record ScenarioOptimizationTiming(Long queueMedianMillis, Long taskMedianMillis, Long modelCallMedianMillis,
                                      int modelCallSamples, Long retrievalMedianMillis, int retrievalSamples,
                                      Long preparationMedianMillis, int preparationSamples) {
        public ScenarioOptimizationTiming(Long queueMedianMillis, Long taskMedianMillis,
                                          Long modelCallMedianMillis, int modelCallSamples) {
            this(queueMedianMillis, taskMedianMillis, modelCallMedianMillis, modelCallSamples, null, 0, null, 0);
        }
    }

    record ScenarioOptimizationPair(String caseId, String inputHash, UUID baselineSampleId,
                                    UUID comparisonSampleId, UUID baselineTaskId, UUID comparisonTaskId,
                                    Integer baselineAttempt, Integer comparisonAttempt, String baselineResultHash,
                                    String comparisonResultHash, boolean complete, String sourceRelation,
                                    java.util.Map<String, Double> qualityDeltas,
                                    java.util.Map<String, java.math.BigDecimal> metricDeltas,
                                    java.util.Map<String, String> metricUnits) { }

    record ScenarioMetricDelta(int comparedPairs, Double meanDelta) { }

    record ScenarioOptimizationPairedSummary(int completePairs, int incompletePairs,
                                             java.util.Map<String, ScenarioMetricDelta> qualityDeltas,
                                             java.util.Map<String, ScenarioOptimizationMetricDelta> metricDeltas) { }

    record ScenarioOptimizationMetricDelta(int comparedPairs, int unavailablePairs,
                                           java.math.BigDecimal totalDelta, java.math.BigDecimal medianDelta,
                                           String unit, String status) { }

    record ScenarioMetric(String status, int numerator, int denominator, int scored, Double value, String reasonCode) { }

    record ScenarioSampleView(UUID sampleId, String caseId, String side, String status, UUID taskId,
                              Integer taskAttempt, String resultHash, String errorCode,
                              ScenarioMetric structure, ScenarioMetric category, ScenarioMetric outcome,
                              ScenarioMetric citationBinding, ScenarioMetric evidenceHit,
                              ScenarioMetric readOnlySafety, String semanticReview, boolean sourcesUnavailable,
                              Long queueMillis, Long executionMillis) { }

    record ScenarioUsageSummary(int calls, Integer inputTokens, Integer outputTokens, int unknownTokenCalls,
                                java.math.BigDecimal estimatedCost, String estimatedCurrency, String estimatedCostStatus,
                                java.math.BigDecimal actualBilledCost, String actualCurrency, String actualCostStatus,
                                Instant asOf) { }

    record ScenarioTimingSummary(Long queueMedianMillis, Long executionMedianMillis, long elapsedMillis) { }

    record ScenarioSampleEvidence(UUID sampleId, String caseId, String side, String requestText,
                                  String resultJson, boolean sourcesUnavailable,
                                  List<String> visibleCitationIds) { }

    record ScenarioReviewCommand(String verdict, String issueType, String comment, UUID supersedesReviewId) { }

    record ScenarioReviewResult(ScenarioReview review, boolean created) { }

    record ScenarioReview(UUID id, UUID sampleId, int revision, UUID supersedesReviewId, int taskAttempt,
                          String resultHash, String rubricVersion, String verdict, String issueType,
                          String comment, boolean selfReviewed, Instant createdAt) { }
}
