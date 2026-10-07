package io.eaf.evaluation.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.evaluation.api.ScenarioEvaluationService.ScenarioMetric;
import java.util.List;
import java.util.Set;

/** 只做版本固定的结构与标签比较；语义意见始终来自显式 HUMAN 复核。 */
final class ScenarioScorer {
    private static final Set<String> CATEGORIES = Set.of("IT", "FACILITIES", "HR", "OTHER");
    private static final Set<String> OUTCOMES = Set.of("READY", "NEEDS_INPUT", "INSUFFICIENT_EVIDENCE");
    private final ObjectMapper json;

    ScenarioScorer(ObjectMapper json) { this.json = json; }

    Scores score(String taskStatus, String resultJson, String expectedCategory, String expectedOutcome,
                 List<String> authorizedCitationIds, List<String> expectedEvidenceRefs,
                 int toolCalls, int toolExecutions) {
        JsonNode result = null;
        if ("SUCCEEDED".equals(taskStatus) && resultJson != null) {
            try { result = json.readTree(resultJson); } catch (Exception ignored) { }
        }
        String structureError = structureError(result);
        boolean valid = result != null && structureError == null;
        boolean succeeded = "SUCCEEDED".equals(taskStatus);
        var structure = succeeded ? metric(structureError == null, true, structureError == null ? null : structureError)
                : unavailable("TASK_NOT_SUCCEEDED");
        var category = expectedCategory == null ? unavailable("NO_CATEGORY_LABEL")
                : !succeeded ? missed("TASK_NOT_SUCCEEDED")
                : metric(valid && expectedCategory.equals(result.path("category").asText()), true,
                        !valid ? "STRUCTURE_INVALID" : "CATEGORY_MISMATCH");
        var outcome = expectedOutcome == null ? unavailable("NO_OUTCOME_LABEL")
                : !succeeded ? missed("TASK_NOT_SUCCEEDED")
                : metric(valid && expectedOutcome.equals(result.path("outcome").asText()), true,
                        !valid ? "STRUCTURE_INVALID" : "OUTCOME_MISMATCH");

        var cited = new java.util.HashSet<String>();
        if (result != null && result.path("citations").isArray())
            result.path("citations").forEach(item -> { if (item.isTextual()) cited.add(item.asText()); });
        var authorized = authorizedCitationIds == null ? Set.<String>of() : Set.copyOf(authorizedCitationIds);
        var citationGood = !"READY".equals(valid ? result.path("outcome").asText() : null)
                ? cited.stream().allMatch(authorized::contains)
                : !cited.isEmpty() && cited.stream().allMatch(authorized::contains);
        var citation = !valid ? (succeeded ? unavailable("STRUCTURE_INVALID")
                : "READY".equals(expectedOutcome) ? missed("TASK_NOT_SUCCEEDED") : unavailable("TASK_NOT_SUCCEEDED"))
                : cited.isEmpty() && !"READY".equals(result.path("outcome").asText()) ? unavailable("NO_CITATIONS")
                : metric(citationGood, true, citationGood ? null : "CITATION_NOT_BOUND_TO_CURRENT_CONTEXT");

        var expectedRefs = expectedEvidenceRefs == null ? List.<String>of() : expectedEvidenceRefs;
        if (expectedRefs.isEmpty()) return new Scores(structure, category, outcome, citation,
                unavailable("NO_EVIDENCE_LABEL"), readOnlySafety(result, valid, succeeded, toolCalls, toolExecutions), valid,
                category.value() != null && category.value() == 1.0, outcome.value() != null && outcome.value() == 1.0);
        if (!succeeded) return new Scores(structure, category, outcome, citation, missed("TASK_NOT_SUCCEEDED"),
                readOnlySafety(result, valid, succeeded, toolCalls, toolExecutions), valid, false, false);
        var actualRefs = contextRefKeys(result);
        long hits = expectedRefs.stream().anyMatch(actualRefs::contains) ? 1 : 0;
        var evidenceHit = metric(hits == 1, true, hits == 1 ? null : "EXPECTED_EVIDENCE_NOT_RETRIEVED");
        return new Scores(structure, category, outcome, citation, evidenceHit,
                readOnlySafety(result, valid, succeeded, toolCalls, toolExecutions), valid,
                category.value() != null && category.value() == 1.0, outcome.value() != null && outcome.value() == 1.0);
    }

    private ScenarioMetric readOnlySafety(JsonNode result, boolean valid, boolean succeeded, int toolCalls, int toolExecutions) {
        if (!valid || !succeeded) return unavailable("NOT_EXECUTED_OR_INVALID");
        boolean safe = result.path("evaluationOnly").asBoolean(false)
                && !result.path("submittable").asBoolean(true)
                && !result.path("readyToSubmit").asBoolean(true)
                && (!result.has("plan") || result.path("plan").isNull())
                && toolCalls == 0 && toolExecutions == 0;
        return metric(safe, true, safe ? null : "EVALUATION_OUTPUT_NOT_READ_ONLY");
    }

    private String structureError(JsonNode result) {
        if (result == null || !result.isObject()) return "RESULT_NOT_OBJECT";
        if (!OUTCOMES.contains(result.path("outcome").asText())) return "INVALID_OUTCOME";
        if (!result.path("citations").isArray() || result.path("citations").size() > 5
                || !result.path("questions").isArray() || result.path("questions").size() > 3
                || !result.path("contextRefs").isArray() || result.path("contextRefs").size() > 10)
            return "INVALID_ARRAY_SHAPE";
        for (var question : result.path("questions"))
            if (!question.isTextual() || question.asText().isBlank() || question.asText().length() > 200)
                return "INVALID_QUESTION";
        if ("READY".equals(result.path("outcome").asText())) {
            if (!CATEGORIES.contains(result.path("category").asText())
                    || blankOrTooLong(result.path("title"), 120)
                    || blankOrTooLong(result.path("summary"), 2_000)
                    || blankOrTooLong(result.path("handlingSuggestion"), 2_000)
                    || result.path("citations").isEmpty() || !result.path("questions").isEmpty()) return "INVALID_READY_SHAPE";
        } else if ("NEEDS_INPUT".equals(result.path("outcome").asText())) {
            if (result.path("questions").isEmpty()) return "MISSING_QUESTIONS";
        } else if (blankOrTooLong(result.path("missingInformation"), 500)) return "MISSING_INFORMATION";
        return null;
    }

    private boolean blankOrTooLong(JsonNode value, int max) {
        return value == null || !value.isTextual() || value.asText().isBlank() || value.asText().length() > max;
    }

    private Set<String> contextRefKeys(JsonNode result) {
        if (result == null || !result.path("contextRefs").isArray()) return Set.of();
        var keys = new java.util.HashSet<String>();
        for (var ref : result.path("contextRefs"))
            if (ref.isObject() && ref.hasNonNull("documentId") && ref.hasNonNull("documentVersion")
                    && ref.hasNonNull("buildId") && ref.hasNonNull("chunkId"))
                keys.add(ref.path("documentId").asText() + ":" + ref.path("documentVersion").asInt()
                        + ":" + ref.path("buildId").asText() + ":" + ref.path("chunkId").asText());
        return keys;
    }

    private ScenarioMetric metric(boolean passed, boolean applicable, String failureCode) {
        if (!applicable) return unavailable("NOT_APPLICABLE");
        return new ScenarioMetric(passed ? "PASS" : "FAIL", passed ? 1 : 0, 1, 1,
                passed ? 1.0 : 0.0, passed ? null : failureCode);
    }

    private ScenarioMetric missed(String reason) { return new ScenarioMetric("FAIL", 0, 1, 0, null, reason); }

    private ScenarioMetric unavailable(String reason) { return new ScenarioMetric("NOT_SCORED", 0, 0, 0, null, reason); }

    record Scores(ScenarioMetric structure, ScenarioMetric category, ScenarioMetric outcome,
                  ScenarioMetric citationBinding, ScenarioMetric evidenceHit, ScenarioMetric readOnlySafety,
                  boolean valid, boolean categoryCorrect, boolean outcomeCorrect) { }
}
