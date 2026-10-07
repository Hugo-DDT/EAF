package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.evaluation.api.ScenarioEvaluationService;
import io.eaf.evaluation.api.ScenarioEvaluationService.CapabilityVersion;
import io.eaf.evaluation.api.ScenarioEvaluationService.ScenarioReviewCommand;
import io.eaf.evaluation.api.ScenarioEvaluationService.ScenarioRunRequest;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 仅投影评分摘要；输入、期望答案和模型正文由 Evaluation Owner 分层读取。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/evaluations/scenarios/service-request-analysis")
public final class ScenarioEvaluationController {
    private final WorkspaceAuthorization workspaces;
    private final ScenarioEvaluationService scenarios;
    private final ObjectMapper json;

    public ScenarioEvaluationController(WorkspaceAuthorization workspaces,
                                        ScenarioEvaluationService scenarios, ObjectMapper json) {
        this.workspaces = workspaces;
        this.scenarios = scenarios;
        this.json = json;
    }

    @GetMapping("/datasets")
    java.util.List<ScenarioEvaluationService.ScenarioDatasetSummary> datasets(
            @PathVariable UUID workspaceId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:read");
        return scenarios.listDatasets(actor, access.workspaceId());
    }

    @PostMapping("/runs")
    ResponseEntity<ScenarioEvaluationService.ScenarioRunReport> create(
            @PathVariable UUID workspaceId, @RequestHeader("Idempotency-Key") String key,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var request = runRequest(raw);
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:run");
        var result = scenarios.createRun(actor, access.workspaceId(), request, key);
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.report());
    }

    @GetMapping("/runs/{runId}")
    ScenarioEvaluationService.ScenarioRunReport get(@PathVariable UUID workspaceId, @PathVariable UUID runId,
                                                     Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:read");
        return scenarios.getRun(actor, access.workspaceId(), runId);
    }

    @GetMapping("/runs/{runId}/samples")
    java.util.List<ScenarioEvaluationService.ScenarioSampleView> samples(
            @PathVariable UUID workspaceId, @PathVariable UUID runId,
            @RequestParam(defaultValue = "40") int limit, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:read");
        return scenarios.listSamples(actor, access.workspaceId(), runId, limit);
    }

    @GetMapping("/runs/{runId}/samples/{sampleId}/evidence")
    ScenarioEvaluationService.ScenarioSampleEvidence evidence(
            @PathVariable UUID workspaceId, @PathVariable UUID runId, @PathVariable UUID sampleId,
            Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:read");
        return scenarios.getEvidence(actor, access.workspaceId(), runId, sampleId);
    }

    @PostMapping("/runs/{runId}/stop")
    ScenarioEvaluationService.ScenarioRunReport stop(@PathVariable UUID workspaceId, @PathVariable UUID runId,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, StopBody.class, Set.of("expectedVersion"));
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:run");
        return scenarios.stopRun(actor, access.workspaceId(), runId, body.expectedVersion());
    }

    @PostMapping("/runs/{runId}/samples/{sampleId}/reviews")
    ResponseEntity<ScenarioEvaluationService.ScenarioReview> review(
            @PathVariable UUID workspaceId, @PathVariable UUID runId, @PathVariable UUID sampleId,
            @RequestHeader("Idempotency-Key") String key, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        var body = convert(raw, ScenarioReviewCommand.class,
                Set.of("verdict", "issueType", "comment", "supersedesReviewId"));
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:review");
        workspaces.require(actor, access.workspaceId(), "evaluation:read");
        var result = scenarios.review(actor, access.workspaceId(), runId, sampleId, body, key);
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.review());
    }

    @GetMapping("/runs/{runId}/samples/{sampleId}/reviews")
    java.util.List<ScenarioEvaluationService.ScenarioReview> reviews(
            @PathVariable UUID workspaceId, @PathVariable UUID runId, @PathVariable UUID sampleId,
            Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:read");
        return scenarios.listReviews(actor, access.workspaceId(), runId, sampleId);
    }

    private <T> T convert(Map<String, Object> raw, Class<T> type, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("请求包含未允许字段。");
        try { return json.convertValue(raw, type); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("请求字段类型无效。"); }
    }

    private ScenarioRunRequest runRequest(Map<String, Object> raw) {
        Set<String> allowed = Set.of("datasetKey", "datasetVersion", "split", "mode", "baseline", "comparison", "deadlineAt");
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("请求包含未允许字段。");
        var mode = raw.get("mode");
        if ("SINGLE".equals(mode) && raw.containsKey("comparison"))
            throw EafException.invalid("SINGLE 不接受 comparison 资产。");
        java.time.Instant deadline = null;
        try { if (raw.get("deadlineAt") != null) deadline = json.convertValue(raw.get("deadlineAt"), java.time.Instant.class); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("deadlineAt 字段类型无效。"); }
        return new ScenarioRunRequest(stringValue(raw.get("datasetKey")), stringValue(raw.get("datasetVersion")),
                stringValue(raw.get("split")), stringValue(mode), assetVersion(raw.get("baseline")),
                assetVersion(raw.get("comparison")), deadline);
    }

    private CapabilityVersion assetVersion(Object raw) {
        if (raw == null) return null;
        if (!(raw instanceof Map<?, ?> map) || map.keySet().stream().anyMatch(key -> !(key instanceof String name)
                || !Set.of("capabilityId", "version").contains(name)))
            throw EafException.invalid("资产字段无效。");
        try { return json.convertValue(raw, CapabilityVersion.class); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("资产字段类型无效。"); }
    }

    private String stringValue(Object value) {
        if (value == null || value instanceof String) return (String) value;
        throw EafException.invalid("字符字段类型无效。");
    }

    private record StopBody(long expectedVersion) { }
}
