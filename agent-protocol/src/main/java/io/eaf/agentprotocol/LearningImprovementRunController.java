package io.eaf.agentprotocol;

import io.eaf.learning.api.CandidateService;
import io.eaf.learning.api.CandidateService.ImprovementRun;
import io.eaf.shared.EafException;
import java.time.Instant;
import java.util.List;
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

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/learning/improvement-runs")
public class LearningImprovementRunController {
    private final CandidateService learning;
    private final com.fasterxml.jackson.databind.ObjectMapper json;

    public LearningImprovementRunController(CandidateService learning, com.fasterxml.jackson.databind.ObjectMapper json) {
        this.learning = learning;
        this.json = json;
    }

    @PostMapping
    ResponseEntity<ImprovementRun> create(@PathVariable UUID workspaceId,
            @RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody Map<String, Object> rawBody,
            Authentication authentication) {
        var body = convert(rawBody);
        var result = learning.createImprovementRun(new CandidateService.ImprovementRunCommand(
                ApiSupport.actor(authentication), workspaceId, body.cardId(), body.baseRevision(),
                body.expectedCardVersion(), body.sourceFeedbackIds(), body.sharedCorrection(), body.datasetKey(),
                body.datasetVersion(), body.deadlineAt(), idempotencyKey));
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.run());
    }

    @GetMapping("/{runId}")
    ImprovementRun get(@PathVariable UUID workspaceId, @PathVariable UUID runId, Authentication authentication) {
        return learning.getImprovementRun(ApiSupport.actor(authentication), workspaceId, runId);
    }

    @PostMapping("/{runId}/stop")
    ImprovementRun stop(@PathVariable UUID workspaceId, @PathVariable UUID runId,
            @RequestParam long expectedVersion, Authentication authentication) {
        return learning.stopImprovementRun(ApiSupport.actor(authentication), workspaceId, runId, expectedVersion);
    }

    private RunBody convert(Map<String, Object> rawBody) {
        var allowed = Set.of("cardId", "baseRevision", "expectedCardVersion", "sourceFeedbackIds",
                "sharedCorrection", "datasetKey", "datasetVersion", "deadlineAt");
        if (rawBody == null || rawBody.isEmpty() || rawBody.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("启动请求包含空体或未允许字段。");
        try { return json.convertValue(rawBody, RunBody.class); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("启动请求字段格式无效。"); }
    }

    record RunBody(UUID cardId, int baseRevision, long expectedCardVersion, List<UUID> sourceFeedbackIds,
                   String sharedCorrection, String datasetKey, String datasetVersion, Instant deadlineAt) { }
}
