package io.eaf.agentprotocol;

import io.eaf.learning.api.CandidateService;
import io.eaf.learning.api.CandidateService.PromptImprovementRun;
import io.eaf.learning.api.CandidateService.PromptAdoption;
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
@RequestMapping("/api/v1/workspaces/{workspaceId}/prompt-improvement-runs")
public class PromptImprovementRunController {
    private final CandidateService learning;
    private final com.fasterxml.jackson.databind.ObjectMapper json;

    public PromptImprovementRunController(CandidateService learning, com.fasterxml.jackson.databind.ObjectMapper json) {
        this.learning = learning; this.json = json;
    }

    @PostMapping
    ResponseEntity<PromptImprovementRun> create(@PathVariable UUID workspaceId,
            @RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody Map<String, Object> raw,
            Authentication authentication) {
        var body = convert(raw, Set.of("targetId", "expectedTargetVersion", "sourceFeedbackIds", "changeNote", "instructionAppendix", "deadlineAt"));
        var result = learning.createPromptImprovementRun(new CandidateService.PromptImprovementRunCommand(
                ApiSupport.actor(authentication), workspaceId, body.targetId(), body.expectedTargetVersion(),
                body.sourceFeedbackIds(), body.changeNote(), body.instructionAppendix(), body.deadlineAt(), idempotencyKey));
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.run());
    }

    @GetMapping("/{runId}")
    PromptImprovementRun get(@PathVariable UUID workspaceId, @PathVariable UUID runId, Authentication authentication) {
        return learning.getPromptImprovementRun(ApiSupport.actor(authentication), workspaceId, runId);
    }

    @PostMapping("/{runId}/stop")
    PromptImprovementRun stop(@PathVariable UUID workspaceId, @PathVariable UUID runId,
            @RequestParam long expectedVersion, Authentication authentication) {
        return learning.stopPromptImprovementRun(ApiSupport.actor(authentication), workspaceId, runId, expectedVersion);
    }

    @PostMapping("/{runId}/adopt")
    PromptAdoption adopt(@PathVariable UUID workspaceId, @PathVariable UUID runId,
            @RequestHeader("Idempotency-Key") String idempotencyKey, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var run = learning.getPromptImprovementRun(actor, workspaceId, runId);
        if (run.candidateId() == null) throw EafException.conflict("PROMPT_ADOPTION_NOT_READY", "改进运行尚无候选可采用。");
        return learning.adoptPromptImprovementRun(new CandidateService.PromptAdoptionCommand(
                actor, workspaceId, run.candidateId(), idempotencyKey));
    }

    @GetMapping("/{runId}/adoption")
    PromptAdoption adoption(@PathVariable UUID workspaceId, @PathVariable UUID runId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var run = learning.getPromptImprovementRun(actor, workspaceId, runId);
        if (run.candidateId() == null) throw EafException.notFound();
        return learning.getPromptAdoption(actor, workspaceId, run.candidateId());
    }

    private RunBody convert(Map<String, Object> raw, Set<String> allowed) {
        if (raw == null || raw.isEmpty() || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("Prompt 改进请求包含空体或未允许字段。");
        try { return json.convertValue(raw, RunBody.class); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("Prompt 改进请求字段格式无效。"); }
    }

    record RunBody(UUID targetId, long expectedTargetVersion, List<UUID> sourceFeedbackIds,
                   String changeNote, String instructionAppendix, Instant deadlineAt) { }
}
