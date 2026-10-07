package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.evaluation.api.EvaluationService;
import io.eaf.evaluation.api.EvaluationService.TeamPreparationReviewCommand;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 只公开合成简报与输出配对，不返回期望答案或来源任务正文。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/evaluations/team-preparation/runs")
public final class TeamPreparationEvaluationController {
    private final WorkspaceAuthorization workspaces;
    private final EvaluationService evaluations;
    private final ObjectMapper json;

    public TeamPreparationEvaluationController(WorkspaceAuthorization workspaces, EvaluationService evaluations,
                                               ObjectMapper json) {
        this.workspaces = workspaces;
        this.evaluations = evaluations;
        this.json = json;
    }

    @GetMapping("/{reportId}")
    EvaluationService.TeamPreparationRun get(@PathVariable UUID workspaceId, @PathVariable UUID reportId,
                                              Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:read");
        return evaluations.getTeamPreparationRun(actor, access.workspaceId(), reportId);
    }

    @GetMapping("/{reportId}/pairs/{caseId}")
    EvaluationService.TeamPreparationPair pair(@PathVariable UUID workspaceId, @PathVariable UUID reportId,
                                                @PathVariable String caseId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:read");
        return evaluations.getTeamPreparationPair(actor, access.workspaceId(), reportId, caseId);
    }

    @PostMapping("/{reportId}/pairs/{caseId}/reviews")
    EvaluationService.TeamPreparationReview review(@PathVariable UUID workspaceId, @PathVariable UUID reportId,
            @PathVariable String caseId, @RequestHeader("Idempotency-Key") String key,
            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw);
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:review");
        workspaces.require(actor, access.workspaceId(), "evaluation:read");
        return evaluations.reviewTeamPreparationPair(actor, access.workspaceId(), reportId, caseId, body, key);
    }

    private TeamPreparationReviewCommand convert(Map<String, Object> raw) {
        var allowed = Set.of("verdict", "comment", "factEvidenceRefs", "supersedesReviewId");
        if (raw == null || raw.isEmpty() || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("语义复核包含未允许字段。");
        try { return json.convertValue(raw, TeamPreparationReviewCommand.class); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("语义复核字段格式无效。"); }
    }
}
