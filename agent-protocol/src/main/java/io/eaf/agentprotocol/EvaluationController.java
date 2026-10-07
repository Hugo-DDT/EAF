package io.eaf.agentprotocol;

import io.eaf.evaluation.api.EvaluationReport;
import io.eaf.evaluation.api.P3QualityRunReport;
import io.eaf.evaluation.api.CandidateEvaluationReport;
import io.eaf.evaluation.api.EvaluationService;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/evaluations")
public class EvaluationController {
    private final WorkspaceAuthorization workspaces;
    private final EvaluationService evaluations;
    public EvaluationController(WorkspaceAuthorization workspaces, EvaluationService evaluations) { this.workspaces = workspaces; this.evaluations = evaluations; }

    @PostMapping("/p1")
    EvaluationReport run(@PathVariable UUID workspaceId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:run");
        return evaluations.run(actor, access.workspaceId());
    }

    @PostMapping("/p2")
    EvaluationReport runP2(@PathVariable UUID workspaceId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:run");
        return evaluations.runP2(actor, access.workspaceId());
    }

    @PostMapping("/p3")
    P3QualityRunReport runP3(@PathVariable UUID workspaceId,
                             @RequestHeader("Idempotency-Key") String idempotencyKey,
                             Authentication authentication) {
        // 身份与评测授权先绑定租户和空间，运行键再把全部样本锁定到同一预算 Scope。
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:run");
        return evaluations.runP3Quality(actor, access.workspaceId(), idempotencyKey);
    }

    @GetMapping("/p3/{reportId}")
    P3QualityRunReport p3Report(@PathVariable UUID workspaceId, @PathVariable UUID reportId,
                                Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:read");
        return evaluations.getP3QualityRun(actor, access.workspaceId(), reportId);
    }

    @PostMapping("/p3/{reportId}/stop")
    P3QualityRunReport stopP3(@PathVariable UUID workspaceId, @PathVariable UUID reportId,
                              Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:run");
        return evaluations.stopP3QualityRun(actor, access.workspaceId(), reportId);
    }

    @PostMapping("/p4")
    EvaluationReport runP4(@PathVariable UUID workspaceId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:run");
        return evaluations.runP4(actor, access.workspaceId());
    }

    @PostMapping("/candidate-snapshots/{snapshotId}/runs")
    CandidateEvaluationReport runCandidateEvaluation(@PathVariable UUID workspaceId, @PathVariable UUID snapshotId,
                                                     Authentication authentication) {
        // 固定保留集答案只由 Evaluation 服务读取，输入仅通过绑定式 EVALUATION Task 进入 Agent Runtime。
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:run");
        return evaluations.runCandidateEvaluation(actor, access.workspaceId(), snapshotId);
    }

    @GetMapping("/{reportId}")
    CandidateEvaluationReport candidateReport(@PathVariable UUID workspaceId, @PathVariable UUID reportId,
                                              Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var access = workspaces.require(actor, workspaceId, "evaluation:read");
        return evaluations.getCandidateEvaluation(actor, access.workspaceId(), reportId);
    }
}
// 本文件负责实现 EAF 的 EvaluationController.java 相关代码。
