package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.learning.api.CandidateApproval;
import io.eaf.learning.api.CandidateRelease;
import io.eaf.learning.api.CandidateProposalResult;
import io.eaf.learning.api.CandidateWithdrawal;
import io.eaf.learning.api.CandidateIteration;
import io.eaf.learning.api.CandidateService;
import io.eaf.learning.api.LearningCandidate;
import io.eaf.shared.EafException;
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
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/learning-candidates")
public class LearningCandidateController {
    private final CandidateService candidates;
    private final ObjectMapper json;

    public LearningCandidateController(CandidateService candidates, ObjectMapper json) {
        this.candidates = candidates; this.json = json;
    }

    @PostMapping
    ResponseEntity<LearningCandidate> propose(@PathVariable UUID workspaceId,
                                               @RequestHeader("Idempotency-Key") String idempotencyKey,
                                               @RequestBody Map<String, Object> rawBody,
                                               Authentication authentication) {
        var body = convert(rawBody, CandidateBody.class,
                Set.of("targetType", "targetId", "baseVersion", "proposedContent", "evidenceRefs", "sourceFeedbackId"));
        var result = candidates.propose(new CandidateService.ManualCandidateCommand(ApiSupport.actor(authentication),
                workspaceId, body.targetType(), body.targetId(), body.baseVersion(), body.proposedContent(),
                body.evidenceRefs(), body.sourceFeedbackId(), idempotencyKey));
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.candidate());
    }

    @PostMapping("/from-feedback/{feedbackId}")
    ResponseEntity<CandidateProposalResult> proposeFromFeedback(@PathVariable UUID workspaceId,
                                                                 @PathVariable UUID feedbackId,
                                                                 Authentication authentication) {
        var result = candidates.proposeFromFeedback(ApiSupport.actor(authentication), workspaceId, feedbackId);
        var created = result.sources().stream().anyMatch(source -> "CREATED".equals(source.outcome()));
        return ResponseEntity.status(created ? HttpStatus.CREATED : HttpStatus.OK).body(result);
    }

    @GetMapping("/{candidateId}")
    LearningCandidate get(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                         Authentication authentication) {
        return candidates.get(ApiSupport.actor(authentication), workspaceId, candidateId);
    }

    @PutMapping("/{candidateId}")
    LearningCandidate revise(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                             @RequestParam long expectedVersion, @RequestBody Map<String, Object> rawBody,
                             Authentication authentication) {
        var body = convert(rawBody, CandidateRevisionBody.class,
                Set.of("targetType", "targetId", "baseVersion", "proposedContent", "evidenceRefs"));
        return candidates.revise(new CandidateService.CandidateRevisionCommand(ApiSupport.actor(authentication),
                workspaceId, candidateId, expectedVersion, body.targetType(), body.targetId(), body.baseVersion(),
                body.proposedContent(), body.evidenceRefs()));
    }

    @PostMapping("/{candidateId}/reviews")
    LearningCandidate review(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                             @RequestParam long expectedVersion, @RequestBody Map<String, Object> rawBody,
                             Authentication authentication) {
        var body = convert(rawBody, ReviewBody.class, Set.of("decision", "reason", "factEvidenceRefs"));
        return candidates.review(new CandidateService.CandidateReviewCommand(ApiSupport.actor(authentication),
                workspaceId, candidateId, expectedVersion, body.decision(), body.reason(), body.factEvidenceRefs()));
    }

    @PostMapping("/{candidateId}/approvals")
    // 控制器只解码批准命令；是否可批准由 Learning 对修订、身份、报告和目标基线复核。
    CandidateApproval approve(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                              @RequestParam long expectedVersion, @RequestBody Map<String, Object> rawBody,
                              Authentication authentication) {
        var body = convert(rawBody, ApprovalBody.class, Set.of("decision", "reason", "evaluationReportId"));
        return candidates.approve(new CandidateService.CandidateApprovalCommand(ApiSupport.actor(authentication),
                workspaceId, candidateId, expectedVersion, body.decision(), body.reason(), body.evaluationReportId()));
    }

    @PostMapping("/{candidateId}/publish")
    // Learning 先持久化并冻结精确修订，再由目标领域 API 发布并返回可恢复的 releaseId。
    CandidateRelease publish(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                              @RequestParam long expectedVersion, @RequestBody Map<String, Object> rawBody,
                              Authentication authentication) {
        var body = convert(rawBody, PublishBody.class, Set.of("approvalId"));
        return candidates.publish(new CandidateService.CandidateReleaseCommand(ApiSupport.actor(authentication),
                workspaceId, candidateId, expectedVersion, body.approvalId()));
    }

    @GetMapping("/{candidateId}/release")
    CandidateRelease getRelease(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                                Authentication authentication) {
        return candidates.getRelease(ApiSupport.actor(authentication), workspaceId, candidateId);
    }

    @PostMapping("/{candidateId}/withdraw-release")
    // 撤回记录先持久化；目标模块按来源键禁用具体版本后，Learning 再补记撤回收据。
    CandidateWithdrawal withdrawRelease(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                                         @RequestParam long expectedVersion, @RequestBody Map<String, Object> rawBody,
                                         Authentication authentication) {
        var body = convert(rawBody, WithdrawalBody.class, Set.of("reasonRef"));
        return candidates.withdrawRelease(new CandidateService.CandidateWithdrawalCommand(ApiSupport.actor(authentication),
                workspaceId, candidateId, expectedVersion, body.reasonRef()));
    }

    @GetMapping("/{candidateId}/withdrawal")
    CandidateWithdrawal getWithdrawal(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                                       Authentication authentication) {
        return candidates.getWithdrawal(ApiSupport.actor(authentication), workspaceId, candidateId);
    }

    // 请求只接收后续 Task ID；Learning 重新核验来源和实际使用版本，拒绝客户端自报效果。
    @PostMapping("/{candidateId}/iterations")
    ResponseEntity<CandidateIteration> recordIteration(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                                                       @RequestBody Map<String, Object> rawBody,
                                                       Authentication authentication) {
        var body = convert(rawBody, IterationBody.class, Set.of("taskId"));
        var result = candidates.recordIteration(ApiSupport.actor(authentication), workspaceId, candidateId, body.taskId());
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.iteration());
    }

    // 只返回不含 Task 正文的来源、版本、哈希及核验状态历史。
    @GetMapping("/{candidateId}/iterations")
    List<CandidateIteration> iterations(@PathVariable UUID workspaceId, @PathVariable UUID candidateId,
                                        Authentication authentication) {
        return candidates.iterations(ApiSupport.actor(authentication), workspaceId, candidateId);
    }

    private <T> T convert(Map<String, Object> rawBody, Class<T> type, Set<String> allowed) {
        if (rawBody == null || rawBody.isEmpty() || rawBody.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("候选请求包含空体或未允许字段。");
        try { return json.convertValue(rawBody, type); }
        catch (IllegalArgumentException invalid) { throw EafException.invalid("候选请求字段格式无效。"); }
    }

    record CandidateBody(String targetType, UUID targetId, String baseVersion, JsonNode proposedContent,
                         List<String> evidenceRefs, UUID sourceFeedbackId) { }
    record CandidateRevisionBody(String targetType, UUID targetId, String baseVersion,
                                 JsonNode proposedContent, List<String> evidenceRefs) { }
    record ReviewBody(String decision, String reason, List<String> factEvidenceRefs) { }
    record ApprovalBody(String decision, String reason, UUID evaluationReportId) { }
    record PublishBody(UUID approvalId) { }
    record WithdrawalBody(String reasonRef) { }
    record IterationBody(UUID taskId) { }
}
// 协议层只解析候选命令；Learning 校验来源、修订和审核，目标领域仍独占正式资产写入。
