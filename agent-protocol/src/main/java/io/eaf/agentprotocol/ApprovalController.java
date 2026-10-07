package io.eaf.agentprotocol;

import io.eaf.approval.api.ApprovalDecisionCommand;
import io.eaf.approval.api.ApprovalService;
import io.eaf.approval.api.ApprovalSnapshot;
import io.eaf.approval.api.ApprovalPendingPage;
import java.time.Instant;
import java.util.UUID;
import org.springframework.web.bind.annotation.*;

/** 审批 API 只改变审批事实，不能直接绕过 Execution 调用外部系统。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/approvals")
public class ApprovalController {
    private final ApprovalService approvals;

    public ApprovalController(ApprovalService approvals) { this.approvals = approvals; }

    @GetMapping("/{approvalId}")
    ApprovalSnapshot get(@PathVariable UUID workspaceId, @PathVariable UUID approvalId,
                         org.springframework.security.core.Authentication authentication) {
        return approvals.get(ApiSupport.actor(authentication), workspaceId, approvalId);
    }

    @GetMapping
    ApprovalPendingPage pending(@PathVariable UUID workspaceId,
                                @RequestParam(required = false) Instant cursorCreatedAt,
                                @RequestParam(required = false) UUID cursorId,
                                @RequestParam(defaultValue = "20") int limit,
                                org.springframework.security.core.Authentication authentication) {
        return approvals.listPending(ApiSupport.actor(authentication), workspaceId, cursorCreatedAt, cursorId, limit);
    }

    @PostMapping("/{approvalId}/decisions")
    ApprovalSnapshot decide(@PathVariable UUID workspaceId, @PathVariable UUID approvalId,
                            @RequestBody DecisionBody body,
                            org.springframework.security.core.Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        return approvals.decide(new ApprovalDecisionCommand(actor, workspaceId, approvalId, body.decision(), body.expectedVersion()));
    }

    record DecisionBody(String decision, long expectedVersion) { }
}
