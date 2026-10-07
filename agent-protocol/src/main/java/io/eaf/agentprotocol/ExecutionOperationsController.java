package io.eaf.agentprotocol;

import io.eaf.execution.api.ExecutionService;
import io.eaf.execution.api.ExecutionVerificationReceipt;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 将可审计核验请求路由到 Execution Owner，不接受调用方提交新的 operationId。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/operations/executions")
public class ExecutionOperationsController {
    private final ExecutionService executions;

    public ExecutionOperationsController(ExecutionService executions) {
        this.executions = executions;
    }

    @PostMapping("/{executionId}/verify")
    ExecutionVerificationReceipt verify(@PathVariable UUID workspaceId, @PathVariable UUID executionId,
                                        @RequestHeader(value = "Idempotency-Key", required = false) String requestKey,
                                        @RequestBody VerifyBody body, Authentication authentication) {
        return executions.verifyOperational(ApiSupport.actor(authentication), workspaceId, executionId,
                requestKey, body.reason());
    }

    /** 核验原因进入本域受控命令账本，不回显参数、结果正文或原因。 */
    public record VerifyBody(String reason) { }
}
