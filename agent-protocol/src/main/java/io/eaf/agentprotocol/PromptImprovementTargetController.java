package io.eaf.agentprotocol;

import io.eaf.prompt.api.PromptOwnerService;
import io.eaf.prompt.api.PromptOwnerService.PromptTarget;
import io.eaf.shared.EafException;
import java.util.Map;
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
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/prompt-improvement-targets")
public class PromptImprovementTargetController {
    private final PromptOwnerService prompts;

    public PromptImprovementTargetController(PromptOwnerService prompts) { this.prompts = prompts; }

    @PostMapping
    ResponseEntity<PromptTarget> register(@PathVariable UUID workspaceId,
            @RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody(required = false) Map<String, Object> body,
            Authentication authentication) {
        if (body != null && !body.isEmpty()) throw EafException.invalid("固定 Prompt 目标登记不接受客户端基线或 Owner 字段。");
        var result = prompts.registerAnalysisTarget(ApiSupport.actor(authentication), workspaceId, idempotencyKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(result);
    }

    @GetMapping("/{targetId}")
    PromptTarget get(@PathVariable UUID workspaceId, @PathVariable UUID targetId, Authentication authentication) {
        return prompts.getTarget(ApiSupport.actor(authentication), workspaceId, targetId);
    }
}
