package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.execution.api.ExecutionCommand;
import io.eaf.execution.api.ExecutionService;
import io.eaf.execution.api.ExecutionSnapshot;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/executions")
public class ExecutionController {
    private final ExecutionService executions;
    private final ObjectMapper json;

    public ExecutionController(ExecutionService executions, ObjectMapper json) { this.executions = executions; this.json = json; }

    @PostMapping
    ResponseEntity<ExecutionSnapshot> submit(@PathVariable UUID workspaceId, @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                             @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
                                             @RequestBody Body body, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var snapshot = executions.submit(new ExecutionCommand(actor, workspaceId, body.taskId(), body.attempt(), body.agentId(), body.agentVersion(), body.toolName(), body.toolVersion(), body.arguments() == null ? null : body.arguments().toString(), idempotencyKey, traceId));
        return ResponseEntity.accepted().header("Location", "/api/v1/workspaces/%s/executions/%s".formatted(workspaceId, snapshot.id())).body(snapshot);
    }

    @GetMapping("/{executionId}")
    ExecutionSnapshot get(@PathVariable UUID workspaceId, @PathVariable UUID executionId, Authentication authentication) {
        return executions.get(ApiSupport.actor(authentication), workspaceId, executionId);
    }

    @PostMapping("/{executionId}/verify")
    ExecutionSnapshot verify(@PathVariable UUID workspaceId, @PathVariable UUID executionId, Authentication authentication) {
        return executions.verify(ApiSupport.actor(authentication), workspaceId, executionId);
    }

    record Body(UUID taskId, int attempt, UUID agentId, String agentVersion, String toolName, String toolVersion, JsonNode arguments) { }
}
// 本文件负责实现 EAF 的 ExecutionController.java 相关代码。
