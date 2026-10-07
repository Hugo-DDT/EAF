package io.eaf.agentprotocol;

import io.eaf.execution.api.ExecutionOutboxReplayReceipt;
import io.eaf.execution.api.ExecutionService;
import io.eaf.knowledge.api.KnowledgeOutboxReplayReceipt;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.memory.api.MemoryOutboxReplayReceipt;
import io.eaf.memory.api.MemoryService;
import io.eaf.shared.EafException;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 按显式 Owner 名称路由 FAILED Outbox 重投；此协议适配器不写入任何领域表。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/operations/outboxes")
public class OutboxReplayOperationsController {
    private final ExecutionService executions;
    private final KnowledgeService knowledge;
    private final MemoryService memories;

    public OutboxReplayOperationsController(ExecutionService executions, KnowledgeService knowledge,
                                            MemoryService memories) {
        this.executions = executions;
        this.knowledge = knowledge;
        this.memories = memories;
    }

    @PostMapping("/{owner}/{eventId}/replay")
    Object replay(@PathVariable UUID workspaceId, @PathVariable String owner, @PathVariable UUID eventId,
                  @RequestHeader(value = "Idempotency-Key", required = false) String requestKey,
                  @RequestBody ReplayBody body, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        return switch (owner) {
            case "execution" -> executions.replayOutbox(actor, workspaceId, eventId, requestKey, body.reason());
            case "knowledge" -> knowledge.replayOutbox(actor, workspaceId, eventId, requestKey, body.reason());
            case "memory" -> memories.replayOutbox(actor, workspaceId, eventId, requestKey, body.reason());
            default -> throw EafException.notFound();
        };
    }

    /** 原因由 Owner 保存并摘要审计，响应只携带有限处置回执。 */
    public record ReplayBody(String reason) { }
}
