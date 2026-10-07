package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskAssetBinding;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Task 协议响应只由应用服务创建，并在每条响应路径显式绑定结果可见性。 */
public record TaskResponse(UUID id, String status, int attempt, long version, String traceId,
                           UUID agentId, String agentVersion, String promptVersion, String input,
                           JsonNode result, String errorCode, String errorDetail, String source,
                           Instant createdAt, Instant updatedAt, TaskAssetBinding assetBinding,
                           UUID rootTaskId, UUID parentTaskId, String entryProtocol, String runKind,
                           List<io.eaf.task.api.TaskStepView> steps) {
    static TaskResponse of(TaskSnapshot task, ObjectMapper mapper,
                           List<io.eaf.task.api.TaskStepView> steps, boolean exposeResult) {
        return new TaskResponse(task.id(), task.status().name(), task.attempt(), task.version(), task.traceId(), task.agentId(), task.agentVersion(), task.promptVersion(), task.inputText(), parse(mapper, exposeResult ? task.resultJson() : null), task.errorCode(), task.errorDetail(), task.source(), task.createdAt(), task.updatedAt(), task.assetBinding(), task.rootTaskId(), task.parentTaskId(), task.entryProtocol(), task.runKind(), steps);
    }
    private static JsonNode parse(ObjectMapper mapper, String text) { try { return text == null ? null : mapper.readTree(text); } catch (Exception e) { return null; } }
}
// 本文件负责实现 EAF 的 TaskResponse.java 相关代码。
