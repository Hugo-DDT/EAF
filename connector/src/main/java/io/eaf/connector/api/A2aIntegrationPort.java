package io.eaf.connector.api;

import java.time.Instant;

/** Integration 承担 A2A HTTP/JSON-RPC 适配，不把凭据或任意 URL 暴露给 Runtime。 */
public interface A2aIntegrationPort {
    RemoteA2aResult sendTask(ConnectorDefinition connector, String rpcId, String skillId,
                             String messageId, String text, Instant deadline);
    RemoteA2aResult getTask(ConnectorDefinition connector, String rpcId, String taskId, Instant deadline);
    // 仅通过已绑定 Connector 对原远端 Task 发取消，不接受模型提供的目标地址。
    RemoteA2aResult cancelTask(ConnectorDefinition connector, String rpcId, String taskId, Instant deadline);
}
// 本文件负责定义固定远端 A2A 调用端口。
