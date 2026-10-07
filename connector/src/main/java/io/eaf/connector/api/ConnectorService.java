package io.eaf.connector.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.UUID;

public interface ConnectorService {
    /** 按 Owner API 停用指定 Workspace Connector；相同请求键只能重放原回执。 */
    ConnectorDisableReceipt disable(ActorContext actor, UUID workspaceId, UUID connectorId,
                                    String requestKey, String reason);
    ConnectorDefinition requireActive(UUID tenantId, UUID workspaceId);
    /** 只允许按已登记 provider 选择连接，不接受任务或模型传入的 endpoint。 */
    ConnectorDefinition requireActive(UUID tenantId, UUID workspaceId, String provider);
    /** 返回固定工具绑定对应的活动 Connector；未知绑定必须失败关闭。 */
    ConnectorDefinition requireActiveForTool(UUID tenantId, UUID workspaceId, String bindingRef);
    /** 生成不含秘密值的稳定 Connector 目标/权限快照版本。 */
    String bindingVersion(ConnectorDefinition connector);
    CustomerRecord readCustomer(UUID tenantId, UUID workspaceId, String customerId, Instant deadline);
    /** Tool bindingRef 只能从固定映射中选择已登记 Provider；它不是 URL 或客户可控目标。 */
    CustomerRecord readCustomer(UUID tenantId, UUID workspaceId, String bindingRef, String customerId, Instant deadline);
    // EVALUATION 只能读合成数据，不接触 Workspace 的常规连接。
    CustomerRecord readCustomerForEvaluation(UUID tenantId, UUID workspaceId, String customerId, Instant readAt);
    ExternalWriteResult createFollowup(UUID tenantId, UUID workspaceId, String operationId, String customerId,
                                       String summary, String ownerId, Instant deadline);
    /** 写入仅使用 Tool 固定 bindingRef，并在出站前比较审批时保存的目标快照。 */
    ExternalWriteResult createFollowup(UUID tenantId, UUID workspaceId, String bindingRef, String expectedBindingVersion,
                                       String operationId, String customerId, String summary, String ownerId, Instant deadline);
    java.util.Optional<FollowupRecord> findFollowup(UUID tenantId, UUID workspaceId, String operationId, Instant deadline);
    /** 回读与写入共享绑定和目标快照，避免目标变更后误查另一个系统。 */
    java.util.Optional<FollowupRecord> findFollowup(UUID tenantId, UUID workspaceId, String bindingRef,
                                                    String expectedBindingVersion, String operationId, Instant deadline);
    /** 结果追加使用独立固定绑定和凭据用途；目标快照须匹配审批时值。 */
    ExternalOutcomeWriteResult recordFollowupOutcome(UUID tenantId, UUID workspaceId, String bindingRef,
            String expectedBindingVersion, String operationId, FollowupOutcomeRecord outcome, Instant deadline);
    java.util.Optional<FollowupOutcomeRecord> findFollowupOutcome(UUID tenantId, UUID workspaceId, String bindingRef,
            String expectedBindingVersion, String operationId, Instant deadline);
    ServiceRequestWriteResult registerServiceRequest(UUID tenantId, UUID workspaceId, String bindingRef,
            String expectedBindingVersion, ServiceRequestPayload payload, Instant deadline);
    java.util.Optional<ServiceRequestReceipt> findServiceRequest(UUID tenantId, UUID workspaceId, String bindingRef,
            String expectedBindingVersion, String operationId, Instant deadline);
    // 只允许传入已登记的 A2A Connector ID；用途和凭据范围由 Connector/Integration 再校验。
    RemoteA2aResult sendA2aTask(UUID tenantId, UUID workspaceId, UUID connectorId, String rpcId,
                                String skillId, String messageId, String text, Instant deadline);
    RemoteA2aResult getA2aTask(UUID tenantId, UUID workspaceId, UUID connectorId, String rpcId,
                               String remoteTaskId, Instant deadline);
    // 独立 a2a.cancel 用途避免将取消请求混入 SendMessage 或普通轮询权限。
    RemoteA2aResult cancelA2aTask(UUID tenantId, UUID workspaceId, UUID connectorId, String rpcId,
                                  String remoteTaskId, Instant deadline);
}
// 本文件负责实现 EAF 的 ConnectorService.java 相关代码。
