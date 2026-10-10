package io.eaf.identity.api;

import io.eaf.shared.ActorContext;
import java.util.Optional;
import java.util.UUID;

public interface IdentityService {
    // 此受众固定给本地 REST 入口；调用方不能用请求字段改写受众。
    String REST_AUDIENCE = "eaf:rest";
    String MCP_AUDIENCE = "eaf:mcp";
    String MCP_READONLY_PROFILE = "MCP_SERVICE_REQUEST_READ_V1";

    // token 只识别主体；AGENT 还必须通过 resolveDelegatedToken 建立可信委托上下文。
    Optional<ActorContext> resolveToken(String token);
    // 外部 token 的 issuer/subject 必须先映射到服务端主体；tenant 与权限只从本地成员和授权读取。
    Optional<ActorContext> resolveExternalPrincipal(String issuer, String subject);
    // 委托必须同时绑定已认证的 Agent、Owner、Workspace 与服务端配置的 audience。
    Optional<ActorContext> resolveDelegatedToken(String token, UUID delegationId, String audience);
    Optional<ActorContext> resolveDelegation(UUID tenantId, UUID ownerId, UUID delegateId, UUID delegationId,
                                             UUID workspaceId, String audience);
    // 当前 CRM Policy 在提交读取前再次核对委托所绑定的客户资源。
    boolean allowsDelegatedCustomerRead(ActorContext actor, String customerId);
    // 签发前在当前授权交集中验证范围，撤销只更新委托状态。
    DelegationSnapshot createDelegation(CreateDelegationCommand command);
    McpReadonlyDelegationSnapshot createMcpReadonlyDelegation(CreateMcpReadonlyDelegationCommand command);
    Optional<McpReadonlyDelegationScope> mcpReadonlyScope(ActorContext actor);
    DelegationSnapshot revokeDelegation(ActorContext owner, UUID workspaceId, UUID delegationId);
}
// 本文件负责实现 EAF 的 IdentityService.java 相关代码。
