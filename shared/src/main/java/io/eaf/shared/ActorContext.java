package io.eaf.shared;

import java.util.Set;
import java.util.UUID;

public record ActorContext(UUID actorId, UUID tenantId, ActorType type, Set<String> actions,
                           UUID principalId, UUID delegationId, UUID delegationWorkspaceId, String authorizationHash) {
    // 旧调用方只创建直接身份；委托身份必须带齐由 Identity 校验的主体、范围与摘要。
    public ActorContext(UUID actorId, UUID tenantId, ActorType type, Set<String> actions) {
        this(actorId, tenantId, type, actions, null, null, null, null);
    }

    public ActorContext {
        if (actorId == null || tenantId == null || type == null) throw new IllegalArgumentException("身份主体字段不能为空。");
        actions = actions == null ? Set.of() : Set.copyOf(actions);
        if (delegationId == null && (principalId != null || delegationWorkspaceId != null || authorizationHash != null))
            throw new IllegalArgumentException("委托身份字段必须成组提供。");
        if (delegationId != null && (principalId == null || delegationWorkspaceId == null
                || authorizationHash == null || !authorizationHash.matches("[0-9a-f]{64}") || type != ActorType.AGENT))
            throw new IllegalArgumentException("有效委托身份缺少绑定字段。");
    }

    public UUID principalIdOrActorId() { return principalId == null ? actorId : principalId; }
    public boolean delegated() { return delegationId != null; }
    public boolean can(String action) { return actions.contains(action); }
}
// 本文件负责实现 EAF 的 ActorContext.java 相关代码。
