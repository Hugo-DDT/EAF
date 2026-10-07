package io.eaf.identity.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public record CreateDelegationCommand(ActorContext owner, UUID workspaceId, UUID delegateId,
                                      Set<String> actions, Set<String> customerIds, Instant expiresAt) {
    public CreateDelegationCommand {
        actions = actions == null ? Set.of() : Collections.unmodifiableSet(new HashSet<>(actions));
        customerIds = customerIds == null ? Set.of() : Collections.unmodifiableSet(new HashSet<>(customerIds));
    }
}
// 委托签发命令只接收已认证 Owner；tenant、audience 与委托状态由 Identity 服务派生。
