package io.eaf.agentprotocol;

import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import org.springframework.security.core.Authentication;

final class ApiSupport {
    private ApiSupport() { }
    static ActorContext actor(Authentication authentication) {
        if (authentication == null || !(authentication.getPrincipal() instanceof ActorContext actor)) throw EafException.unauthenticated();
        return actor;
    }
}
// 本文件负责实现 EAF 的 ApiSupport.java 相关代码。
