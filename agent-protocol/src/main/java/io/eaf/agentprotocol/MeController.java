package io.eaf.agentprotocol;

import io.eaf.shared.ActorContext;
import java.util.Set;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/me")
public class MeController {
    @GetMapping
    MeResponse me(Authentication authentication) {
        ActorContext actor = ApiSupport.actor(authentication);
        return new MeResponse(actor.actorId(), actor.tenantId(), actor.type().name(), Set.copyOf(actor.actions()));
    }
    record MeResponse(java.util.UUID actorId, java.util.UUID tenantId, String type, Set<String> actions) { }
}
// 本文件负责实现 EAF 的 MeController.java 相关代码。
