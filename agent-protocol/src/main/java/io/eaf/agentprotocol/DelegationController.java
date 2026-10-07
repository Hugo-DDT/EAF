package io.eaf.agentprotocol;

import io.eaf.identity.api.CreateDelegationCommand;
import io.eaf.identity.api.IdentityService;
import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/delegations")
public class DelegationController {
    private final IdentityService identities;

    public DelegationController(IdentityService identities) {
        this.identities = identities;
    }

    @PostMapping
    ResponseEntity<DelegationResponse> create(@PathVariable UUID workspaceId, @RequestBody CreateBody body,
                                               Authentication authentication) {
        if (body == null) throw io.eaf.shared.EafException.invalid("委托请求体不能为空。");
        var snapshot = identities.createDelegation(new CreateDelegationCommand(ApiSupport.actor(authentication),
                workspaceId, body.delegateId(), body.actions(), body.customerIds(), body.expiresAt()));
        return ResponseEntity.status(HttpStatus.CREATED).body(DelegationResponse.of(snapshot));
    }

    @PostMapping("/{delegationId}/revoke")
    DelegationResponse revoke(@PathVariable UUID workspaceId, @PathVariable UUID delegationId,
                              Authentication authentication) {
        return DelegationResponse.of(identities.revokeDelegation(ApiSupport.actor(authentication), workspaceId, delegationId));
    }

    record CreateBody(UUID delegateId, Set<String> actions, Set<String> customerIds, Instant expiresAt) { }
    record DelegationResponse(UUID id, UUID workspaceId, UUID delegateId, String audience,
                              Instant expiresAt, Instant revokedAt, String scopeHash) {
        static DelegationResponse of(io.eaf.identity.api.DelegationSnapshot snapshot) {
            return new DelegationResponse(snapshot.id(), snapshot.workspaceId(), snapshot.delegateId(),
                    snapshot.audience(), snapshot.expiresAt(), snapshot.revokedAt(), snapshot.scopeHash());
        }
    }
}
// Owner 通过 REST 显式签发或撤销短期委托；客户端不能声明 Owner、tenant 或 audience。
