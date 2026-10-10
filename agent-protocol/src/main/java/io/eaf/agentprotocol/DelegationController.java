package io.eaf.agentprotocol;

import io.eaf.identity.api.CreateDelegationCommand;
import io.eaf.identity.api.CreateMcpReadonlyDelegationCommand;
import io.eaf.identity.api.IdentityService;
import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
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

    @PostMapping("/mcp-readonly")
    ResponseEntity<McpDelegationResponse> createMcpReadonly(@PathVariable UUID workspaceId,
                                                            @RequestBody JsonNode body,
                                                            Authentication authentication) {
        var fields = Set.of("delegateId", "capabilityId", "capabilityVersion", "knowledgeDocumentIds", "expiresAt");
        if (body == null || !body.isObject()) throw io.eaf.shared.EafException.invalid("MCP 委托请求体必须是 JSON 对象。");
        var names = new HashSet<String>();
        body.fieldNames().forEachRemaining(names::add);
        if (!names.equals(fields)) throw io.eaf.shared.EafException.invalid("MCP 委托只接受已定义的五个字段。");
        var delegateId = uuid(body, "delegateId");
        var capabilityId = uuid(body, "capabilityId");
        var capabilityVersion = text(body, "capabilityVersion");
        var expiresAt = instant(body, "expiresAt");
        var documentNode = body.path("knowledgeDocumentIds");
        if (!documentNode.isArray() || documentNode.size() == 0 || documentNode.size() > 10)
            throw io.eaf.shared.EafException.invalid("knowledgeDocumentIds 必须包含 1 至 10 个文档。");
        var documentIds = new HashSet<UUID>();
        for (var item : documentNode) {
            if (!item.isTextual() || !documentIds.add(parseUuid(item.asText(), "knowledgeDocumentIds")))
                throw io.eaf.shared.EafException.invalid("knowledgeDocumentIds 必须是不同的 UUID。");
        }
        var snapshot = identities.createMcpReadonlyDelegation(new CreateMcpReadonlyDelegationCommand(
                ApiSupport.actor(authentication), workspaceId, delegateId, capabilityId, capabilityVersion,
                documentIds, expiresAt));
        return ResponseEntity.status(HttpStatus.CREATED).body(McpDelegationResponse.of(snapshot));
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

    private static UUID uuid(JsonNode body, String field) {
        if (!body.path(field).isTextual()) throw io.eaf.shared.EafException.invalid(field + " 必须是 UUID。");
        return parseUuid(body.path(field).asText(), field);
    }

    private static UUID parseUuid(String value, String field) {
        try { return UUID.fromString(value); }
        catch (IllegalArgumentException malformed) { throw io.eaf.shared.EafException.invalid(field + " 必须是 UUID。"); }
    }

    private static String text(JsonNode body, String field) {
        if (!body.path(field).isTextual() || body.path(field).asText().isBlank())
            throw io.eaf.shared.EafException.invalid(field + " 不能为空。");
        return body.path(field).asText();
    }

    private static Instant instant(JsonNode body, String field) {
        if (!body.path(field).isTextual()) throw io.eaf.shared.EafException.invalid(field + " 必须是 ISO-8601 时间。");
        try { return Instant.parse(body.path(field).asText()); }
        catch (java.time.format.DateTimeParseException malformed) {
            throw io.eaf.shared.EafException.invalid(field + " 必须是 ISO-8601 时间。");
        }
    }

    record McpCapability(UUID id, String version, String contentHash) { }
    record McpDelegationResponse(UUID id, UUID workspaceId, UUID delegateId, String audience,
                                 Instant expiresAt, Instant revokedAt, String scopeHash, String profile,
                                 McpCapability capability, Set<UUID> knowledgeDocumentIds) {
        static McpDelegationResponse of(io.eaf.identity.api.McpReadonlyDelegationSnapshot snapshot) {
            var delegation = snapshot.delegation();
            return new McpDelegationResponse(delegation.id(), delegation.workspaceId(), delegation.delegateId(),
                    delegation.audience(), delegation.expiresAt(), delegation.revokedAt(), delegation.scopeHash(),
                    snapshot.profile(), new McpCapability(snapshot.capabilityId(), snapshot.capabilityVersion(),
                    snapshot.capabilityHash()), snapshot.knowledgeDocumentIds());
        }
    }
}
// Owner 通过 REST 显式签发或撤销短期委托；客户端不能声明 Owner、tenant 或 audience。
