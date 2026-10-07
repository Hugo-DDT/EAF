package io.eaf.agentprotocol;

import io.eaf.connector.api.ConnectorDisableReceipt;
import io.eaf.connector.api.ConnectorService;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 协议层只转发指定 Connector 停用命令，权限、状态迁移和审计由 Connector Owner 负责。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/operations/connectors")
public class ConnectorOperationsController {
    private final ConnectorService connectors;

    public ConnectorOperationsController(ConnectorService connectors) {
        this.connectors = connectors;
    }

    @PutMapping("/{connectorId}/disable")
    ConnectorDisableReceipt disable(@PathVariable UUID workspaceId, @PathVariable UUID connectorId,
                                    @RequestHeader(value = "Idempotency-Key", required = false) String requestKey,
                                    @RequestBody ConnectorDisableBody body, Authentication authentication) {
        return connectors.disable(ApiSupport.actor(authentication), workspaceId, connectorId, requestKey, body.reason());
    }

    /** 原因只进入受控运维账本及其摘要审计，不回显到回执。 */
    public record ConnectorDisableBody(String reason) { }
}
