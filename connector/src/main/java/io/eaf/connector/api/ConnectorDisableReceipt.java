package io.eaf.connector.api;

import java.time.Instant;
import java.util.UUID;

/** Connector Owner 返回的停用命令回执，不包含连接目标或凭据内容。 */
public record ConnectorDisableReceipt(UUID commandId, UUID connectorId, String status,
                                       long version, Instant createdAt, boolean replayed) { }
