package io.eaf.connector.api;

import java.time.Instant;

public interface IntegrationPort {
    CustomerRecord readCustomer(ConnectorDefinition connector, String customerId, Instant deadline);
    ExternalWriteResult createFollowup(ConnectorDefinition connector, String operationId, String customerId,
                                       String summary, String ownerId, Instant deadline);
    java.util.Optional<FollowupRecord> findFollowup(ConnectorDefinition connector, String operationId, Instant deadline);
    ExternalOutcomeWriteResult recordFollowupOutcome(ConnectorDefinition connector, String operationId,
            FollowupOutcomeRecord outcome, Instant deadline);
    java.util.Optional<FollowupOutcomeRecord> findFollowupOutcome(ConnectorDefinition connector,
            String operationId, Instant deadline);
}
// 本文件负责实现 EAF 的 IntegrationPort.java 相关代码。
