package io.eaf.connector.api;

import java.time.Instant;
import java.util.Optional;

public interface ServiceRequestIntegrationPort {
    ServiceRequestWriteResult register(ConnectorDefinition connector, ServiceRequestPayload payload, Instant deadline);
    Optional<ServiceRequestReceipt> find(ConnectorDefinition connector, String operationId, Instant deadline);
}
