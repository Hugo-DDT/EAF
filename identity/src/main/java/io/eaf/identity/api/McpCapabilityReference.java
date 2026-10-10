package io.eaf.identity.api;

import java.util.UUID;

public record McpCapabilityReference(UUID id, String version, String contentHash) { }
