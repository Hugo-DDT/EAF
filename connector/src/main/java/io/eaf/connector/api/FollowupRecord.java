package io.eaf.connector.api;

import java.time.Instant;

public record FollowupRecord(String operationId, String externalId, String customerId,
                             String summary, String ownerId, String status, Instant acceptedAt) { }
