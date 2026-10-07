package io.eaf.connector.api;

import java.time.Instant;
import java.util.UUID;

public record ServiceRequestReceipt(String requestId, String operationId, String status, String requesterId,
                                    String category, String title, String summary, String handlingSuggestion,
                                    UUID sourceTaskId, String sourceResultHash, Instant acceptedAt) { }
