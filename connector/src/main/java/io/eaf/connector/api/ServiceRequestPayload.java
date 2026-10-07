package io.eaf.connector.api;

import java.util.UUID;

public record ServiceRequestPayload(String operationId, String requesterId, String category, String title,
                                    String summary, String handlingSuggestion, UUID sourceTaskId,
                                    String sourceResultHash) { }
