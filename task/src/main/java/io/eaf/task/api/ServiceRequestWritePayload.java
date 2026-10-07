package io.eaf.task.api;

import java.util.UUID;

/** Server-verified registration payload expanded from an immutable submission. */
public record ServiceRequestWritePayload(UUID submissionId, UUID requesterId, UUID sourceTaskId,
                                         String sourceResultHash, String category, String title,
                                         String summary, String handlingSuggestion, String contextRefsJson) { }
