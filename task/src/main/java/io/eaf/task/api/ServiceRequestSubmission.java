package io.eaf.task.api;

import java.time.Instant;
import java.util.UUID;

/** Immutable link between a confirmed analysis result and its fixed registration workflow. */
public record ServiceRequestSubmission(UUID id, UUID sourceTaskId, UUID ownerId, long sourceTaskVersion,
                                       String sourceResultHash, String payloadJson, String contextRefsJson,
                                       UUID workflowInstanceId, Instant createdAt) { }
