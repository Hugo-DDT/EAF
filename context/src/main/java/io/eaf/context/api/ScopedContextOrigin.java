package io.eaf.context.api;

import java.time.Instant;
import java.util.UUID;

public record ScopedContextOrigin(UUID sourceWorkspaceId, String scopeKind, String sourceRef,
                                  String contentHash, String accessPath,
                                  UUID documentId, Integer documentVersion, UUID chunkId, UUID buildId,
                                  Integer startOffset, Integer endOffset, String offsetUnit,
                                  UUID memoryId, String memoryVersion, String memoryScope, Instant expiresAt,
                                  UUID shareId, Long shareVersion) { }
