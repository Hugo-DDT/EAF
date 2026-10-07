package io.eaf.memory.api;

import java.time.Instant;
import java.util.UUID;

/** Memory Outbox Owner 仅返回原事件标识、状态和累计尝试次数。 */
public record MemoryOutboxReplayReceipt(UUID commandId, UUID eventId, String status,
                                        int attempts, Instant createdAt, boolean replayed) { }
