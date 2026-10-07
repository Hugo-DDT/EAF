package io.eaf.learning.api;

import java.time.Instant;
import java.util.UUID;

/** Learning Owner 仅公开反馈事件的安全来源引用和投递状态。 */
public record FeedbackOutboxItem(UUID eventId, UUID taskId, String sourceType, String eventType,
                                 String status, Instant createdAt) { }
