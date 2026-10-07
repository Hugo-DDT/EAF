package io.eaf.learning.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Learning Owner 返回反馈 outbox 页及消费者是否已配置的真实状态。 */
public record FeedbackOutboxPage(List<FeedbackOutboxItem> items, long totalSize,
                                 Instant nextCreatedAt, UUID nextEventId,
                                 String consumerState, boolean pendingIsFailure) { }
