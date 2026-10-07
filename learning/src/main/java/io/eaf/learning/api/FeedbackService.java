package io.eaf.learning.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Learning 所有的反馈写入与查询边界；反馈本身不发布目标资产。 */
public interface FeedbackService {
    FeedbackSubmission submit(SubmitFeedbackCommand command);
    List<Feedback> list(ActorContext actor, UUID workspaceId, UUID taskId);
    /** 仅为个人经验整理读取本人可见的普通 USER Task 反馈，不解封原回答或知识正文。 */
    Feedback requireExperienceSource(ActorContext actor, UUID workspaceId, UUID feedbackId);
    // 当前未配置反馈消费者时明确标记 NOT_CONFIGURED，避免把 PENDING 当作投递器故障。
    FeedbackOutboxPage listOutboxOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                            Instant createdAfter, Instant cursorCreatedAt,
                                            UUID cursorEventId, int pageSize);
}
