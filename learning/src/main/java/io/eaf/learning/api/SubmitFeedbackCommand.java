package io.eaf.learning.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** 员工提交的纠正；来源类型只由服务端从原 Task 派生。 */
public record SubmitFeedbackCommand(ActorContext actor, UUID workspaceId, UUID taskId,
                                    String correction, String evidence, UUID executionId,
                                    String idempotencyKey) { }
