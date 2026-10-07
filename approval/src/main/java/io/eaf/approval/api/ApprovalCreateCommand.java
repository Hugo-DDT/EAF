package io.eaf.approval.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** 创建审批请求只记录事实，不执行外部工具。 */
public record ApprovalCreateCommand(ActorContext actor, UUID workspaceId, UUID taskId,
                                    ApprovalBinding binding, String idempotencyKey) { }
