package io.eaf.approval.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** 审批决定必须带 expectedVersion，防止两个审批人覆盖彼此的终态。 */
public record ApprovalDecisionCommand(ActorContext actor, UUID workspaceId, UUID approvalId,
                                      String decision, long expectedVersion) { }
