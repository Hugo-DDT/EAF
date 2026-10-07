package io.eaf.workflow.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

// Owner 从可信身份上下文取得，不能由请求体冒充。
public record CreateWorkflowCommand(ActorContext actor, UUID workspaceId, String name, String description,
                                    CreateWorkflowVersionCommand initialVersion) { }
