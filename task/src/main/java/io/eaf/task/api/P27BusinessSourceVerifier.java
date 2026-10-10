package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** Workflow Owner verifies fixed P27 Tool task provenance without exposing Workflow tables. */
public interface P27BusinessSourceVerifier {
    P27BusinessTaskSource requireToolSource(ActorContext actor, UUID workspaceId, UUID taskId, int attempt,
            String toolName, String toolVersion, String toolArgumentsJson);
}
