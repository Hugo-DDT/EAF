package io.eaf.workflow.api;

import java.util.List;
import java.util.UUID;

public record AutomationTaskSource(UUID runId, UUID subscriptionId, long authorizationEpoch,
        String inputSnapshotJson, String inputHash, String profileHash, boolean generationAttempted,
        String resultJson, List<AutomationDigestItem> items) {
    public AutomationTaskSource { items = items == null ? List.of() : List.copyOf(items); }
}
