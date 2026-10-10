package io.eaf.workflow.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record AutomationSubscriptionPage(List<AutomationSubscription> items, Instant nextCreatedAt, UUID nextId) {
    public AutomationSubscriptionPage { items = items == null ? List.of() : List.copyOf(items); }
}
