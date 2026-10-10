package io.eaf.workflow.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record AutomationRunPage(List<AutomationRun> items, Instant nextCreatedAt, UUID nextId) {
    public AutomationRunPage { items = items == null ? List.of() : List.copyOf(items); }
}
