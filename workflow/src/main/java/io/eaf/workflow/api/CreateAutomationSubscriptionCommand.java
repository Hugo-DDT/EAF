package io.eaf.workflow.api;

import io.eaf.shared.ActorContext;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.util.UUID;

public record CreateAutomationSubscriptionCommand(ActorContext actor, UUID workspaceId, String name,
        String triggerKind, DayOfWeek dayOfWeek, LocalTime localTime, String timeZone, UUID workItemId,
        int maxItems, Instant expiresAt, int maxRuns, String idempotencyKey) { }
