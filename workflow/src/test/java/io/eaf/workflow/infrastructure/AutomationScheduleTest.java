package io.eaf.workflow.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.eaf.shared.EafException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class AutomationScheduleTest {
    @Test
    void daylightSavingGapMovesToFirstValidLocalTime() {
        var slot = JdbcWorkflowAutomationService.nextSlotAfter(Instant.parse("2026-03-28T12:00:00Z"),
                DayOfWeek.SUNDAY, LocalTime.of(2, 30), ZoneId.of("Europe/Paris"));

        assertEquals(Instant.parse("2026-03-29T01:30:00Z"), slot.instant());
    }

    @Test
    void daylightSavingOverlapUsesOneStableOccurrence() {
        var slot = JdbcWorkflowAutomationService.nextSlotAfter(Instant.parse("2026-10-24T12:00:00Z"),
                DayOfWeek.SUNDAY, LocalTime.of(2, 30), ZoneId.of("Europe/Paris"));

        assertEquals(Instant.parse("2026-10-25T00:30:00Z"), slot.instant());
    }

    @Test
    void exactSlotDoesNotCreateASecondOccurrenceForTheSameLocalDate() {
        var slot = JdbcWorkflowAutomationService.nextSlotAfter(Instant.parse("2026-10-25T00:30:00Z"),
                DayOfWeek.SUNDAY, LocalTime.of(2, 30), ZoneId.of("Europe/Paris"));

        assertEquals(Instant.parse("2026-11-01T01:30:00Z"), slot.instant());
    }

    @Test
    void digestMarkdownEscapesModelLinksAndInlineMarkup() {
        assertEquals("\\[open\\]\\(https://example.org\\) &lt;tag&gt; \\`code\\`",
                JdbcWorkflowAutomationService.markdown("[open](https://example.org) <tag> `code`"));
    }

    @Test
    void permissionFailureIsReportedAsAuthorizationRevoked() {
        assertEquals("AUTHORIZATION_REVOKED", JdbcWorkflowAutomationService.safeBlockReason(
                EafException.forbidden("permission revoked")));
    }
}
