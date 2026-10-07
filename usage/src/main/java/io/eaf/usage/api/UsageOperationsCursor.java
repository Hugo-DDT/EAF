package io.eaf.usage.api;

import java.time.Instant;
import java.util.UUID;

/** Usage 运维列表使用的稳定开始时间游标。 */
public record UsageOperationsCursor(Instant startedAt, UUID usageId) { }
