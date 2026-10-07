package io.eaf.task.api;

import java.time.Instant;
import java.util.UUID;

/** Task 域游标只携带排序位置，不携带可绕过 Workspace 授权的身份信息。 */
public record TaskPageCursor(Instant updatedAt, UUID taskId) { }
