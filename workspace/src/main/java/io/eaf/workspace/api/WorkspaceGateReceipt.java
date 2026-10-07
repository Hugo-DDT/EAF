package io.eaf.workspace.api;

import java.time.Instant;
import java.util.UUID;

/** 不含原因正文的操作收据；重复请求返回首次提交的版本与时间。 */
public record WorkspaceGateReceipt(UUID commandId, boolean enabled, long version,
                                   Instant appliedAt, boolean replayed) { }
