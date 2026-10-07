package io.eaf.usage.api;

import java.util.List;

/** Usage Owner 按当前 Workspace 授权返回的调用计量页。 */
public record UsageOperationsPage(List<UsageOperationsItem> items, long totalSize,
                                  UsageOperationsCursor nextCursor) { }
