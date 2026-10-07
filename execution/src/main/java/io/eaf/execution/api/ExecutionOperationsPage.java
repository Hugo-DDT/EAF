package io.eaf.execution.api;

import java.util.List;

/** Execution Owner 按当前 Workspace 授权返回的运行摘要页。 */
public record ExecutionOperationsPage(List<ExecutionOperationsItem> items, long totalSize,
                                      ExecutionOperationsCursor nextCursor) { }
