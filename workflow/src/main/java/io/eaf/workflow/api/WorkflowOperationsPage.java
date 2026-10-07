package io.eaf.workflow.api;

import java.util.List;

/** Workflow Owner 按当前 Workspace 授权返回的运行摘要页。 */
public record WorkflowOperationsPage(List<WorkflowOperationsItem> items, long totalSize,
                                     WorkflowOperationsCursor nextCursor) { }
