package io.eaf.task.api;

import java.util.List;

/** Task 域按当前授权范围返回的一页查询结果。 */
public record TaskPage(List<TaskSnapshot> items, long totalSize, TaskPageCursor nextCursor) { }
