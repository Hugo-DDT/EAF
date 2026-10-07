package io.eaf.agentruntime.api;

import java.util.List;

/** unavailable=true 表示快照来源已失效，正文整体隐藏。 */
public record TaskSources(boolean unavailable, List<TaskSourceContent> items) { }
