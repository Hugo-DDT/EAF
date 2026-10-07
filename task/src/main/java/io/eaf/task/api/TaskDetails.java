package io.eaf.task.api;

import java.util.List;

public record TaskDetails(TaskSnapshot task, List<TaskStepView> steps) { }
// 本文件负责实现 EAF 的 TaskDetails.java 相关代码。
