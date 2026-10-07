package io.eaf.task.api;

/** 执行门返回数据库中的任务来源，供后续 Policy/Connector 选择隔离路径。 */
public record TaskExecutionCheck(boolean allowed, String code, String detail, String source) { }
// 本文件负责实现 EAF 的 TaskExecutionCheck.java 相关代码。
