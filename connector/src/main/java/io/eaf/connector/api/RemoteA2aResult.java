package io.eaf.connector.api;

/** Connector 返回协议层已核验的 Task 外壳；业务字段仍由 Execution 按固定 Schema 校验。 */
public record RemoteA2aResult(RemoteA2aOutcome outcome, String taskId, String contextId,
                              String state, String taskJson, String errorCode) { }
// 本文件负责实现远端 A2A Task 响应的最小投影。
