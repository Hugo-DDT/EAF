package io.eaf.connector.api;

/** 远端协议调用区分已获回复、明确拒绝和无法确定的传输结果。 */
public enum RemoteA2aOutcome { TASK, REJECTED, UNKNOWN }
// 本文件负责实现远端 A2A 调用结果分类。
