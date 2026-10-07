package io.eaf.task.api;

/** Task 域向 Execution 调度器返回远端等待是否成功转入本地队列。 */
public enum RemoteTaskWakeStatus { WOKEN, CAPACITY_DEFERRED, EXPIRED, TERMINAL, NOT_WAITING }
// 本文件负责实现远端等待 Task 的唤醒结果。
