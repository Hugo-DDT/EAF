package io.eaf.task.api;

/** WAITING_REMOTE 与 CANCELLING_REMOTE 都会释放本地 Worker；后者仍待 peer 明确取消。 */
public enum TaskStatus { QUEUED, RUNNING, WAITING_APPROVAL, WAITING_VERIFICATION, WAITING_REMOTE, CANCELLING_REMOTE, SUCCEEDED, FAILED, TIMED_OUT, CANCELLED }
// 本文件负责实现 EAF 的 TaskStatus.java 相关代码。
