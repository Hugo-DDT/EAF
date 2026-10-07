package io.eaf.task.api;

import java.util.List;

public interface TaskRunner {
    RunOutcome run(TaskWorkItem workItem);

    default void recoverOnStartup() { }

    // 只收敛 Task 域本轮确认过期的 attempt，避免误改其他实例的活跃运行。
    default void recoverExpired(List<TaskAttemptRecovery> expired) { recoverOnStartup(); }

    record RunOutcome(TaskStatus status, String resultJson, String errorCode, String errorDetail,
                      boolean modelCalled, Integer inputTokens, Integer outputTokens,
                      int modelCalls, int toolCalls, int toolExecutions) {
        public RunOutcome(TaskStatus status, String resultJson, String errorCode, String errorDetail,
                          boolean modelCalled, Integer inputTokens, Integer outputTokens) {
            this(status, resultJson, errorCode, errorDetail, modelCalled, inputTokens, outputTokens,
                    modelCalled ? 1 : 0, 0, 0);
        }
        public static RunOutcome success(String resultJson) { return success(resultJson, true, null, null); }
        public static RunOutcome success(String resultJson, boolean modelCalled, Integer inputTokens, Integer outputTokens) {
            return new RunOutcome(TaskStatus.SUCCEEDED, resultJson, null, null, modelCalled, inputTokens, outputTokens, modelCalled ? 1 : 0, 0, 0);
        }
        public static RunOutcome failed(String code, String detail) { return failed(code, detail, false, null, null); }
        public static RunOutcome failed(String code, String detail, boolean modelCalled, Integer inputTokens, Integer outputTokens) {
            return new RunOutcome(TaskStatus.FAILED, null, code, detail, modelCalled, inputTokens, outputTokens);
        }
        public static RunOutcome timedOut(String code, String detail) { return timedOut(code, detail, false, null, null); }
        public static RunOutcome timedOut(String code, String detail, boolean modelCalled, Integer inputTokens, Integer outputTokens) {
            return new RunOutcome(TaskStatus.TIMED_OUT, null, code, detail, modelCalled, inputTokens, outputTokens);
        }
        public static RunOutcome waiting(TaskStatus status, String code, String detail, String resultJson) {
            if (status != TaskStatus.WAITING_APPROVAL && status != TaskStatus.WAITING_VERIFICATION
                    && status != TaskStatus.WAITING_REMOTE)
                throw new IllegalArgumentException("等待状态必须是审批、核验或远端等待。");
            return new RunOutcome(status, resultJson, code, detail, false, null, null, 0, 0, 0);
        }
    }
}
// 本文件负责实现 EAF 的 TaskRunner.java 相关代码。
