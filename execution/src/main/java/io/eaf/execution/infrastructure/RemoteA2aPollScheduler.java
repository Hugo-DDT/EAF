package io.eaf.execution.infrastructure;

import io.eaf.execution.api.ExecutionService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时器只唤醒到期 Task；远端 GetTask 保持在 Runtime 持有 Task 租约时执行。 */
@Component
@ConditionalOnProperty(name = "eaf.execution.remote-poller-enabled", havingValue = "true", matchIfMissing = true)
public class RemoteA2aPollScheduler {
    private final ExecutionService executions;

    public RemoteA2aPollScheduler(ExecutionService executions) { this.executions = executions; }

    @Scheduled(fixedDelayString = "${eaf.execution.remote-poll-delay:1000}")
    public void wakeDueTasks() { executions.wakeDueRemoteTasks(); }
}
