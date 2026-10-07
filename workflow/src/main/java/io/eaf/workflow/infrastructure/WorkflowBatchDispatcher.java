package io.eaf.workflow.infrastructure;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** 批次 Worker 只分配有界 Workflow，不执行模型调用。 */
@Component
public class WorkflowBatchDispatcher {
    private final JdbcWorkflowService workflows;
    private final boolean enabled;

    public WorkflowBatchDispatcher(JdbcWorkflowService workflows,
            @Value("${eaf.workflow.dispatcher-enabled:true}") boolean enabled) {
        this.workflows = workflows;
        this.enabled = enabled;
    }

    @Scheduled(fixedDelayString = "${eaf.workflow.batch-poll-delay:1000}")
    public void dispatch() { if (enabled) workflows.dispatchBatchOne(); }
}
