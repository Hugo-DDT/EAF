package io.eaf.task.infrastructure;

import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskExecutionService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class TaskDispatcher {
    private final TaskService tasks;
    private final TaskRunner runner;
    private final TaskExecutionService execution;
    private final boolean enabled;
    private final int maxConcurrent;

    public TaskDispatcher(TaskService tasks, TaskRunner runner, TaskExecutionService execution,
                          @Value("${eaf.task.dispatcher-enabled:true}") boolean enabled,
                          @Value("${eaf.task.max-concurrent:8}") int maxConcurrent) {
        this.tasks = tasks; this.runner = runner; this.execution = execution; this.enabled = enabled;
        if (maxConcurrent <= 0) throw new IllegalArgumentException("eaf.task.max-concurrent must be positive.");
        this.maxConcurrent = maxConcurrent;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void recover() {
        recoverExpired();
    }

    @Scheduled(fixedDelayString = "${eaf.task.recovery-delay:1000}")
    public void recoverExpired() {
        if (!enabled) return;
        runner.recoverExpired(tasks.recoverOnStartup());
    }

    @Scheduled(fixedDelayString = "${eaf.task.poll-delay:100}")
    public void dispatch() {
        if (!enabled) return;
        for (var i = 0; i < maxConcurrent; i++) if (!execution.dispatchNext()) break;
    }

}
// 本文件负责实现 EAF 的 TaskDispatcher.java 相关代码。
