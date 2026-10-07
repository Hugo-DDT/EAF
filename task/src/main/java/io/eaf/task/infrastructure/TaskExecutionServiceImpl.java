package io.eaf.task.infrastructure;

import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.task.api.TaskExecutionService;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.task.api.TaskWorkItem;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** 把 Task 租约、Runtime、完成持久化和本地并发额度作为一个运行生命周期管理。 */
@Service
public final class TaskExecutionServiceImpl implements TaskExecutionService {
    private static final System.Logger LOG = System.getLogger(TaskExecutionServiceImpl.class.getName());

    private final TaskService tasks;
    private final TaskRunner runner;
    private final TaskOperationalMetrics metrics;
    private final Semaphore slots;
    private final Duration evaluationWait;
    private final Duration shutdownGrace;
    private final long heartbeatMillis;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeats = Executors.newScheduledThreadPool(2, daemonThreads());
    private final ConcurrentHashMap<RunKey, RunHandle> running = new ConcurrentHashMap<>();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicBoolean forcedShutdown = new AtomicBoolean();

    public TaskExecutionServiceImpl(TaskService tasks, TaskRunner runner, TaskOperationalMetrics metrics,
                                    @Value("${eaf.task.max-concurrent:8}") int maxConcurrent,
                                    @Value("${eaf.task.lease-duration:PT15S}") Duration leaseDuration,
                                    @Value("${eaf.task.evaluation-slot-wait:PT5S}") Duration evaluationWait,
                                    @Value("${eaf.task.shutdown-grace:PT30S}") Duration shutdownGrace) {
        if (maxConcurrent <= 0 || !positive(leaseDuration) || !positive(evaluationWait) || !positive(shutdownGrace))
            throw new IllegalArgumentException("eaf.task concurrency and durations must be positive.");
        this.tasks = tasks;
        this.runner = runner;
        this.metrics = metrics;
        this.slots = new Semaphore(maxConcurrent);
        this.evaluationWait = evaluationWait;
        this.shutdownGrace = shutdownGrace;
        this.heartbeatMillis = Math.max(100L, leaseDuration.toMillis() / 3);
    }

    @Override
    public boolean dispatchNext() {
        var slot = tryReserve();
        if (slot == null) return false;
        try {
            var work = tasks.claimOne();
            if (work.isEmpty()) {
                slot.release();
                return false;
            }
            launch(work.get(), slot);
            return true;
        } catch (RuntimeException failure) {
            slot.release();
            throw failure;
        }
    }

    @Override
    public TaskSnapshot executeEvaluation(ActorContext actor, UUID workspaceId, UUID taskId) {
        var initial = tasks.get(actor, workspaceId, taskId);
        requireEvaluation(initial);
        if (terminal(initial.status())) return initial;
        var slot = reserveForEvaluation();
        try {
            var current = tasks.get(actor, workspaceId, taskId);
            requireEvaluation(current);
            if (terminal(current.status())) return current;
            var work = tasks.claim(taskId).orElse(null);
            if (work == null) {
                current = tasks.get(actor, workspaceId, taskId);
                if (terminal(current.status())) return current;
                throw EafException.conflict("EVALUATION_TASK_BUSY", "稳定评测 Task 正由其他 Worker 执行。");
            }
            var handle = launch(work, slot);
            slot = null;
            try {
                handle.finished.get();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                handle.interrupt();
                throw executionIncomplete();
            } catch (ExecutionException incomplete) {
                throw executionIncomplete();
            }
            return tasks.get(actor, workspaceId, taskId);
        } finally {
            if (slot != null) slot.release();
        }
    }

    private void requireEvaluation(TaskSnapshot task) {
        if (!"EVALUATION".equals(task.source())) throw EafException.notFound();
    }

    private Slot tryReserve() {
        if (!accepting.get() || !slots.tryAcquire()) return null;
        metrics.executionStarted();
        return new Slot();
    }

    private Slot reserveForEvaluation() {
        if (!accepting.get()) throw taskExecutionBusy();
        try {
            if (!slots.tryAcquire(evaluationWait.toMillis(), TimeUnit.MILLISECONDS)) throw taskExecutionBusy();
            metrics.executionStarted();
            var slot = new Slot();
            if (!accepting.get()) {
                slot.release();
                throw taskExecutionBusy();
            }
            return slot;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw taskExecutionBusy();
        }
    }

    private RunHandle launch(TaskWorkItem work, Slot slot) {
        var handle = new RunHandle(work, slot);
        running.put(new RunKey(work.id(), work.attempt()), handle);
        try {
            handle.heartbeat = heartbeats.scheduleWithFixedDelay(handle::renew, heartbeatMillis,
                    heartbeatMillis, TimeUnit.MILLISECONDS);
            if (!accepting.get()) throw new RejectedExecutionException("Task execution is stopping.");
            executor.execute(handle);
        } catch (RuntimeException rejected) {
            handle.rejectBeforeStart();
        }
        return handle;
    }

    private void completeRejected(RunHandle handle) {
        try {
            tasks.complete(handle.work, TaskRunner.RunOutcome.failed("DISPATCH_REJECTED", "本地任务执行器未能接收已领取的任务。"));
            handle.finished.complete(null);
        } catch (RuntimeException persistenceFailure) {
            handle.finished.completeExceptionally(persistenceFailure);
        }
    }

    private EafException taskExecutionBusy() {
        return new EafException(429, "TASK_EXECUTION_BUSY", "本地任务执行容量已满，请稍后重试。", true);
    }

    private EafException executionIncomplete() {
        return new EafException(503, "TASK_EXECUTION_INCOMPLETE", "评测 Task 未能持久化完成，结果不可用于评分。", true);
    }

    private boolean terminal(TaskStatus status) {
        return status == TaskStatus.SUCCEEDED || status == TaskStatus.FAILED || status == TaskStatus.TIMED_OUT
                || status == TaskStatus.CANCELLED;
    }

    @PreDestroy
    public void shutdown() {
        if (!accepting.compareAndSet(true, false)) return;
        executor.shutdown();
        boolean drained = false;
        try {
            drained = executor.awaitTermination(shutdownGrace.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        if (!drained) {
            forcedShutdown.set(true);
            for (var handle : running.values()) handle.interrupt();
            for (var pending : executor.shutdownNow())
                if (pending instanceof TaskExecutionServiceImpl.RunHandle handle) handle.abandonBeforeStart();
            heartbeats.shutdownNow();
        } else {
            heartbeats.shutdown();
        }
    }

    private static boolean positive(Duration duration) {
        return duration != null && !duration.isNegative() && !duration.isZero();
    }

    private static ThreadFactory daemonThreads() {
        return task -> {
            var thread = new Thread(task, "eaf-task-lease-heartbeat");
            thread.setDaemon(true);
            return thread;
        };
    }

    private record RunKey(UUID taskId, int attempt) { }

    private final class Slot {
        private final AtomicBoolean released = new AtomicBoolean();
        void release() {
            if (released.compareAndSet(false, true)) {
                slots.release();
                metrics.executionStopped();
            }
        }
    }

    private final class RunHandle implements Runnable {
        private final TaskWorkItem work;
        private final Slot slot;
        private final AtomicBoolean leaseLost = new AtomicBoolean();
        private final AtomicBoolean ended = new AtomicBoolean();
        private final CompletableFuture<Void> finished = new CompletableFuture<>();
        private final RunKey key;
        private final Object startLock = new Object();
        private volatile ScheduledFuture<?> heartbeat;
        private boolean prevented;
        private Thread worker;

        private RunHandle(TaskWorkItem work, Slot slot) {
            this.work = work;
            this.slot = slot;
            this.key = new RunKey(work.id(), work.attempt());
        }

        private void renew() {
            if (ended.get() || leaseLost.get()) return;
            try {
                if (!tasks.renewLease(work)) loseLease();
            } catch (RuntimeException unavailable) {
                loseLease();
            }
        }

        private void loseLease() {
            if (leaseLost.compareAndSet(false, true)) interrupt();
        }

        private void interrupt() {
            synchronized (startLock) {
                if (worker != null) worker.interrupt();
            }
        }

        private void rejectBeforeStart() {
            synchronized (startLock) {
                if (worker != null || prevented) return;
                prevented = true;
            }
            stopHeartbeat();
            running.remove(key, this);
            completeRejected(this);
            ended.set(true);
            slot.release();
        }

        private void abandonBeforeStart() {
            synchronized (startLock) {
                if (worker != null || ended.get()) return;
                prevented = true;
            }
            abandon();
        }

        private void abandon() {
            if (!ended.compareAndSet(false, true)) return;
            stopHeartbeat();
            running.remove(key, this);
            slot.release();
            finished.completeExceptionally(new IllegalStateException("Task worker stopped before execution."));
        }

        @Override
        public void run() {
            boolean abortBeforeStart;
            synchronized (startLock) {
                if (prevented) return;
                abortBeforeStart = forcedShutdown.get();
                if (abortBeforeStart) prevented = true;
                else worker = Thread.currentThread();
            }
            if (abortBeforeStart) {
                abandon();
                return;
            }
            RuntimeException persistFailure = null;
            try {
                if (leaseLost.get()) {
                    persistFailure = new IllegalStateException("Task lease was lost before execution started.");
                    return;
                }
                TaskRunner.RunOutcome outcome;
                try {
                    outcome = runner.run(work);
                } catch (RuntimeException failure) {
                    outcome = TaskRunner.RunOutcome.failed("RUNTIME_FAILURE", "任务运行失败。");
                }
                if (!leaseLost.get() && !forcedShutdown.get()) {
                    try {
                        tasks.complete(work, outcome);
                    } catch (RuntimeException failure) {
                        persistFailure = failure;
                    }
                } else {
                    // 宽限期后即使 Runtime 忽略中断，也留给原租约恢复，不能迟交成功。
                    persistFailure = new IllegalStateException("Task worker stopped before completion.");
                }
            } finally {
                ended.set(true);
                stopHeartbeat();
                running.remove(key, this);
                slot.release();
                if (persistFailure == null) finished.complete(null);
                else finished.completeExceptionally(persistFailure);
            }
        }

        private void stopHeartbeat() {
            var scheduled = heartbeat;
            if (scheduled != null) scheduled.cancel(false);
        }
    }
}
