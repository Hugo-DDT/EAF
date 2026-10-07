-- 仅 P21 固定知识/经验分支可写入份额；旧 Task 的 NULL 保留原预算语义。
alter table task.task add column max_active_slice_ms bigint;
alter table task.task add constraint task_active_slice_check
    check (max_active_slice_ms is null or max_active_slice_ms > 0);
