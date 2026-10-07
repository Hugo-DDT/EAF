-- Runtime 运行行快照其 Task Worker fence，旧 Worker 不能覆盖被回收的运行状态。
alter table agent_runtime.run
    add column task_lease_owner_id uuid,
    add column task_lease_fence bigint not null default 0;
