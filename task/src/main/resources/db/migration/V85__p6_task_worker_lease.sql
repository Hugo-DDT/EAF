-- Task 域以递增 fence 和过期租约隔离旧 Worker 的晚到结果。
alter table task.task
    add column lease_owner_id uuid,
    add column lease_fence bigint not null default 0,
    add column lease_until timestamptz;

create index task_running_lease on task.task(lease_until, root_task_id, id)
    where status = 'RUNNING';
