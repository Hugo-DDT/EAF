-- P4 的等待状态不是失败：审批期间暂停业务执行，resume 后继续同一个 attempt。
alter table task.task drop constraint if exists task_task_status_check;
alter table task.task add constraint task_task_status_check check (status in ('QUEUED', 'RUNNING', 'WAITING_APPROVAL', 'WAITING_VERIFICATION', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED'));
create table task.resume_request (
    task_id uuid not null,
    idempotency_key varchar(200) not null,
    created_at timestamptz not null default now(),
    primary key (task_id, idempotency_key)
);
