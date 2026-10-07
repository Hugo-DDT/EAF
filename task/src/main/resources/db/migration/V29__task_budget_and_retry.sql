alter table task.task add column max_steps integer not null default 24;
alter table task.task add column steps_used integer not null default 0;
alter table task.task add column model_calls integer not null default 0;
alter table task.task add column tool_calls integer not null default 0;
alter table task.task add column tool_executions integer not null default 0;
alter table task.task add column token_used integer not null default 0;
alter table task.task add column token_reserved integer not null default 0;
alter table task.task add constraint task_budget_check check (max_steps > 0 and steps_used >= 0 and model_calls >= 0 and tool_calls >= 0 and tool_executions >= 0 and token_used >= 0 and token_reserved >= 0);

create table task.retry_request (
    task_id uuid not null,
    idempotency_key varchar(200) not null,
    created_at timestamptz not null default now(),
    primary key (task_id, idempotency_key)
);
-- 本文件负责 EAF 的 V29__task_budget_and_retry.sql 相关定义。
