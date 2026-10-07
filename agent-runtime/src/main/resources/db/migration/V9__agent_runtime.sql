create schema if not exists agent_runtime;

create table agent_runtime.run (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    task_id uuid not null,
    attempt integer not null,
    status varchar(30) not null,
    started_at timestamptz not null,
    ended_at timestamptz,
    error_code varchar(80),
    unique (task_id, attempt)
);

create table agent_runtime.step (
    run_id uuid not null,
    task_id uuid not null,
    attempt integer not null,
    step_no integer not null,
    type varchar(40) not null,
    role varchar(30),
    content text,
    validation varchar(80),
    occurred_at timestamptz not null,
    primary key (run_id, step_no)
);
-- 本文件负责 EAF 的 V9__agent_runtime.sql 相关定义。
