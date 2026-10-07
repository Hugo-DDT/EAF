create schema if not exists observability;
create table observability.trace_observation (
    id uuid primary key,
    trace_id varchar(80) not null,
    task_id uuid not null,
    run_id uuid not null,
    phase varchar(80) not null,
    elapsed_ms bigint not null,
    outcome varchar(40) not null,
    error_code varchar(80),
    occurred_at timestamptz not null,
    unique (trace_id, task_id, run_id, phase)
);
-- 本文件负责 EAF 的 V12__observability.sql 相关定义。
