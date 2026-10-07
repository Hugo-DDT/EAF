create schema if not exists usage;

create table usage.model_usage (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    task_id uuid not null,
    run_id uuid not null,
    provider varchar(100),
    model varchar(150),
    input_tokens integer,
    output_tokens integer,
    reserved_tokens integer not null,
    status varchar(30) not null,
    error_code varchar(80),
    started_at timestamptz not null,
    ended_at timestamptz,
    unique (task_id, run_id)
);
-- 本文件负责 EAF 的 V11__usage.sql 相关定义。
