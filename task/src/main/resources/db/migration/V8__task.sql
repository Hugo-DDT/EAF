create schema if not exists task;

create table task.task (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    actor_id uuid not null,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    prompt_id uuid not null,
    prompt_version varchar(40) not null,
    model_profile_id uuid not null,
    input_text text not null,
    business_entity_type varchar(80),
    business_entity_id varchar(160),
    idempotency_key varchar(200) not null,
    request_hash varchar(64) not null,
    trace_id varchar(80) not null,
    status varchar(30) not null check (status in ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED')),
    attempt integer not null check (attempt > 0),
    row_version bigint not null,
    result_json jsonb,
    error_code varchar(80),
    error_detail varchar(1000),
    source varchar(30) not null check (source in ('USER', 'EVALUATION')),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    active_deadline_at timestamptz not null,
    deadline_at timestamptz not null,
    unique (tenant_id, workspace_id, actor_id, idempotency_key)
);

create table task.task_attempt (
    task_id uuid not null,
    attempt integer not null,
    status varchar(30) not null,
    started_at timestamptz,
    ended_at timestamptz,
    primary key (task_id, attempt)
);
-- 本文件负责 EAF 的 V8__task.sql 相关定义。
