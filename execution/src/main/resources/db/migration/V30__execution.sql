create schema if not exists execution;

create table execution.execution (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    actor_id uuid not null,
    task_id uuid not null,
    attempt integer not null,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    tool_name varchar(120) not null,
    tool_version varchar(40) not null,
    arguments_json jsonb not null,
    request_hash varchar(64) not null,
    idempotency_key varchar(200) not null,
    status varchar(30) not null check (status in ('RECEIVED','VALIDATING','READY','EXECUTING','VERIFYING','SUCCEEDED','DENIED','FAILED','UNKNOWN','CANCELLED')),
    policy_version varchar(80),
    result_json jsonb,
    error_code varchar(80),
    error_detail varchar(1000),
    created_at timestamptz not null,
    started_at timestamptz,
    ended_at timestamptz,
    unique (tenant_id, workspace_id, actor_id, task_id, attempt, idempotency_key)
);
-- 本文件负责 EAF 的 V30__execution.sql 相关定义。
