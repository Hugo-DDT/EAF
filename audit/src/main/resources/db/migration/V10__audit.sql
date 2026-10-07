create extension if not exists pgcrypto;
create schema if not exists audit;

create table audit.audit_event (
    id uuid primary key,
    fact_key varchar(200) not null unique,
    tenant_id uuid not null,
    workspace_id uuid,
    actor_id uuid,
    task_id uuid,
    action varchar(100) not null,
    result varchar(40) not null,
    payload_json jsonb not null default '{}'::jsonb,
    trace_id varchar(80),
    occurred_at timestamptz not null default now()
);

create or replace function audit.reject_mutation() returns trigger language plpgsql as $$
begin
    raise exception 'audit events are append-only';
end $$;
create trigger audit_event_append_only before update or delete on audit.audit_event
    for each row execute function audit.reject_mutation();
-- 本文件负责 EAF 的 V10__audit.sql 相关定义。
