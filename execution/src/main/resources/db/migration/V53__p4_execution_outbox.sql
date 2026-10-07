create table execution.outbox (
    event_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    execution_id uuid not null,
    event_type varchar(120) not null,
    aggregate_version bigint not null,
    payload_json jsonb not null,
    attempts integer not null default 0,
    status varchar(20) not null default 'PENDING' check (status in ('PENDING','SENT','FAILED')),
    lease_until timestamptz,
    next_attempt_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    unique (execution_id, event_type)
);
create index execution_outbox_pending on execution.outbox(status, next_attempt_at);
-- outbox 与 Execution 在同一事务内写入；payload 只保留状态引用，不携带凭证或完整上下文。
