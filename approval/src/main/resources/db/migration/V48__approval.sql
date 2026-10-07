create schema if not exists approval;

create table approval.request (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    task_id uuid not null,
    execution_id uuid not null unique,
    idempotency_key varchar(200),
    binding_hash varchar(64) not null,
    binding_json jsonb not null,
    state varchar(20) not null check (state in ('PENDING','APPROVED','REJECTED','EXPIRED','CANCELLED')),
    expires_at timestamptz not null,
    decided_by uuid,
    decided_at timestamptz,
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null
);

create unique index approval_request_idempotency on approval.request(tenant_id, workspace_id, idempotency_key)
where idempotency_key is not null;

create table approval.outbox (
    event_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    approval_id uuid not null,
    event_type varchar(120) not null,
    payload_json jsonb not null,
    attempts integer not null default 0,
    status varchar(20) not null default 'PENDING' check (status in ('PENDING','SENT','FAILED')),
    next_attempt_at timestamptz not null default now(),
    created_at timestamptz not null default now()
);

-- P4 的最小审批角色只扩大测试数据范围；生产身份和角色目录仍由部署配置提供。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status) values
 ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000002', 'approval:read', 'ACTIVE'),
 ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000002', 'approval:decide', 'ACTIVE')
on conflict (workspace_id, actor_id, action) do nothing;
