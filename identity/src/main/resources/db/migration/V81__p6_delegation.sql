-- 委托由 Identity 持有；动作/客户范围不可变，撤销只改变状态与时间戳。
create table identity.delegation (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    delegate_id uuid not null,
    audience varchar(80) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'REVOKED')),
    created_at timestamptz not null,
    expires_at timestamptz not null,
    revoked_at timestamptz,
    scope_hash varchar(64) not null check (length(scope_hash) = 64),
    check (owner_id <> delegate_id),
    check ((status = 'ACTIVE' and revoked_at is null) or (status = 'REVOKED' and revoked_at is not null))
);

create index delegation_active_lookup on identity.delegation(tenant_id, delegate_id, workspace_id, audience, expires_at)
    where status = 'ACTIVE';

create table identity.delegation_action (
    delegation_id uuid not null references identity.delegation(id),
    action varchar(80) not null,
    primary key (delegation_id, action)
);

create table identity.delegation_customer (
    delegation_id uuid not null references identity.delegation(id),
    customer_id varchar(160) not null,
    primary key (delegation_id, customer_id)
);
