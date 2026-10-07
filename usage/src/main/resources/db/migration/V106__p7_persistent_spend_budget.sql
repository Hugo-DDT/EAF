-- 金额范围和调用预留归 Usage 所有；模型请求前先占额，结果不明时不得释放。
create table usage.spend_scope (
    tenant_id uuid not null,
    workspace_id uuid not null,
    scope_type varchar(30) not null check (scope_type in ('TASK', 'WORKFLOW', 'EVALUATION', 'JOB')),
    scope_id uuid not null,
    currency varchar(3) not null check (currency ~ '^[A-Z]{3}$'),
    limit_amount numeric(18, 8) not null check (limit_amount > 0),
    spent_amount numeric(18, 8) not null default 0 check (spent_amount >= 0),
    reserved_amount numeric(18, 8) not null default 0 check (reserved_amount >= 0),
    status varchar(20) not null default 'ACTIVE' check (status in ('ACTIVE', 'STOPPED')),
    stop_reason varchar(60),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, workspace_id, scope_type, scope_id),
    check (spent_amount + reserved_amount <= limit_amount or status = 'STOPPED'),
    check ((status = 'ACTIVE' and stop_reason is null) or (status = 'STOPPED' and stop_reason is not null))
);

create table usage.spend_reservation (
    call_key varchar(240) primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    scope_type varchar(30) not null,
    scope_id uuid not null,
    provider varchar(100) not null,
    model varchar(150) not null,
    call_type varchar(30) not null check (call_type in ('CHAT', 'EMBEDDING_QUERY', 'EMBEDDING_INDEX', 'EMBEDDING_BATCH', 'PEER')),
    max_tokens bigint not null check (max_tokens >= 0),
    price_version varchar(80) not null,
    price_source varchar(120) not null,
    price_source_version varchar(120) not null,
    price_effective_at timestamptz not null,
    billing_unit varchar(20) not null check (billing_unit in ('TOKEN_MILLION', 'REQUEST')),
    input_price_per_million numeric(18, 8),
    output_price_per_million numeric(18, 8),
    request_price numeric(18, 8),
    currency varchar(3) not null check (currency ~ '^[A-Z]{3}$'),
    reserved_amount numeric(18, 8) not null check (reserved_amount >= 0),
    settled_amount numeric(18, 8),
    billed_amount numeric(18, 8),
    state varchar(20) not null check (state in ('RESERVED', 'SETTLED', 'RELEASED')),
    created_at timestamptz not null default now(),
    settled_at timestamptz,
    foreign key (tenant_id, workspace_id, scope_type, scope_id)
        references usage.spend_scope(tenant_id, workspace_id, scope_type, scope_id),
    check (settled_amount is null or settled_amount >= 0),
    check (billed_amount is null or billed_amount >= 0),
    check ((state = 'RESERVED' and settled_amount is null and settled_at is null)
        or (state = 'SETTLED' and settled_amount is not null and settled_at is not null)
        or (state = 'RELEASED' and settled_amount is null and settled_at is not null)),
    check ((billing_unit = 'TOKEN_MILLION' and input_price_per_million is not null
            and output_price_per_million is not null and request_price is null)
        or (billing_unit = 'REQUEST' and input_price_per_million is null
            and output_price_per_million is null and request_price is not null))
);

create index spend_reservation_scope_idx on usage.spend_reservation(tenant_id, workspace_id, scope_type, scope_id, state);
