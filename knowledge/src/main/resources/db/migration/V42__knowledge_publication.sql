create table knowledge.document_publication (
    tenant_id uuid not null,
    workspace_id uuid not null,
    document_id uuid not null,
    asset_version integer not null check (asset_version > 0),
    build_id uuid not null references knowledge.index_build(id),
    status varchar(20) not null check (status in ('ACTIVE', 'REVOKED')),
    row_version bigint not null default 1 check (row_version > 0),
    updated_at timestamptz not null,
    primary key (tenant_id, workspace_id, document_id),
    foreign key (tenant_id, workspace_id, document_id)
        references knowledge.document(tenant_id, workspace_id, id)
);

create table knowledge.publication_event (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    document_id uuid not null,
    asset_version integer not null check (asset_version > 0),
    build_id uuid not null references knowledge.index_build(id),
    action varchar(20) not null check (action in ('PUBLISHED', 'REVOKED')),
    document_status varchar(20) not null check (document_status in ('PUBLISHED', 'REVOKED')),
    document_row_version bigint not null check (document_row_version > 0),
    idempotency_key varchar(200) not null,
    request_hash char(64) not null,
    occurred_at timestamptz not null,
    unique (tenant_id, workspace_id, document_id, idempotency_key),
    foreign key (tenant_id, workspace_id, document_id)
        references knowledge.document(tenant_id, workspace_id, id)
);

create index knowledge_publication_event_scope_idx
    on knowledge.publication_event(tenant_id, workspace_id, document_id, occurred_at);

-- 当前指针服务新检索，追加事件保留发布/撤回事实；两者都受同一文档行锁保护。
