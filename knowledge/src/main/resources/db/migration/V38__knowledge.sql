create schema if not exists knowledge;

create table knowledge.document (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    title varchar(200) not null,
    source_ref varchar(500) not null,
    metadata jsonb not null default '{}'::jsonb,
    status varchar(20) not null check (status in ('DRAFT', 'PUBLISHED', 'REVOKED')),
    idempotency_key varchar(200) not null,
    request_hash char(64) not null,
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    unique (tenant_id, id),
    unique (tenant_id, workspace_id, owner_id, idempotency_key),
    unique (tenant_id, workspace_id, id)
);

create table knowledge.document_version (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    document_id uuid not null,
    asset_version integer not null check (asset_version > 0),
    content text not null check (btrim(content) <> '' and octet_length(content) <= 102400),
    content_hash char(64) not null,
    status varchar(20) not null check (status in ('DRAFT', 'PUBLISHED', 'REVOKED')),
    created_at timestamptz not null,
    unique (tenant_id, document_id, asset_version),
    foreign key (tenant_id, workspace_id, document_id)
        references knowledge.document(tenant_id, workspace_id, id)
);

create table knowledge.document_permission (
    tenant_id uuid not null,
    workspace_id uuid not null,
    document_id uuid not null,
    actor_id uuid not null,
    action varchar(40) not null check (action in ('knowledge:read', 'knowledge:write', 'knowledge:publish')),
    status varchar(20) not null check (status in ('ACTIVE', 'REVOKED')),
    primary key (tenant_id, document_id, actor_id, action),
    foreign key (tenant_id, workspace_id, document_id)
        references knowledge.document(tenant_id, workspace_id, id)
);

-- 原文属于 knowledge；版本和文档授权同租户、同 Workspace，避免跨域或跨租户挂接。
