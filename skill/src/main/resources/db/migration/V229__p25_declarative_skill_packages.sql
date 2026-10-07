create table skill.declarative_package (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    profile varchar(80) not null check (profile = 'EAF_DECLARATIVE_CAPABILITY_V1'),
    format_version varchar(20) not null check (format_version = '1'),
    template_version varchar(20) not null check (template_version = '1'),
    name varchar(120) not null,
    package_hash char(64) not null check (package_hash ~ '^[0-9a-f]{64}$'),
    source_workspace_id uuid not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    capability_hash char(64) not null check (capability_hash ~ '^[0-9a-f]{64}$'),
    skill_id uuid not null,
    skill_version varchar(40) not null,
    skill_hash char(64) not null check (skill_hash ~ '^[0-9a-f]{64}$'),
    manifest_json jsonb not null,
    files_json jsonb not null,
    idempotency_key_hash char(64) not null check (idempotency_key_hash ~ '^[0-9a-f]{64}$'),
    request_hash char(64) not null check (request_hash ~ '^[0-9a-f]{64}$'),
    status varchar(20) not null default 'STORED' check (status in ('STORED', 'ARCHIVED')),
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null default now(),
    archived_at timestamptz,
    unique (tenant_id, workspace_id, owner_id, idempotency_key_hash),
    check ((status = 'STORED' and archived_at is null) or (status = 'ARCHIVED' and archived_at is not null))
);

create index declarative_package_owner_page_idx
    on skill.declarative_package (tenant_id, workspace_id, owner_id, status, created_at desc, id);
