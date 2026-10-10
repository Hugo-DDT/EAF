-- 仅由来源名单创建的 READ 才能随来源撤回，避免误改 Owner 或人工授权。
alter table knowledge.document_permission
    add column grant_origin varchar(32) not null default 'MANUAL'
        check (grant_origin in ('MANUAL', 'DOCUMENT_OWNER', 'MANAGED_SOURCE'));

create table knowledge.managed_source (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    name varchar(160) not null,
    source_type varchar(32) not null check (source_type = 'MANAGED_TEXT_V1'),
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    source_revision bigint not null check (source_revision > 0),
    create_key_hash char(64) not null,
    create_request_hash char(64) not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    unique (tenant_id, workspace_id, owner_id, create_key_hash),
    unique (tenant_id, workspace_id, id)
);

create table knowledge.managed_source_operation (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    source_id uuid not null,
    actor_id uuid not null,
    operation_type varchar(20) not null check (operation_type in ('SYNC', 'STATE')),
    request_key_hash char(64) not null,
    request_hash char(64) not null,
    previous_source_revision bigint not null check (previous_source_revision > 0),
    source_revision bigint not null check (source_revision >= previous_source_revision),
    result_status varchar(20) not null check (result_status = 'APPLIED'),
    result_source_status varchar(20) check (result_source_status in ('ACTIVE', 'DISABLED')),
    observed_at timestamptz not null,
    unique (tenant_id, workspace_id, source_id, request_key_hash),
    foreign key (tenant_id, workspace_id, source_id)
        references knowledge.managed_source(tenant_id, workspace_id, id)
);

create table knowledge.managed_source_item (
    tenant_id uuid not null,
    workspace_id uuid not null,
    source_id uuid not null,
    item_id varchar(120) not null,
    document_id uuid not null,
    display_title varchar(200) not null,
    source_version varchar(160) not null,
    content_hash char(64) not null,
    acl_version varchar(160) not null,
    acl_hash char(64) not null,
    availability varchar(20) not null check (availability in ('AVAILABLE', 'DELETED', 'UNAVAILABLE')),
    reason_code varchar(80),
    observed_at timestamptz not null,
    primary key (tenant_id, workspace_id, source_id, item_id),
    unique (tenant_id, workspace_id, document_id),
    foreign key (tenant_id, workspace_id, source_id)
        references knowledge.managed_source(tenant_id, workspace_id, id),
    foreign key (tenant_id, workspace_id, document_id)
        references knowledge.document(tenant_id, workspace_id, id)
);

create table knowledge.managed_source_reader (
    tenant_id uuid not null,
    workspace_id uuid not null,
    source_id uuid not null,
    item_id varchar(120) not null,
    actor_id uuid not null,
    primary key (tenant_id, workspace_id, source_id, item_id, actor_id),
    foreign key (tenant_id, workspace_id, source_id, item_id)
        references knowledge.managed_source_item(tenant_id, workspace_id, source_id, item_id)
);

create table knowledge.managed_source_sync_item (
    sync_id uuid not null references knowledge.managed_source_operation(id),
    position integer not null check (position >= 0),
    item_id varchar(120) not null,
    change_type varchar(20) not null check (change_type in ('UPSERT', 'DELETE', 'ACCESS', 'UNAVAILABLE')),
    result varchar(32) not null,
    document_id uuid,
    document_version integer,
    source_version varchar(160),
    content_hash char(64),
    acl_version varchar(160),
    acl_hash char(64),
    availability varchar(20) check (availability in ('AVAILABLE', 'DELETED', 'UNAVAILABLE')),
    reason_code varchar(80),
    primary key (sync_id, item_id),
    unique (sync_id, position)
);

create table knowledge.managed_source_version (
    tenant_id uuid not null,
    workspace_id uuid not null,
    source_id uuid not null,
    item_id varchar(120) not null,
    document_id uuid not null,
    document_version integer not null check (document_version > 0),
    source_version varchar(160) not null,
    content_hash char(64) not null,
    acl_version varchar(160) not null,
    acl_hash char(64) not null,
    sync_id uuid not null references knowledge.managed_source_operation(id),
    observed_at timestamptz not null,
    primary key (tenant_id, workspace_id, document_id, document_version),
    foreign key (tenant_id, workspace_id, source_id, item_id)
        references knowledge.managed_source_item(tenant_id, workspace_id, source_id, item_id),
    foreign key (tenant_id, workspace_id, document_id)
        references knowledge.document(tenant_id, workspace_id, id)
);

create index managed_source_item_document_idx
    on knowledge.managed_source_item(tenant_id, workspace_id, document_id);
create index managed_source_version_source_idx
    on knowledge.managed_source_version(tenant_id, workspace_id, source_id, item_id, document_version);
