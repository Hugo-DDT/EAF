create unique index knowledge_index_build_share_binding_uq
    on knowledge.index_build(tenant_id, workspace_id, document_id, asset_version, id);

create table knowledge.context_share (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    document_id uuid not null,
    recipient_id uuid not null,
    document_version integer not null check (document_version > 0),
    build_id uuid not null,
    content_hash char(64) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'REVOKED')),
    row_version bigint not null check (row_version > 0),
    created_by uuid not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    unique (tenant_id, workspace_id, document_id, recipient_id),
    foreign key (tenant_id, workspace_id, document_id)
        references knowledge.document(tenant_id, workspace_id, id),
    foreign key (tenant_id, workspace_id, document_id, document_version, build_id)
        references knowledge.index_build(tenant_id, workspace_id, document_id, asset_version, id)
);

create index knowledge_context_share_recipient_idx
    on knowledge.context_share(tenant_id, recipient_id, workspace_id, document_id)
    where status = 'ACTIVE';
