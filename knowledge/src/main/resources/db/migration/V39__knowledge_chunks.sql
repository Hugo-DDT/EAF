alter table knowledge.document_version
    add constraint document_version_scope_version_unique unique (tenant_id, workspace_id, document_id, asset_version);

create table knowledge.chunk (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    document_id uuid not null,
    asset_version integer not null check (asset_version > 0),
    source_ref varchar(500) not null,
    chunking_version varchar(40) not null,
    chunk_order integer not null check (chunk_order > 0),
    start_offset integer not null check (start_offset >= 0),
    end_offset integer not null check (end_offset > start_offset),
    offset_unit varchar(30) not null check (offset_unit = 'UNICODE_CODE_POINT'),
    content text not null check (btrim(content) <> ''),
    content_hash char(64) not null,
    created_at timestamptz not null,
    unique (tenant_id, workspace_id, document_id, asset_version, chunking_version, chunk_order),
    unique (tenant_id, workspace_id, document_id, asset_version, chunking_version, start_offset, end_offset),
    foreign key (tenant_id, workspace_id, document_id, asset_version)
        references knowledge.document_version(tenant_id, workspace_id, document_id, asset_version)
);

-- Chunk 是文档版本的可重建派生物；偏移按 Unicode code point 记录，避免切断 emoji 等代理项。
