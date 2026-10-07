alter table knowledge.chunk
    add column heading_path jsonb not null default '[]'::jsonb;

alter table knowledge.index_build
    add column lexical_index_version varchar(40) not null default 'none';

create table knowledge.lexical_entry (
    tenant_id uuid not null,
    workspace_id uuid not null,
    document_id uuid not null,
    asset_version integer not null,
    build_id uuid not null references knowledge.index_build(id),
    chunk_id uuid not null references knowledge.chunk(id),
    normalization_version varchar(40) not null,
    search_vector tsvector not null,
    created_at timestamptz not null,
    primary key (tenant_id, workspace_id, document_id, asset_version, build_id, chunk_id)
);

create index knowledge_lexical_entry_search_idx on knowledge.lexical_entry using gin(search_vector);
create index knowledge_lexical_entry_build_idx on knowledge.lexical_entry(tenant_id, workspace_id, build_id);

-- 已发布旧构建保持可检索；结构化切块与词法索引只在显式新建的构建中生成。
