create table knowledge.index_build (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    document_id uuid not null,
    asset_version integer not null check (asset_version > 0),
    chunking_version varchar(40) not null,
    provider varchar(80) not null,
    model varchar(150) not null,
    model_revision varchar(120) not null,
    dimension integer not null check (dimension = 8),
    distance_metric varchar(20) not null check (distance_metric = 'COSINE'),
    configuration_signature char(64) not null,
    idempotency_key varchar(200) not null,
    status varchar(20) not null check (status in ('PENDING', 'INDEXING', 'READY', 'FAILED')),
    total_chunks integer not null check (total_chunks >= 0),
    completed_chunks integer not null default 0 check (completed_chunks >= 0),
    next_chunk_order integer not null default 1 check (next_chunk_order > 0),
    input_tokens integer not null default 0 check (input_tokens >= 0),
    embedding_calls integer not null default 0 check (embedding_calls >= 0),
    failure_code varchar(80),
    created_at timestamptz not null,
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null,
    unique (tenant_id, workspace_id, document_id, asset_version, chunking_version, configuration_signature),
    unique (tenant_id, workspace_id, document_id, asset_version, idempotency_key)
);

alter table knowledge.embedding
    add column build_id uuid references knowledge.index_build(id);

create index knowledge_index_build_scope_status_idx
    on knowledge.index_build(tenant_id, workspace_id, status, updated_at);

create index knowledge_embedding_build_idx
    on knowledge.embedding(build_id);

-- Index Build 拥有派生向量的构建进度；失败时保留已写批次，但 READY 只由完整校验推进。
