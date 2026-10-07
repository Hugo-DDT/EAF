-- 扩展固定安装到 public，避免应用连接的 search_path 改变后找不到类型。
create extension if not exists vector with schema public;

create table knowledge.embedding (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    document_id uuid not null,
    asset_version integer not null check (asset_version > 0),
    chunk_id uuid not null references knowledge.chunk(id),
    provider varchar(80) not null,
    model varchar(150) not null,
    model_revision varchar(120) not null,
    dimension integer not null check (dimension = 8),
    chunking_version varchar(40) not null,
    distance_metric varchar(20) not null check (distance_metric = 'COSINE'),
    configuration_signature char(64) not null,
    embedding public.vector(8) not null,
    created_at timestamptz not null,
    unique (tenant_id, workspace_id, chunk_id, configuration_signature)
);

create index knowledge_embedding_scope_config_idx
    on knowledge.embedding(tenant_id, workspace_id, configuration_signature);

-- 向量是 knowledge Chunk 的派生存储；固定 8 维基线并保存完整配置签名，避免同维度模型混用。
