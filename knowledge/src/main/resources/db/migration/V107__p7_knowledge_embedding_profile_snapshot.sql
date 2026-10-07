-- 向量列改为可变维度；每行仍通过数据库约束绑定到构建快照的维度。
alter table knowledge.embedding
    alter column embedding type public.vector
    using embedding::public.vector;

alter table knowledge.embedding
    drop constraint embedding_dimension_check;

alter table knowledge.embedding
    add constraint knowledge_embedding_dimension_range
        check (dimension between 1 and 16000),
    add constraint knowledge_embedding_vector_dimension
        check (public.vector_dims(embedding) = dimension);

alter table knowledge.index_build
    drop constraint index_build_dimension_check;

alter table knowledge.index_build
    add constraint knowledge_index_build_dimension_range
        check (dimension between 1 and 16000),
    add column max_batch_size integer not null default 32
        check (max_batch_size between 1 and 32),
    add column max_input_tokens integer not null default 8192
        check (max_input_tokens between 1 and 8192),
    add column max_text_code_points integer not null default 8192
        check (max_text_code_points between 1 and 8192),
    add column retry_generation integer not null default 0
        check (retry_generation >= 0);

-- 精确检索仍按租户、Workspace、维度和完整配置签名筛选；ANN 索引等规模测量后再引入。
drop index knowledge.knowledge_embedding_scope_config_idx;
create index knowledge_embedding_scope_profile_idx
    on knowledge.embedding(tenant_id, workspace_id, dimension, configuration_signature);
