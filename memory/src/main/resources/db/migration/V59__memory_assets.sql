create schema if not exists memory;

-- Memory 版本只引用本域定义；scope 限于当前 Workspace 与 Owner，避免隐式跨域共享。
create table memory.definition (
    id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    logical_key varchar(120) not null,
    created_at timestamptz not null default now(),
    primary key (id, workspace_id),
    unique (id, tenant_id, workspace_id),
    unique (tenant_id, workspace_id, logical_key),
    check (length(trim(logical_key)) > 0)
);

create table memory.version (
    memory_id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    asset_version varchar(40) not null,
    memory_type varchar(20) not null check (memory_type in ('EPISODIC', 'SEMANTIC', 'PROCEDURAL', 'PREFERENCE')),
    scope varchar(20) not null check (scope in ('PERSONAL', 'TEAM')),
    content varchar(8000) not null check (length(trim(content)) > 0),
    confidence double precision not null check (confidence >= 0 and confidence <= 1),
    expires_at timestamptz not null,
    source_type varchar(32) not null check (source_type in ('OWNER_ATTESTATION', 'CONTROLLED_SEED', 'LEARNING_RELEASE')),
    source_ref varchar(500) not null check (length(trim(source_ref)) > 0),
    evidence_refs jsonb not null default '[]'::jsonb check (jsonb_typeof(evidence_refs) = 'array'),
    status varchar(20) not null check (status in ('DRAFT', 'PUBLISHED', 'REVOKED')),
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null default now(),
    primary key (memory_id, workspace_id, asset_version),
    unique (memory_id, tenant_id, workspace_id, asset_version),
    foreign key (memory_id, tenant_id, workspace_id) references memory.definition(id, tenant_id, workspace_id)
);

create table memory.release (
    release_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    memory_id uuid not null,
    memory_version varchar(40) not null,
    action varchar(10) not null check (action in ('PUBLISHED', 'REVOKED')),
    actor_id uuid not null,
    occurred_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, memory_id, memory_version, action),
    foreign key (memory_id, tenant_id, workspace_id, memory_version)
        references memory.version(memory_id, tenant_id, workspace_id, asset_version)
);

-- 发布只改变状态；已发布内容及来源不可变，续期和纠正必须创建新版本。
create or replace function memory.reject_published_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.status in ('PUBLISHED', 'REVOKED') and (
        new.memory_id is distinct from old.memory_id
        or new.tenant_id is distinct from old.tenant_id
        or new.workspace_id is distinct from old.workspace_id
        or new.asset_version is distinct from old.asset_version
        or new.memory_type is distinct from old.memory_type
        or new.scope is distinct from old.scope
        or new.content is distinct from old.content
        or new.confidence is distinct from old.confidence
        or new.expires_at is distinct from old.expires_at
        or new.source_type is distinct from old.source_type
        or new.source_ref is distinct from old.source_ref
        or new.evidence_refs is distinct from old.evidence_refs
        or new.created_at is distinct from old.created_at
    ) then
        raise exception 'published memory content is immutable';
    end if;
    return new;
end;
$$;

create trigger memory_published_immutable
before update on memory.version
for each row execute function memory.reject_published_mutation();

create or replace function memory.reject_release_mutation()
returns trigger
language plpgsql
as $$
begin
    raise exception 'memory release history is append-only';
end;
$$;

create trigger memory_release_append_only
before update or delete on memory.release
for each row execute function memory.reject_release_mutation();

-- 合成受控样本先以草稿存在，须由 Owner 显式发布，发布路径会追加 Memory 与通用审计事实。
insert into memory.definition(id, tenant_id, workspace_id, owner_id, logical_key)
values ('56000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', 'customer-a-renewal');

insert into memory.version(memory_id, tenant_id, workspace_id, asset_version, memory_type, scope,
                           content, confidence, expires_at, source_type, source_ref, evidence_refs, status)
values ('56000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0', 'SEMANTIC', 'TEAM',
        '合成演示记录：客户 A 的续约时间需在 CRM 查询中再次确认。', 0.8,
        '2026-12-31T23:59:59Z', 'CONTROLLED_SEED', 'test:synthetic-customer-a',
        '["test:synthetic-customer-a"]', 'DRAFT');
