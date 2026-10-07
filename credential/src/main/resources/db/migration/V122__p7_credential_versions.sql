create schema if not exists credential;

create table credential.binding (
    id uuid primary key,
    scope_type varchar(20) not null check (scope_type in ('WORKSPACE', 'SYSTEM')),
    tenant_id uuid,
    workspace_id uuid,
    owner_id uuid,
    credential_ref varchar(120) not null check (credential_ref ~ '^[a-z0-9][a-z0-9._-]{0,119}$'),
    audience varchar(160) not null check (audience ~ '^[a-z][a-z0-9._:-]{0,159}$'),
    allowed_uses text[] not null check (cardinality(allowed_uses) > 0),
    permissions text[] not null check (cardinality(permissions) > 0),
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    current_version bigint not null check (current_version > 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint credential_scope_shape check (
        (scope_type = 'WORKSPACE' and tenant_id is not null and workspace_id is not null)
        or (scope_type = 'SYSTEM' and tenant_id is null and workspace_id is null)
    ),
    constraint credential_binding_scope_key unique nulls not distinct (scope_type, tenant_id, workspace_id, credential_ref)
);

create table credential.secret_version (
    binding_id uuid not null references credential.binding(id),
    version bigint not null check (version > 0),
    secret_ref varchar(280) not null check (secret_ref ~ '^(env|vault|aws-sm|azure-kv)://[A-Za-z0-9._:/-]{1,240}$'),
    status varchar(20) not null check (status in ('ACTIVE', 'REVOKED')),
    valid_from timestamptz not null,
    expires_at timestamptz,
    created_at timestamptz not null default now(),
    primary key (binding_id, version),
    check (expires_at is null or expires_at > valid_from)
);

alter table credential.binding add constraint credential_current_version_fk
    foreign key (id, current_version) references credential.secret_version(binding_id, version)
    deferrable initially deferred;

comment on table credential.binding is 'Credential 所有的不可含秘密明文授权绑定；SYSTEM 限定于已登记的 Provider 用途。';
comment on table credential.secret_version is '追加保存的秘密后端引用版本；轮换后旧版本撤销且解析端不缓存。';
comment on column credential.secret_version.secret_ref is '受限 Secret 后端引用，不是秘密值。';

-- 本迁移只保存工作区边界和 Provider 用途元数据；秘密值由后端单独托管。
