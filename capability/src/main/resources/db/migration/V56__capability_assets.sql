create schema if not exists capability;

-- Capability 只保存自有 Manifest 与精确引用，不跨域 JOIN 或建立外域外键。
create table capability.definition (
    id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    name varchar(120) not null,
    description varchar(500) not null,
    created_at timestamptz not null default now(),
    primary key (id, workspace_id),
    unique (id, tenant_id, workspace_id),
    unique (tenant_id, workspace_id, name)
);

create table capability.version (
    capability_id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    asset_version varchar(40) not null,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    skill_id uuid not null,
    skill_version varchar(40) not null,
    prompt_id uuid not null,
    prompt_version varchar(40) not null,
    evaluation_ref varchar(120) not null,
    status varchar(20) not null check (status in ('DRAFT', 'IN_REVIEW', 'PUBLISHED', 'DEPRECATED', 'REVOKED')),
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null default now(),
    primary key (capability_id, workspace_id, asset_version),
    unique (capability_id, tenant_id, workspace_id, asset_version),
    foreign key (capability_id, tenant_id, workspace_id) references capability.definition(id, tenant_id, workspace_id)
);

create table capability.tool_dependency (
    tenant_id uuid not null,
    workspace_id uuid not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    tool_name varchar(120) not null,
    tool_version varchar(40) not null,
    input_schema jsonb not null,
    output_schema jsonb not null,
    primary key (tenant_id, workspace_id, capability_id, capability_version, tool_name),
    foreign key (capability_id, tenant_id, workspace_id, capability_version)
        references capability.version(capability_id, tenant_id, workspace_id, asset_version)
);

create table capability.release (
    release_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    action varchar(10) not null check (action in ('PUBLISHED', 'REVOKED')),
    actor_id uuid not null,
    occurred_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, capability_id, capability_version, action),
    foreign key (capability_id, tenant_id, workspace_id, capability_version)
        references capability.version(capability_id, tenant_id, workspace_id, asset_version)
);

create or replace function capability.reject_published_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.status in ('PUBLISHED', 'DEPRECATED', 'REVOKED') and (
        new.capability_id is distinct from old.capability_id
        or new.tenant_id is distinct from old.tenant_id
        or new.workspace_id is distinct from old.workspace_id
        or new.asset_version is distinct from old.asset_version
        or new.agent_id is distinct from old.agent_id
        or new.agent_version is distinct from old.agent_version
        or new.skill_id is distinct from old.skill_id
        or new.skill_version is distinct from old.skill_version
        or new.prompt_id is distinct from old.prompt_id
        or new.prompt_version is distinct from old.prompt_version
        or new.evaluation_ref is distinct from old.evaluation_ref
        or new.created_at is distinct from old.created_at
    ) then
        raise exception 'published capability content is immutable';
    end if;
    return new;
end;
$$;

create trigger capability_published_immutable
before update on capability.version
for each row execute function capability.reject_published_mutation();

create or replace function capability.reject_published_dependency_mutation()
returns trigger
language plpgsql
as $$
declare
    current_status varchar(20);
begin
    select status into current_status from capability.version
    where tenant_id = coalesce(old.tenant_id, new.tenant_id)
      and workspace_id = coalesce(old.workspace_id, new.workspace_id)
      and capability_id = coalesce(old.capability_id, new.capability_id)
      and asset_version = coalesce(old.capability_version, new.capability_version);
    if current_status <> 'DRAFT' then
        raise exception 'published capability dependencies are immutable';
    end if;
    if tg_op = 'DELETE' then return old; end if;
    return new;
end;
$$;

create trigger capability_dependency_immutable
before update or delete on capability.tool_dependency
for each row execute function capability.reject_published_dependency_mutation();

create or replace function capability.reject_release_mutation()
returns trigger
language plpgsql
as $$
begin
    raise exception 'capability release history is append-only';
end;
$$;

create trigger capability_release_append_only
before update or delete on capability.release
for each row execute function capability.reject_release_mutation();

-- Capability 管理权限沿用 Skill 权限，发现权限与 Agent 发现保持一致。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'capability:read', 'ACTIVE'
from workspace."grant" where action = 'agent:read'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'capability:write', 'ACTIVE'
from workspace."grant" where action = 'skill:write'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'capability:publish', 'ACTIVE'
from workspace."grant" where action = 'skill:publish'
on conflict (workspace_id, actor_id, action) do nothing;

insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('54000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'customer-risk-followup', '分析客户风险，并在获准时申请创建 CRM 跟进。');

insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version,
                               evaluation_ref, status)
values ('54000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0', '20000000-0000-4000-8000-000000000001', '2.0.0',
        '53000000-0000-4000-8000-000000000001', '1.0.0',
        '21000000-0000-4000-8000-000000000001', '2.0.0', 'p2-v1', 'PUBLISHED');

insert into capability.tool_dependency(tenant_id, workspace_id, capability_id, capability_version,
                                       tool_name, tool_version, input_schema, output_schema)
values
    ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
     '54000000-0000-4000-8000-000000000001', '1.0.0', 'crm.customer.query', '1.0.0',
     '{"type":"object","required":["customerId"],"additionalProperties":false,"properties":{"customerId":{"type":"string","maxLength":160,"minLength":1}}}',
     '{"type":"object","required":["customerId","renewalStatus","lastContactDate","complaintSummary"],"additionalProperties":false}'),
    ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
     '54000000-0000-4000-8000-000000000001', '1.0.0', 'crm.followup.create', '1.0.0',
     '{"type":"object","required":["customerId","summary"],"additionalProperties":false,"properties":{"customerId":{"type":"string","maxLength":160,"minLength":1},"summary":{"type":"string","maxLength":2000,"minLength":1}}}',
     '{"type":"object","required":["operationId","externalId","status"],"additionalProperties":false}');

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
values ('54000000-0000-4000-8000-000000000002', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000001', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001');
-- 本文件负责 EAF 的 V56__capability_assets.sql 相关定义。
