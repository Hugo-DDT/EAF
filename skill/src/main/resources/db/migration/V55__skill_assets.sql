create schema if not exists skill;

-- Skill 定义只持有名称和可信 Owner；行为内容按版本保存，发布后不原地改写。
create table skill.definition (
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

create table skill.version (
    skill_id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    asset_version varchar(40) not null,
    input_schema jsonb not null check (jsonb_typeof(input_schema) = 'object' and input_schema ->> 'type' = 'object'),
    output_schema jsonb not null check (jsonb_typeof(output_schema) = 'object' and output_schema ->> 'type' = 'object'),
    prompt_id uuid not null,
    prompt_version varchar(40) not null,
    evaluation_ref varchar(120) not null,
    status varchar(20) not null check (status in ('DRAFT', 'IN_REVIEW', 'PUBLISHED', 'DEPRECATED', 'REVOKED')),
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null default now(),
    primary key (skill_id, workspace_id, asset_version),
    unique (skill_id, tenant_id, workspace_id, asset_version),
    foreign key (skill_id, tenant_id, workspace_id) references skill.definition(id, tenant_id, workspace_id)
);

-- 保存当时实际绑定的 Tool Schema；解析 Skill 时重新比较，发现同版本 Schema 漂移即拒绝。
create table skill.tool_dependency (
    tenant_id uuid not null,
    workspace_id uuid not null,
    skill_id uuid not null,
    skill_version varchar(40) not null,
    tool_name varchar(120) not null,
    tool_version varchar(40) not null,
    input_schema jsonb not null,
    output_schema jsonb not null,
    primary key (tenant_id, workspace_id, skill_id, skill_version, tool_name),
    foreign key (skill_id, tenant_id, workspace_id, skill_version)
        references skill.version(skill_id, tenant_id, workspace_id, asset_version)
);

-- 发布/撤回事实独立追加保存，用于解释版本历史而不改写旧快照。
create table skill.release (
    release_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    skill_id uuid not null,
    skill_version varchar(40) not null,
    action varchar(10) not null check (action in ('PUBLISHED', 'REVOKED')),
    actor_id uuid not null,
    occurred_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, skill_id, skill_version, action),
    foreign key (skill_id, tenant_id, workspace_id, skill_version)
        references skill.version(skill_id, tenant_id, workspace_id, asset_version)
);

create or replace function skill.reject_published_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.status in ('PUBLISHED', 'DEPRECATED', 'REVOKED') and (
        new.skill_id is distinct from old.skill_id
        or new.tenant_id is distinct from old.tenant_id
        or new.workspace_id is distinct from old.workspace_id
        or new.asset_version is distinct from old.asset_version
        or new.input_schema is distinct from old.input_schema
        or new.output_schema is distinct from old.output_schema
        or new.prompt_id is distinct from old.prompt_id
        or new.prompt_version is distinct from old.prompt_version
        or new.evaluation_ref is distinct from old.evaluation_ref
        or new.created_at is distinct from old.created_at
    ) then
        raise exception 'published skill content is immutable';
    end if;
    return new;
end;
$$;

create trigger skill_published_immutable
before update on skill.version
for each row execute function skill.reject_published_mutation();

create or replace function skill.reject_published_dependency_mutation()
returns trigger
language plpgsql
as $$
declare
    current_status varchar(20);
begin
    select status into current_status from skill.version
    where tenant_id = coalesce(old.tenant_id, new.tenant_id)
      and workspace_id = coalesce(old.workspace_id, new.workspace_id)
      and skill_id = coalesce(old.skill_id, new.skill_id)
      and asset_version = coalesce(old.skill_version, new.skill_version);
    if current_status <> 'DRAFT' then
        raise exception 'published skill dependencies are immutable';
    end if;
    if tg_op = 'DELETE' then
        return old;
    end if;
    return new;
end;
$$;

create trigger skill_dependency_immutable
before update or delete on skill.tool_dependency
for each row execute function skill.reject_published_dependency_mutation();

create or replace function skill.reject_release_mutation()
returns trigger
language plpgsql
as $$
begin
    raise exception 'skill release history is append-only';
end;
$$;

create trigger skill_release_append_only
before update or delete on skill.release
for each row execute function skill.reject_release_mutation();

-- 可发现权限沿用 Agent 可见身份；写入与发布权限沿用已配置的知识维护角色。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'skill:read', 'ACTIVE'
from workspace."grant" where action = 'agent:read'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'skill:write', 'ACTIVE'
from workspace."grant" where action = 'knowledge:write'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'skill:publish', 'ACTIVE'
from workspace."grant" where action = 'knowledge:publish'
on conflict (workspace_id, actor_id, action) do nothing;

insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'customer-risk-analysis', '基于授权客户事实形成带不确定性的风险分析。');

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","maxLength":8000,"minLength":1}}}',
        '{"type":"object","required":["riskLevel","summary","reasons","uncertainties"],"additionalProperties":false,"properties":{"riskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"summary":{"type":"string"},"reasons":{"type":"array","items":{"type":"string"}},"uncertainties":{"type":"array","items":{"type":"string"}}}}',
        '21000000-0000-4000-8000-000000000001', '2.0.0', 'p2-v1', 'PUBLISHED');

insert into skill.tool_dependency(tenant_id, workspace_id, skill_id, skill_version, tool_name, tool_version, input_schema, output_schema)
select '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
       '53000000-0000-4000-8000-000000000001', '1.0.0', name, asset_version, input_schema, output_schema
from tool.version
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name = 'crm.customer.query' and asset_version = '1.0.0' and status = 'PUBLISHED';

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000002', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000001', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001');
-- 本文件负责 EAF 的 V55__skill_assets.sql 相关定义。
