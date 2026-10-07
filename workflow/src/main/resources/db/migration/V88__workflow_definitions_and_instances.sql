-- Workflow 域只保存定义版本、精确依赖、实例启动快照和恢复状态。
create schema if not exists workflow;

create table workflow.definition (
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

create table workflow.version (
    workflow_id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    asset_version varchar(40) not null,
    input_schema jsonb not null check (jsonb_typeof(input_schema) = 'object' and input_schema ->> 'type' = 'object'),
    output_schema jsonb not null check (jsonb_typeof(output_schema) = 'object' and output_schema ->> 'type' = 'object'),
    entry_step_id varchar(64) not null,
    steps_json jsonb not null check (jsonb_typeof(steps_json) = 'array'),
    content_hash varchar(64),
    status varchar(20) not null check (status in ('DRAFT', 'PUBLISHED', 'WITHDRAWN')),
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null default now(),
    primary key (workflow_id, workspace_id, asset_version),
    unique (workflow_id, tenant_id, workspace_id, asset_version),
    foreign key (workflow_id, tenant_id, workspace_id)
        references workflow.definition(id, tenant_id, workspace_id),
    check (content_hash is null or content_hash ~ '^[0-9a-f]{64}$')
);

-- Capability 引用单独落表，步骤 JSON 只保存流程结构，不隐含授权。
create table workflow.capability_dependency (
    tenant_id uuid not null,
    workspace_id uuid not null,
    workflow_id uuid not null,
    workflow_version varchar(40) not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    content_hash varchar(64),
    primary key (tenant_id, workspace_id, workflow_id, workflow_version, capability_id, capability_version),
    foreign key (workflow_id, tenant_id, workspace_id, workflow_version)
        references workflow.version(workflow_id, tenant_id, workspace_id, asset_version),
    check (content_hash is null or content_hash ~ '^[0-9a-f]{64}$')
);

create table workflow.release (
    release_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    workflow_id uuid not null,
    workflow_version varchar(40) not null,
    action varchar(12) not null check (action in ('PUBLISHED', 'WITHDRAWN')),
    actor_id uuid not null,
    occurred_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, workflow_id, workflow_version, action),
    foreign key (workflow_id, tenant_id, workspace_id, workflow_version)
        references workflow.version(workflow_id, tenant_id, workspace_id, asset_version)
);

create table workflow.instance (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    actor_id uuid not null,
    initiator_id uuid not null,
    delegation_id uuid,
    source varchar(16) not null check (source in ('USER', 'EVALUATION')),
    workflow_id uuid not null,
    workflow_version varchar(40) not null,
    definition_hash varchar(64) not null check (definition_hash ~ '^[0-9a-f]{64}$'),
    root_budget_scope_id uuid not null,
    input_json jsonb not null check (jsonb_typeof(input_json) = 'object'),
    input_hash varchar(64) not null check (input_hash ~ '^[0-9a-f]{64}$'),
    definition_snapshot jsonb not null,
    dependency_snapshot jsonb not null check (jsonb_typeof(dependency_snapshot) = 'array'),
    idempotency_key_hash varchar(64) not null check (idempotency_key_hash ~ '^[0-9a-f]{64}$'),
    request_hash varchar(64) not null check (request_hash ~ '^[0-9a-f]{64}$'),
    status varchar(20) not null check (status in ('QUEUED', 'RUNNING', 'WAITING_CHILD', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')),
    result_json jsonb,
    created_at timestamptz not null default now(),
    foreign key (workflow_id, tenant_id, workspace_id, workflow_version)
        references workflow.version(workflow_id, tenant_id, workspace_id, asset_version),
    unique (tenant_id, workspace_id, actor_id, idempotency_key_hash)
);

create index workflow_instance_history_idx
    on workflow.instance(tenant_id, workspace_id, workflow_id, workflow_version, created_at desc);

create or replace function workflow.protect_published_version()
returns trigger
language plpgsql
as $$
begin
    -- 已发布定义只允许从 PUBLISHED 转为 WITHDRAWN；撤回后也不能复活或改内容。
    if old.status = 'WITHDRAWN' and new.status <> old.status then
        raise exception 'withdrawn workflow version cannot be restored';
    end if;
    if old.status = 'PUBLISHED' and new.status not in ('PUBLISHED', 'WITHDRAWN') then
        raise exception 'published workflow version cannot return to draft';
    end if;
    if old.status in ('PUBLISHED', 'WITHDRAWN') and (
        new.workflow_id is distinct from old.workflow_id
        or new.tenant_id is distinct from old.tenant_id
        or new.workspace_id is distinct from old.workspace_id
        or new.asset_version is distinct from old.asset_version
        or new.input_schema is distinct from old.input_schema
        or new.output_schema is distinct from old.output_schema
        or new.entry_step_id is distinct from old.entry_step_id
        or new.steps_json is distinct from old.steps_json
        or new.content_hash is distinct from old.content_hash
        or new.created_at is distinct from old.created_at
    ) then
        raise exception 'published workflow content is immutable';
    end if;
    return new;
end;
$$;

create trigger workflow_version_immutable
before update on workflow.version
for each row execute function workflow.protect_published_version();

create or replace function workflow.protect_published_dependency()
returns trigger
language plpgsql
as $$
declare
    current_status varchar(20);
begin
    select status into current_status from workflow.version
    where tenant_id = coalesce(old.tenant_id, new.tenant_id)
      and workspace_id = coalesce(old.workspace_id, new.workspace_id)
      and workflow_id = coalesce(old.workflow_id, new.workflow_id)
      and asset_version = coalesce(old.workflow_version, new.workflow_version);
    if current_status <> 'DRAFT' then
        raise exception 'published workflow dependencies are immutable';
    end if;
    if tg_op = 'DELETE' then return old; end if;
    return new;
end;
$$;

create trigger workflow_dependency_immutable
before update or delete on workflow.capability_dependency
for each row execute function workflow.protect_published_dependency();

create or replace function workflow.protect_release_history()
returns trigger
language plpgsql
as $$
begin
    raise exception 'workflow release history is append-only';
end;
$$;

create trigger workflow_release_append_only
before update or delete on workflow.release
for each row execute function workflow.protect_release_history();

-- Workflow 的管理权限沿用 Capability，启动权限沿用 Task 创建权限。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'workflow:read', 'ACTIVE'
from workspace."grant" where action = 'capability:read'
on conflict (workspace_id, actor_id, action) do nothing;
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'workflow:write', 'ACTIVE'
from workspace."grant" where action = 'capability:write'
on conflict (workspace_id, actor_id, action) do nothing;
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'workflow:publish', 'ACTIVE'
from workspace."grant" where action = 'capability:publish'
on conflict (workspace_id, actor_id, action) do nothing;
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'workflow:start', 'ACTIVE'
from workspace."grant" where action = 'task:create'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('58000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'customer-followup', '先分析客户风险，再按固定分支决定是否创建 CRM 跟进。');

insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, status)
values ('58000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["customerId"],"additionalProperties":false,"properties":{"customerId":{"type":"string","minLength":1,"maxLength":160}}}',
        '{"type":"object","required":["riskLevel","summary","followupCreated"],"additionalProperties":false,"properties":{"riskLevel":{"type":"string"},"summary":{"type":"string"},"followupCreated":{"type":"boolean"}}}',
        'analyze',
        '[
          {"id":"analyze","type":"RUN_CAPABILITY","nextStepId":"risk-branch","capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.0.0","toolName":null,"toolVersion":null,"inputMapping":{"input":"$.input.customerId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},
          {"id":"risk-branch","type":"BRANCH","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":"$.steps.analyze.output.riskLevel","conditionValue":"HIGH","whenTrueStepId":"create-followup","whenFalseStepId":"complete-no-action","outputMapping":{}},
          {"id":"create-followup","type":"RUN_TOOL","nextStepId":"complete-created","capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.0.0","toolName":"crm.followup.create","toolVersion":"1.0.0","inputMapping":{"customerId":"$.input.customerId","summary":"$.steps.analyze.output.summary"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},
          {"id":"complete-no-action","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"riskLevel":"$.steps.analyze.output.riskLevel","summary":"$.steps.analyze.output.summary","followupCreated":"literal:false"}},
          {"id":"complete-created","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"riskLevel":"$.steps.analyze.output.riskLevel","summary":"$.steps.analyze.output.summary","followupCreated":"literal:true"}}
        ]'::jsonb,
        'DRAFT');

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                            capability_id, capability_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-000000000001', '1.0.0',
        '54000000-0000-4000-8000-000000000001', '1.0.0');
