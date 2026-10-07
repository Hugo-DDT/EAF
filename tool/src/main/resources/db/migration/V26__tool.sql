create schema if not exists tool;

create table tool.version (
    tenant_id uuid not null,
    workspace_id uuid not null,
    name varchar(120) not null,
    asset_version varchar(40) not null,
    description varchar(500) not null,
    input_schema jsonb not null,
    output_schema jsonb not null,
    permission_action varchar(80) not null,
    effect varchar(10) not null check (effect in ('READ', 'WRITE')),
    binding_ref varchar(120) not null,
    status varchar(20) not null check (status in ('DRAFT', 'IN_REVIEW', 'PUBLISHED', 'DEPRECATED', 'REVOKED')),
    created_at timestamptz not null default now(),
    primary key (tenant_id, workspace_id, name, asset_version)
);

insert into tool.version(tenant_id, workspace_id, name, asset_version, description, input_schema, output_schema, permission_action, effect, binding_ref, status)
select tenant_id, id, 'crm.customer.query', '1.0.0', '读取测试 CRM 的合成客户分析字段。',
       '{"type":"object","required":["customerId"],"additionalProperties":false,"properties":{"customerId":{"type":"string","maxLength":160,"minLength":1}}}',
       '{"type":"object","required":["customerId","renewalStatus","lastContactDate","complaintSummary"],"additionalProperties":false}',
       'crm:customer:read', 'READ', 'test-crm.customer-read', 'PUBLISHED'
from workspace.workspace where status = 'ACTIVE';

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select g.tenant_id, g.workspace_id, g.actor_id, 'tool:read', 'ACTIVE'
from workspace."grant" g
where g.action = 'agent:read'
on conflict (workspace_id, actor_id, action) do nothing;
-- 本文件负责 EAF 的 V26__tool.sql 相关定义。
