create table agent.tool_binding (
    tenant_id uuid not null,
    workspace_id uuid not null,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    tool_name varchar(120) not null,
    tool_version varchar(40) not null,
    primary key (tenant_id, workspace_id, agent_id, agent_version, tool_name, tool_version)
);

insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, model_profile_id, status)
select '20000000-0000-4000-8000-000000000001', tenant_id, id, 'customer-risk-analysis-tools', '2.0.0',
       '21000000-0000-4000-8000-000000000001', '2.0.0', '22000000-0000-4000-8000-000000000001', 'PUBLISHED'
from workspace.workspace where id in ('10000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002')
on conflict (id, workspace_id, asset_version) do nothing;

insert into agent.tool_binding(tenant_id, workspace_id, agent_id, agent_version, tool_name, tool_version)
select w.tenant_id, w.id, '20000000-0000-4000-8000-000000000001', '2.0.0', 'crm.customer.query', '1.0.0'
from workspace.workspace w where w.id in ('10000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002');
-- 本文件负责 EAF 的 V31__agent_tool_binding.sql 相关定义。
