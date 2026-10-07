-- Agent 2.2.0 同时固定绑定 CRM 只读 1.1.0 与受控写入 1.1.0。
insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, model_profile_id, status)
select id, tenant_id, workspace_id, 'customer-risk-analysis-crm-write-contract', '2.2.0',
       prompt_id, prompt_version, model_profile_id, 'PUBLISHED'
from agent.version
where id = '20000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '2.1.0' and status = 'PUBLISHED'
on conflict (id, workspace_id, asset_version) do nothing;

insert into agent.tool_binding(tenant_id, workspace_id, agent_id, agent_version, tool_name, tool_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001', '2.2.0', 'crm.customer.query', '1.1.0'),
       ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001', '2.2.0', 'crm.followup.create', '1.1.0')
on conflict do nothing;
