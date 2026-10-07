-- Capability 1.4.0 固定引用 CRM 写入 Agent、Skill 与两项 Tool 版本。
insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
values ('54000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.4.0', '20000000-0000-4000-8000-000000000001', '2.2.0',
        '53000000-0000-4000-8000-000000000001', '1.2.0',
        '21000000-0000-4000-8000-000000000001', '2.0.0', 'p7-crm-write-contract-v1', 'PUBLISHED')
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.tool_dependency(tenant_id, workspace_id, capability_id, capability_version,
                                       tool_name, tool_version, input_schema, output_schema)
select tenant_id, workspace_id, '54000000-0000-4000-8000-000000000001', '1.4.0',
       name, asset_version, input_schema, output_schema
from tool.version
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and ((name = 'crm.customer.query' and asset_version = '1.1.0')
    or (name = 'crm.followup.create' and asset_version = '1.1.0'))
  and status = 'PUBLISHED'
on conflict (tenant_id, workspace_id, capability_id, capability_version, tool_name) do nothing;

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
values ('54000000-0000-4000-8000-000000000004', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000001', '1.4.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
