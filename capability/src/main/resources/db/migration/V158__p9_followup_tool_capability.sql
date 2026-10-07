-- Capability 新版本同时固定兼容 Agent、Skill 与核验输出明确的写 Tool。
insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
select capability_id, tenant_id, workspace_id, '1.5.0', agent_id, '2.3.0', skill_id, '1.3.0',
       prompt_id, prompt_version, 'p9-followup-tool-output-v1', 'PUBLISHED'
from capability.version
where capability_id = '54000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.4.0' and status = 'PUBLISHED'
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.tool_dependency(tenant_id, workspace_id, capability_id, capability_version,
                                       tool_name, tool_version, input_schema, output_schema)
select tenant_id, workspace_id, capability_id, '1.5.0', tool_name, tool_version, input_schema, output_schema
from capability.tool_dependency
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and capability_id = '54000000-0000-4000-8000-000000000001'
  and capability_version = '1.4.0' and tool_name = 'crm.customer.query'
on conflict do nothing;

insert into capability.tool_dependency(tenant_id, workspace_id, capability_id, capability_version,
                                       tool_name, tool_version, input_schema, output_schema)
select tenant_id, workspace_id, '54000000-0000-4000-8000-000000000001', '1.5.0',
       name, asset_version, input_schema, output_schema
from tool.version
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name = 'crm.followup.create' and asset_version = '1.2.0' and status = 'PUBLISHED'
on conflict do nothing;

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
values ('54000000-0000-4000-8000-000000000005', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000001', '1.5.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
