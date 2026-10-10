insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('54000000-0000-4000-8000-000000000022', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'p27-business-tool', '为固定 P27 业务连接 Workflow 提供四个受限 Tool。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
values ('54000000-0000-4000-8000-000000000022', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0', '20000000-0000-4000-8000-000000000021', '1.0.0',
        '53000000-0000-4000-8000-000000000034', '1.0.0', '21000000-0000-4000-8000-000000000021', '1.0.0',
        'p27-business-tool-v1', 'PUBLISHED')
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.tool_dependency(tenant_id, workspace_id, capability_id, capability_version,
                                       tool_name, tool_version, input_schema, output_schema)
select tenant_id, workspace_id, '54000000-0000-4000-8000-000000000022', '1.0.0', name, asset_version,
       input_schema, output_schema
from tool.version where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name in ('oa.todo.list', 'oa.todo.get', 'service.request.status.get', 'service.request.result.record')
  and asset_version = '1.0.0' and status = 'PUBLISHED'
on conflict do nothing;

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
values ('54000000-0000-4000-8000-000000000122', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000022', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
