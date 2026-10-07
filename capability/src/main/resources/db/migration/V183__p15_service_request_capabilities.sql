insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('54000000-0000-4000-8000-000000000012', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-plan', '只读分析服务请求并引用当前正式知识。'),
       ('54000000-0000-4000-8000-000000000013', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-registration', '仅供固定登记 Workflow 调用内部服务请求登记 Tool。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
values ('54000000-0000-4000-8000-000000000012', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '20000000-0000-4000-8000-000000000010', '1.0.0',
        '53000000-0000-4000-8000-00000000000f', '1.0.0',
        '21000000-0000-4000-8000-00000000000f', '1.0.0', 'p15-service-request-plan-v1', 'PUBLISHED'),
       ('54000000-0000-4000-8000-000000000013', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '20000000-0000-4000-8000-00000000000f', '1.0.0',
        '53000000-0000-4000-8000-000000000010', '1.0.0',
        '21000000-0000-4000-8000-000000000010', '1.0.0', 'p15-service-request-registration-v1', 'PUBLISHED')
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.tool_dependency(tenant_id, workspace_id, capability_id, capability_version,
                                       tool_name, tool_version, input_schema, output_schema)
select tenant_id, workspace_id, '54000000-0000-4000-8000-000000000013', '1.0.0', name, asset_version,
       input_schema, output_schema
from tool.version where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name = 'service.request.register' and asset_version = '1.0.0' and status = 'PUBLISHED'
on conflict do nothing;

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
values ('54000000-0000-4000-8000-000000000016', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000012', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
       ('54000000-0000-4000-8000-000000000017', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000013', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
