insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-000000000034', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'p27-business-tool', '固定 P27 OA 查询与服务台结果同步 Tool 合同。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-000000000034', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":4000}}}',
        '{"type":"object","required":["output"],"additionalProperties":false,"properties":{"output":{"type":"string","maxLength":12000}}}',
        '21000000-0000-4000-8000-000000000021', '1.0.0', 'p27-business-tool-v1', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.tool_dependency(tenant_id, workspace_id, skill_id, skill_version, tool_name, tool_version,
                                 input_schema, output_schema)
select tenant_id, workspace_id, '53000000-0000-4000-8000-000000000034', '1.0.0', name, asset_version,
       input_schema, output_schema
from tool.version where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name in ('oa.todo.list', 'oa.todo.get', 'service.request.status.get', 'service.request.result.record')
  and asset_version = '1.0.0' and status = 'PUBLISHED'
on conflict do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000134', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000034', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
