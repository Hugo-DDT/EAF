insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-00000000000f', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-plan', '基于正式知识为内部服务请求生成有界草稿。'),
       ('53000000-0000-4000-8000-000000000010', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-registration', '只承载固定内部服务请求登记 Tool 契约。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-00000000000f', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":8000}}}',
        '{"type":"object","required":["output"],"additionalProperties":false,"properties":{"output":{"type":"string","maxLength":12000}}}',
        '21000000-0000-4000-8000-00000000000f', '1.0.0', 'p15-service-request-plan-v1', 'PUBLISHED'),
       ('53000000-0000-4000-8000-000000000010', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":12000}}}',
        '{"type":"object","required":["output"],"additionalProperties":false,"properties":{"output":{"type":"string","maxLength":12000}}}',
        '21000000-0000-4000-8000-000000000010', '1.0.0', 'p15-service-request-registration-v1', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.tool_dependency(tenant_id, workspace_id, skill_id, skill_version, tool_name, tool_version,
                                  input_schema, output_schema)
select tenant_id, workspace_id, '53000000-0000-4000-8000-000000000010', '1.0.0', name, asset_version,
       input_schema, output_schema
from tool.version where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name = 'service.request.register' and asset_version = '1.0.0' and status = 'PUBLISHED'
on conflict do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000014', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-00000000000f', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
       ('53000000-0000-4000-8000-000000000015', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000010', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
