-- Skill 新版本将 P9 写 Tool Schema 快照固定为 1.2.0。
insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
select skill_id, tenant_id, workspace_id, '1.3.0', input_schema, output_schema,
       prompt_id, prompt_version, 'p9-followup-tool-output-v1', 'PUBLISHED'
from skill.version
where skill_id = '53000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.2.0' and status = 'PUBLISHED'
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.tool_dependency(tenant_id, workspace_id, skill_id, skill_version, tool_name, tool_version,
                                 input_schema, output_schema)
select tenant_id, workspace_id, skill_id, '1.3.0', tool_name, tool_version, input_schema, output_schema
from skill.tool_dependency
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and skill_id = '53000000-0000-4000-8000-000000000001'
  and skill_version = '1.2.0' and tool_name = 'crm.customer.query'
on conflict do nothing;

insert into skill.tool_dependency(tenant_id, workspace_id, skill_id, skill_version, tool_name, tool_version,
                                 input_schema, output_schema)
select tenant_id, workspace_id, '53000000-0000-4000-8000-000000000001', '1.3.0',
       name, asset_version, input_schema, output_schema
from tool.version
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name = 'crm.followup.create' and asset_version = '1.2.0' and status = 'PUBLISHED'
on conflict do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000005', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000001', '1.3.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
