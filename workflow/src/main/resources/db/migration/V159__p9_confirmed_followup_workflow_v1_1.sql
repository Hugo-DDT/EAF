-- P9 v1.1 映射有类型声明的 CRM 核验结果；已发布的初版定义仍保留。
insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, content_hash, status, row_version)
select workflow_id, tenant_id, workspace_id, '1.1.0', input_schema, output_schema, entry_step_id,
       jsonb_set(jsonb_set(steps_json, '{0,capabilityVersion}', '"1.5.0"'::jsonb), '{0,toolVersion}', '"1.2.0"'::jsonb),
       'aa2ff02d8324c264743152a8af8487b7581096213eaa6ab1bd7c9db339631ea9', 'DRAFT', 1
from workflow.version
where workflow_id = '58000000-0000-4000-8000-000000000009'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.0.0' and status = 'PUBLISHED'
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version, capability_id, capability_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-000000000009', '1.1.0',
        '54000000-0000-4000-8000-000000000001', '1.5.0')
on conflict do nothing;

update workflow.version
set status = 'PUBLISHED', row_version = 2
where workflow_id = '58000000-0000-4000-8000-000000000009'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.1.0' and status = 'DRAFT';

insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id)
values ('58000000-0000-4000-8000-00000000000b', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-000000000009', '1.1.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, workflow_id, workflow_version, action) do nothing;
