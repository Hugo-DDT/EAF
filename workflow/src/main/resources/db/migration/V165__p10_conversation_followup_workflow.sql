-- P10 只新增稳定的来源绑定字段；CRM 写入与审批仍沿用 P9/P7 执行链路。
insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, content_hash, status, row_version)
select workflow_id, tenant_id, workspace_id, '1.2.0',
       '{"type":"object","required":["customerId","sourceTaskId","sourceTaskVersion","summary"],"additionalProperties":false,"properties":{"customerId":{"type":"string","minLength":1,"maxLength":160},"sourceTaskId":{"type":"string","minLength":36,"maxLength":36},"sourceTaskVersion":{"type":"string","minLength":1,"maxLength":30},"summary":{"type":"string","minLength":1,"maxLength":2000},"conversationId":{"type":"string","minLength":36,"maxLength":36},"briefRevision":{"type":"integer"}}}',
       output_schema, entry_step_id, steps_json, 'ebae6ff889568bf35b13a36d78757bf978a72733da974e22106f8367f8a68c0f', 'DRAFT', 1
from workflow.version
where workflow_id = '58000000-0000-4000-8000-000000000009'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.1.0' and status = 'PUBLISHED'
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version, capability_id, capability_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-000000000009', '1.2.0', '54000000-0000-4000-8000-000000000001', '1.5.0')
on conflict do nothing;

update workflow.version
set status = 'PUBLISHED', row_version = 2
where workflow_id = '58000000-0000-4000-8000-000000000009'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.2.0' and status = 'DRAFT';

insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id)
values ('58000000-0000-4000-8000-00000000000c', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-000000000009', '1.2.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, workflow_id, workflow_version, action) do nothing;
