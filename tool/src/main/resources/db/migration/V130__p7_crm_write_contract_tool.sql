-- 新写入 Tool 固定到 P7 CRM 写入契约；旧版测试 CRM Tool 快照保持不变。
insert into tool.version(tenant_id, workspace_id, name, asset_version, description, input_schema,
                        output_schema, permission_action, effect, binding_ref, status)
select tenant_id, workspace_id, name, '1.1.0',
       '为版本化 CRM 写入契约创建需审批并可按 operationId 核验的跟进记录。',
       input_schema, output_schema, permission_action, effect,
       'p7-crm-write-contract.followup-create', 'PUBLISHED'
from tool.version
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name = 'crm.followup.create' and asset_version = '1.0.0' and status = 'PUBLISHED'
on conflict (tenant_id, workspace_id, name, asset_version) do nothing;
