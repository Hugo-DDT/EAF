-- P4 的旧 Tool 只声明必填输出名；P9 新版本补全类型，旧发布版本保持不变。
insert into tool.version(tenant_id, workspace_id, name, asset_version, description, input_schema,
                        output_schema, permission_action, effect, binding_ref, status)
select tenant_id, workspace_id, name, '1.2.0',
       '为用户确认的 CRM 跟进创建请求提供可映射的核验结果 Schema。', input_schema,
       '{"type":"object","required":["operationId","externalId","status"],"additionalProperties":false,"properties":{"operationId":{"type":"string"},"externalId":{"type":"string"},"status":{"type":"string"}}}'::jsonb,
       permission_action, effect, binding_ref, 'PUBLISHED'
from tool.version
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name = 'crm.followup.create' and asset_version = '1.1.0' and status = 'PUBLISHED'
on conflict (tenant_id, workspace_id, name, asset_version) do nothing;
