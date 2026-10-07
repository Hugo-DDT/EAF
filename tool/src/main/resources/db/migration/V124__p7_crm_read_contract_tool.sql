-- 发布新的只读 Tool 版本，保留已发布 1.0.0 的输入/输出快照不变。
insert into tool.version(tenant_id, workspace_id, name, asset_version, description, input_schema,
                        output_schema, permission_action, effect, binding_ref, status)
select tenant_id, workspace_id, name, '1.1.0',
       '读取 CRM 契约夹具客户事实，并保留来源标识和外部版本。', input_schema,
       '{"type":"object","required":["customerId","renewalStatus","lastContactDate","complaintSummary","sourceId","externalVersion"],"additionalProperties":false,"properties":{"customerId":{"type":"string","minLength":1,"maxLength":160},"renewalStatus":{"type":"string","minLength":1,"maxLength":80},"lastContactDate":{"type":"string","minLength":1,"maxLength":40},"complaintSummary":{"type":"string","maxLength":2000},"sourceId":{"type":"string","minLength":1,"maxLength":160},"externalVersion":{"type":"string","minLength":1,"maxLength":120}}}',
       permission_action, effect, 'p7-crm-read-contract.customer-read', 'PUBLISHED'
from tool.version
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and name = 'crm.customer.query' and asset_version = '1.0.0' and status = 'PUBLISHED'
on conflict (tenant_id, workspace_id, name, asset_version) do nothing;
