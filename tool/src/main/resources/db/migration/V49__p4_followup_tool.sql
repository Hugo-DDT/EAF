-- P4 只开放一个可审计的写工具；写入仍必须经过审批与执行模块。
insert into tool.version(tenant_id, workspace_id, name, asset_version, description, input_schema, output_schema, permission_action, effect, binding_ref, status)
select tenant_id, id, 'crm.followup.create', '1.0.0', '为测试 CRM 客户创建跟进记录。',
       '{"type":"object","required":["customerId","summary"],"additionalProperties":false,"properties":{"customerId":{"type":"string","maxLength":160,"minLength":1},"summary":{"type":"string","maxLength":2000,"minLength":1}}}',
       '{"type":"object","required":["operationId","externalId","status"],"additionalProperties":false}',
       'crm:followup:create', 'WRITE', 'test-crm.followup-create', 'PUBLISHED'
from workspace.workspace where status = 'ACTIVE'
on conflict (tenant_id, workspace_id, name, asset_version) do nothing;

insert into agent.tool_binding(tenant_id, workspace_id, agent_id, agent_version, tool_name, tool_version)
select w.tenant_id, w.id, '20000000-0000-4000-8000-000000000001', '2.0.0', 'crm.followup.create', '1.0.0'
from workspace.workspace w
where w.status = 'ACTIVE'
on conflict do nothing;
