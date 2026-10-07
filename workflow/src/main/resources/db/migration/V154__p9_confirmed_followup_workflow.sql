-- 只包含用户确认后的 RUN_TOOL 和完成步骤；审批、幂等与写入核验沿用现有 Execution。
insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('58000000-0000-4000-8000-000000000009', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'p9-customer-followup', 'Create a CRM follow-up after user confirmation; the existing execution approval remains required.')
on conflict (id, workspace_id) do nothing;

insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, content_hash, status, row_version)
values ('58000000-0000-4000-8000-000000000009', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["customerId","sourceTaskId","sourceTaskVersion","summary"],"additionalProperties":false,"properties":{"customerId":{"type":"string","minLength":1,"maxLength":160},"sourceTaskId":{"type":"string","minLength":36,"maxLength":36},"sourceTaskVersion":{"type":"string","minLength":1,"maxLength":30},"summary":{"type":"string","minLength":1,"maxLength":2000}}}',
        '{"type":"object","required":["operationId","externalId","status"],"additionalProperties":false,"properties":{"operationId":{"type":"string"},"externalId":{"type":"string"},"status":{"type":"string"}}}',
        'write',
        '[{"id":"write","type":"RUN_TOOL","nextStepId":"complete","capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.0.0","toolName":"crm.followup.create","toolVersion":"1.0.0","inputMapping":{"customerId":"$.input.customerId","summary":"$.input.summary"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"complete","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"operationId":"$.steps.write.output.operationId","externalId":"$.steps.write.output.externalId","status":"$.steps.write.output.status"}}]'::jsonb,
        '0e7b9ab7dc1719e42c602d67574d24e40f2ee58471d968562cdd4c16eea4f571', 'DRAFT', 1)
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                            capability_id, capability_version, content_hash)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-000000000009', '1.0.0',
        '54000000-0000-4000-8000-000000000001', '1.0.0',
        '599d7283777f179f951ee6a4e7afb5118752719f567ed0288f4614c029d2c747')
on conflict do nothing;

-- 发布依赖先绑定在 DRAFT 版本上；发布后数据库触发器将冻结依赖集合与流程正文。
update workflow.version
set status = 'PUBLISHED', row_version = 2
where workflow_id = '58000000-0000-4000-8000-000000000009'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.0.0' and status = 'DRAFT';

insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id)
values ('58000000-0000-4000-8000-00000000000a', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-000000000009', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, workflow_id, workflow_version, action) do nothing;
