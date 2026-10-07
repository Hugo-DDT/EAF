insert into agent.tool_binding(tenant_id, workspace_id, agent_id, agent_version, tool_name, tool_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-00000000000f', '1.0.0', 'service.request.register', '1.0.0')
on conflict do nothing;

insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('58000000-0000-4000-8000-00000000000e', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-registration', '将用户确认的内部服务请求经 Policy、独立审批、登记与同 operationId 回读核验。')
on conflict (id, workspace_id) do nothing;

insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, content_hash, status, row_version)
values ('58000000-0000-4000-8000-00000000000e', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["submissionId"],"additionalProperties":false,"properties":{"submissionId":{"type":"string","minLength":36,"maxLength":36}}}',
        '{"type":"object","required":["requestId","status","operationId"],"additionalProperties":false,"properties":{"requestId":{"type":"string","minLength":1,"maxLength":160},"status":{"type":"string","enum":["REGISTERED"]},"operationId":{"type":"string","minLength":36,"maxLength":36}}}',
        'register',
        '[{"id":"register","type":"RUN_TOOL","nextStepId":"done","capabilityId":"54000000-0000-4000-8000-000000000013","capabilityVersion":"1.0.0","toolName":"service.request.register","toolVersion":"1.0.0","inputMapping":{"submissionId":"$.input.submissionId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"done","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"requestId":"$.steps.register.output.requestId","status":"$.steps.register.output.status","operationId":"$.steps.register.output.operationId"}}]'::jsonb,
        null, 'DRAFT', 1)
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                            capability_id, capability_version, content_hash)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-00000000000e', '1.0.0', '54000000-0000-4000-8000-000000000013', '1.0.0',
        '43517612934f783d09a9e801e6c520153a4b20faaeccc5bfbfa626c439de34cb')
on conflict do nothing;

-- hash is generated from the same canonical WorkflowDefinition used by publish().
update workflow.version set status = 'PUBLISHED',
    content_hash = '051a3b499b2450a6f38147422fe78d06c81405b5008bef8bcfac4cec60bcca0d', row_version = 2
where workflow_id = '58000000-0000-4000-8000-00000000000e'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.0.0' and status = 'DRAFT';

insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id)
values ('58000000-0000-4000-8000-000000000014', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-00000000000e', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, workflow_id, workflow_version, action) do nothing;
