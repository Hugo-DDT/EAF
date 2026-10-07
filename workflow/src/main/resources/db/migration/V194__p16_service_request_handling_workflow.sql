insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('58000000-0000-4000-8000-00000000000f', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-handling', '固定准备建议、人工处理、结果整理与安全完成摘要。')
on conflict (id, workspace_id) do nothing;

insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, content_hash, status, row_version)
values ('58000000-0000-4000-8000-00000000000f', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["submissionId","registrationWorkflowId","registrationExecutionId","operationId","requestId","sharedBrief","assigneeId"],"additionalProperties":false,"properties":{"submissionId":{"type":"string","minLength":36,"maxLength":36},"registrationWorkflowId":{"type":"string","minLength":36,"maxLength":36},"registrationExecutionId":{"type":"string","minLength":36,"maxLength":36},"operationId":{"type":"string","minLength":36,"maxLength":36},"requestId":{"type":"string","minLength":1,"maxLength":160},"sharedBrief":{"type":"string","minLength":1,"maxLength":2000},"assigneeId":{"type":"string","minLength":36,"maxLength":36}}}',
        '{"type":"object","required":["requestId","outcome","summary","nextAction","completedBy","completedAt","resultSummary","remainingWork"],"additionalProperties":false,"properties":{"requestId":{"type":"string","minLength":1,"maxLength":160},"outcome":{"type":"string","enum":["COMPLETED","BLOCKED","NEEDS_FOLLOWUP"]},"summary":{"type":"string","minLength":1,"maxLength":2000},"nextAction":{"type":"string","maxLength":1000},"completedBy":{"type":"string","minLength":36,"maxLength":36},"completedAt":{"type":"string","minLength":1,"maxLength":50},"resultSummary":{"type":"string","minLength":1,"maxLength":2000},"remainingWork":{"type":"string","maxLength":1000}}}',
        'prepare',
        '[{"id":"prepare","type":"RUN_CAPABILITY","nextStepId":"handle","capabilityId":"54000000-0000-4000-8000-000000000014","capabilityVersion":"1.0.0","toolName":null,"toolVersion":null,"inputMapping":{"sharedBrief":"$.input.sharedBrief"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"handle","type":"HUMAN_TASK","nextStepId":"summarize","capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{"assigneeId":"$.input.assigneeId","sharedBrief":"$.input.sharedBrief","handlingAdvice":"$.steps.prepare.output.handlingAdvice","cautions":"$.steps.prepare.output.cautions"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"summarize","type":"RUN_CAPABILITY","nextStepId":"done","capabilityId":"54000000-0000-4000-8000-000000000015","capabilityVersion":"1.0.0","toolName":null,"toolVersion":null,"inputMapping":{"sharedBrief":"$.input.sharedBrief","outcome":"$.steps.handle.output.outcome","summary":"$.steps.handle.output.summary","nextAction":"$.steps.handle.output.nextAction"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"done","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"requestId":"$.input.requestId","outcome":"$.steps.handle.output.outcome","summary":"$.steps.handle.output.summary","nextAction":"$.steps.handle.output.nextAction","completedBy":"$.steps.handle.output.completedBy","completedAt":"$.steps.handle.output.completedAt","resultSummary":"$.steps.summarize.output.resultSummary","remainingWork":"$.steps.summarize.output.remainingWork"}}]'::jsonb,
        null, 'DRAFT', 1)
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                            capability_id, capability_version, content_hash)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-00000000000f', '1.0.0', '54000000-0000-4000-8000-000000000014', '1.0.0', 'f9567940661e67e531491287416d46d5908f5645fbe72e1e9b0292e261843024'),
       ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-00000000000f', '1.0.0', '54000000-0000-4000-8000-000000000015', '1.0.0', 'fb1e6e8888dfb99c8bf4cc924e9bc98ab4103fd6fc8ad82960b7e61757a86114')
on conflict do nothing;

-- 依赖先绑定在 DRAFT 版本上；发布后由数据库触发器冻结流程正文和依赖集合。
update workflow.version set status = 'PUBLISHED',
    content_hash = 'ca4e6d42e4a655b41df4897bb756877ccdc0a6a3fc74f28588cebe93a79d9029', row_version = 2
where workflow_id = '58000000-0000-4000-8000-00000000000f'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.0.0' and status = 'DRAFT';

insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id)
values ('58000000-0000-4000-8000-000000000015', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-00000000000f', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, workflow_id, workflow_version, action) do nothing;
