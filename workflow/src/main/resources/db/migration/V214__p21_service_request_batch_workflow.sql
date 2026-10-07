insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('58000000-0000-4000-8000-000000000018', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-batch-analysis', '同一批次项目固定并行执行正式知识查证与 TEAM 经验整理。')
on conflict (id, workspace_id) do nothing;

insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, content_hash, status, row_version)
values ('58000000-0000-4000-8000-000000000018', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["requestText","scenarioKey","experienceRefsJson"],"additionalProperties":false,"properties":{"requestText":{"type":"string","minLength":1,"maxLength":8000},"scenarioKey":{"type":"string","maxLength":64},"experienceRefsJson":{"type":"string","minLength":2,"maxLength":1000}}}',
        '{"type":"object","required":["knowledgeTaskId","experienceTaskId"],"additionalProperties":false,"properties":{"knowledgeTaskId":{"type":"string"},"experienceTaskId":{"type":"string"}}}',
        'gather',
        '[{"id":"gather","type":"PARALLEL_READ","nextStepId":"done","capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{},"parallelBranches":[{"role":"knowledge","capabilityId":"54000000-0000-4000-8000-000000000016","capabilityVersion":"1.0.0","inputMapping":{"requestText":"$.input.requestText"}},{"role":"experience","capabilityId":"54000000-0000-4000-8000-000000000017","capabilityVersion":"1.0.0","inputMapping":{"requestText":"$.input.requestText","scenarioKey":"$.input.scenarioKey","experienceRefsJson":"$.input.experienceRefsJson"}}]},{"id":"done","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"knowledgeTaskId":"$.steps.gather.output.knowledgeTaskId","experienceTaskId":"$.steps.gather.output.experienceTaskId"}}]'::jsonb,
        '1e3cbee5537c4252bffe404784662395a7e37a96fb834d6ca7a656eccaa7a3c0', 'DRAFT', 1)
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                            capability_id, capability_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-000000000018', '1.0.0', '54000000-0000-4000-8000-000000000016', '1.0.0'),
       ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-000000000018', '1.0.0', '54000000-0000-4000-8000-000000000017', '1.0.0')
on conflict do nothing;

update workflow.version set status = 'PUBLISHED', row_version = 2
where workflow_id = '58000000-0000-4000-8000-000000000018' and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.0.0' and status = 'DRAFT';

insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id)
values ('58000000-0000-4000-8000-000000000030', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-000000000018', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, workflow_id, workflow_version, action) do nothing;
