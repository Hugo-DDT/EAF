insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('58000000-0000-4000-8000-00000000001d', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'project-brief-handoff', '固定项目证据整理、人工补充与明确接收。')
on conflict (id, workspace_id) do nothing;

insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, content_hash, status, row_version)
values ('58000000-0000-4000-8000-00000000001d', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["title","goal","creatorId","reviewerId","recipientId","evidenceBundleJson","evidenceBundleHash"],"additionalProperties":false,"properties":{"title":{"type":"string","minLength":1,"maxLength":120},"goal":{"type":"string","minLength":1,"maxLength":1000},"creatorId":{"type":"string","minLength":36,"maxLength":36},"reviewerId":{"type":"string","minLength":36,"maxLength":36},"recipientId":{"type":"string","minLength":36,"maxLength":36},"evidenceBundleJson":{"type":"string","minLength":2,"maxLength":24000},"evidenceBundleHash":{"type":"string","minLength":64,"maxLength":64}}}',
        '{"type":"object","required":["disposition","completedBy","completedAt","artifactVersion"],"additionalProperties":false,"properties":{"disposition":{"type":"string","enum":["RECEIVED","NEEDS_FOLLOWUP"]},"completedBy":{"type":"string","minLength":36,"maxLength":36},"completedAt":{"type":"string","minLength":1,"maxLength":50},"artifactVersion":{"type":"string","enum":["2"]}}}',
        'prepare',
        '[{"id":"prepare","type":"RUN_CAPABILITY","nextStepId":"review","capabilityId":"54000000-0000-4000-8000-000000000023","capabilityVersion":"1.0.0","toolName":null,"toolVersion":null,"inputMapping":{"evidenceBundleJson":"$.input.evidenceBundleJson","evidenceBundleHash":"$.input.evidenceBundleHash"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"review","type":"HUMAN_TASK","nextStepId":"handoff","capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{"assigneeId":"$.input.reviewerId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"handoff","type":"HUMAN_TASK","nextStepId":"complete","capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{"assigneeId":"$.input.recipientId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"complete","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"disposition":"$.steps.handoff.output.disposition","completedBy":"$.steps.handoff.output.completedBy","completedAt":"$.steps.handoff.output.completedAt","artifactVersion":"$.steps.handoff.output.artifactVersion"}}]'::jsonb,
        null, 'DRAFT', 1)
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                           capability_id, capability_version, content_hash)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-00000000001d', '1.0.0', '54000000-0000-4000-8000-000000000023', '1.0.0',
        'e7afec391708bf6e6df0658d843c05e42ca5d3376f97f2327b8190b2d2b09a1a')
on conflict do nothing;

update workflow.version set status = 'PUBLISHED',
    content_hash = '2e9803fa80caa02903fc311b2aac0957bce41212eb1ca3aa1699bef3c6108013', row_version = 2
where workflow_id = '58000000-0000-4000-8000-00000000001d'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.0.0' and status = 'DRAFT';

insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id)
values ('58000000-0000-4000-8000-000000000020', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-00000000001d', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, workflow_id, workflow_version, action) do nothing;
