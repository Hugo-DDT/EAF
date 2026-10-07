insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('58000000-0000-4000-8000-000000000002', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'customer-followup-analysis-baseline', 'P7 协作评测基线：仅运行分析并返回建议，不执行分支或写入。')
on conflict (id, workspace_id) do nothing;

insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, status)
values ('58000000-0000-4000-8000-000000000002', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["customerId"],"additionalProperties":false,"properties":{"customerId":{"type":"string","minLength":1,"maxLength":160}}}',
        '{"type":"object","required":["riskLevel","summary"],"additionalProperties":false,"properties":{"riskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"summary":{"type":"string"}}}',
        'analyze',
        '[
          {"id":"analyze","type":"RUN_CAPABILITY","nextStepId":"complete","capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.0.0","toolName":null,"toolVersion":null,"inputMapping":{"input":"$.input.customerId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},
          {"id":"complete","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"riskLevel":"$.steps.analyze.output.riskLevel","summary":"$.steps.analyze.output.summary"}}
        ]'::jsonb,
        'DRAFT')
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                            capability_id, capability_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-000000000002', '1.0.0',
        '54000000-0000-4000-8000-000000000001', '1.0.0')
on conflict do nothing;

insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('58000000-0000-4000-8000-000000000003', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'customer-followup-analysis-reviewer', 'P7 reviewer 对照：仅运行分析和只读复核，不执行分支或写入。')
on conflict (id, workspace_id) do nothing;

insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, status)
values ('58000000-0000-4000-8000-000000000003', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["customerId"],"additionalProperties":false,"properties":{"customerId":{"type":"string","minLength":1,"maxLength":160}}}',
        '{"type":"object","required":["riskLevel","reviewRiskLevel","summary","reviewRationale","reviewCitations"],"additionalProperties":false,"properties":{"riskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"reviewRiskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"summary":{"type":"string"},"reviewRationale":{"type":"string"},"reviewCitations":{"type":"array","items":{"type":"string"}}}}',
        'analyze',
        '[
          {"id":"analyze","type":"RUN_CAPABILITY","nextStepId":"peer-review","capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.0.0","toolName":null,"toolVersion":null,"inputMapping":{"input":"$.input.customerId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},
          {"id":"peer-review","type":"RUN_TOOL","nextStepId":"complete","capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.1.0","toolName":"agent.risk.review","toolVersion":"1.0.0","inputMapping":{"customerId":"$.input.customerId","riskSummary":"$.steps.analyze.output.summary"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},
          {"id":"complete","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"riskLevel":"$.steps.analyze.output.riskLevel","reviewRiskLevel":"$.steps.peer-review.output.riskLevel","summary":"$.steps.analyze.output.summary","reviewRationale":"$.steps.peer-review.output.rationale","reviewCitations":"$.steps.peer-review.output.citations"}}
        ]'::jsonb,
        'DRAFT')
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                            capability_id, capability_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-000000000003', '1.0.0',
        '54000000-0000-4000-8000-000000000001', '1.0.0'),
       ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-000000000003', '1.0.0',
        '54000000-0000-4000-8000-000000000001', '1.1.0')
on conflict do nothing;
