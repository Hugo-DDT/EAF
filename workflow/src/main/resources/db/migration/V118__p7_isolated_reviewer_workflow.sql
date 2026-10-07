-- 将协作对照 Workflow 固定到只含合成评测 reviewer 的 Capability/Tool 版本。
update workflow.version
set output_schema = '{"type":"object","required":["riskLevel","reviewRiskLevel","summary","reviewRationale"],"additionalProperties":false,"properties":{"riskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"reviewRiskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"summary":{"type":"string"},"reviewRationale":{"type":"string"}}}'::jsonb,
    steps_json = '[
  {"id":"analyze","type":"RUN_CAPABILITY","nextStepId":"peer-review","capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.2.0","toolName":null,"toolVersion":null,"inputMapping":{"input":"$.input.customerId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},
  {"id":"peer-review","type":"RUN_TOOL","nextStepId":"complete","capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.2.0","toolName":"agent.risk.review.evaluation","toolVersion":"1.0.0","inputMapping":{"customerId":"$.input.customerId","riskSummary":"$.steps.analyze.output.summary"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},
  {"id":"complete","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"riskLevel":"$.steps.analyze.output.riskLevel","reviewRiskLevel":"$.steps.peer-review.output.riskLevel","summary":"$.steps.analyze.output.summary","reviewRationale":"$.steps.peer-review.output.rationale"}}
]'
::jsonb
where workflow_id = '58000000-0000-4000-8000-000000000003' and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '1.0.0' and status = 'DRAFT';

delete from workflow.capability_dependency
where tenant_id = '70000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and workflow_id = '58000000-0000-4000-8000-000000000003' and workflow_version = '1.0.0';

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                            capability_id, capability_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '58000000-0000-4000-8000-000000000003', '1.0.0',
        '54000000-0000-4000-8000-000000000001', '1.2.0');
