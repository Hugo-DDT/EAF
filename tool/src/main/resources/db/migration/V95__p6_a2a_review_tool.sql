-- P6 只登记一个只读风险复核工具；Tool 声明不授予远端建议任何本地业务写入权。
insert into tool.version(tenant_id, workspace_id, name, asset_version, description, input_schema,
                        output_schema, permission_action, effect, binding_ref, status)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        'agent.risk.review', '1.0.0', 'READ_ONLY_ADVISORY：将最少客户风险摘要发送给固定 A2A reviewer，仅返回只读建议。',
        '{"type":"object","required":["customerId","riskSummary"],"additionalProperties":false,"properties":{"customerId":{"type":"string","minLength":1,"maxLength":160},"riskSummary":{"type":"string","minLength":1,"maxLength":2000}}}',
        '{"type":"object","required":["riskLevel","rationale","citations"],"additionalProperties":false,"properties":{"riskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"rationale":{"type":"string","minLength":1,"maxLength":1000},"citations":{"type":"array","maxItems":8,"items":{"type":"string","minLength":1,"maxLength":160}}}}',
        'agent:risk-review', 'READ', 'A2A_REVIEW_PEER:risk-review@1.0.0', 'PUBLISHED')
on conflict (tenant_id, workspace_id, name, asset_version) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '80000000-0000-4000-8000-000000000001', 'agent:risk-review', 'ACTIVE')
on conflict (workspace_id, actor_id, action) do nothing;

insert into agent.tool_binding(tenant_id, workspace_id, agent_id, agent_version, tool_name, tool_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001', '2.0.0', 'agent.risk.review', '1.0.0')
on conflict do nothing;
