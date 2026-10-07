insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-00000000000b', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'conversational-knowledge-answer', '连续知识问答；历史只用于理解对话。'),
       ('53000000-0000-4000-8000-00000000000c', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'conversational-customer-assistant', '以用户确认简报为基线的客户分析与跟进草稿。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-00000000000b', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":12000}}}',
        '{"type":"object","required":["answer","answerStatus","citations","missingInformation"],"additionalProperties":false,"properties":{"answer":{"type":"string","maxLength":2000},"answerStatus":{"type":"string","enum":["ANSWERED","INSUFFICIENT","CONFLICTING"]},"citations":{"type":"array","items":{"type":"string"}},"missingInformation":{"type":"array","items":{"type":"string"}},"clarificationQuestion":{"type":"string","maxLength":500}}}',
        '21000000-0000-4000-8000-00000000000b', '1.0.0', 'p10-conversational-qa-v1', 'PUBLISHED'),
       ('53000000-0000-4000-8000-00000000000c', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":12000}}}',
        '{"type":"object","required":["riskLevel","summary","reasons","uncertainties","followupDraft"],"additionalProperties":false,"properties":{"riskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"summary":{"type":"string","maxLength":2000},"reasons":{"type":"array","items":{"type":"string"}},"uncertainties":{"type":"array","items":{"type":"string"}},"followupDraft":{"type":"object"},"clarificationQuestion":{"type":"string","maxLength":500},"briefSuggestion":{"type":"object"}}}',
        '21000000-0000-4000-8000-00000000000c', '1.0.0', 'p10-conversational-customer-v1', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-00000000000d', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-00000000000b', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
       ('53000000-0000-4000-8000-00000000000e', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-00000000000c', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
