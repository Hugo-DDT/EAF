insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-00000000000d', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'experience-draft-assistant', '只把本人反馈整理为可编辑经验草稿。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-00000000000b', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.1.0',
        '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":12000}}}',
        '{"type":"object","required":["answer","answerStatus","citations","missingInformation"],"additionalProperties":false,"properties":{"answer":{"type":"string","maxLength":2000},"answerStatus":{"type":"string","enum":["ANSWERED","INSUFFICIENT","CONFLICTING"]},"citations":{"type":"array","items":{"type":"string"}},"missingInformation":{"type":"array","items":{"type":"string"}},"clarificationQuestion":{"type":"string","maxLength":500}}}',
        '21000000-0000-4000-8000-00000000000b', '1.1.0', 'p11-conversational-qa-v2', 'PUBLISHED'),
       ('53000000-0000-4000-8000-00000000000c', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.1.0',
        '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":12000}}}',
        '{"type":"object","required":["riskLevel","summary","reasons","uncertainties","followupDraft"],"additionalProperties":false,"properties":{"riskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"summary":{"type":"string","maxLength":2000},"reasons":{"type":"array","items":{"type":"string"}},"uncertainties":{"type":"array","items":{"type":"string"}},"followupDraft":{"type":"object"},"clarificationQuestion":{"type":"string","maxLength":500},"briefSuggestion":{"type":"object"}}}',
        '21000000-0000-4000-8000-00000000000c', '1.1.0', 'p11-conversational-customer-v2', 'PUBLISHED'),
       ('53000000-0000-4000-8000-00000000000d', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":12000}}}',
        '{"type":"object","required":["title","content"],"additionalProperties":false,"properties":{"title":{"type":"string","minLength":1,"maxLength":80},"content":{"type":"string","minLength":1,"maxLength":800}}}',
        '21000000-0000-4000-8000-00000000000d', '1.0.0', 'p11-experience-draft-v1', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-00000000000f', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-00000000000b', '1.1.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
       ('53000000-0000-4000-8000-000000000010', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-00000000000c', '1.1.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
       ('53000000-0000-4000-8000-000000000011', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-00000000000d', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
