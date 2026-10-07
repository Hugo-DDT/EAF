insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-000000000018', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-batch-knowledge', '约束 P21 正式知识只读分析的输入与结构化结果。'),
       ('53000000-0000-4000-8000-000000000019', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-batch-experience', '约束 P21 显式 TEAM 经验只读整理的输入与结构化结果。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-000000000018', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["requestText"],"additionalProperties":false,"properties":{"requestText":{"type":"string","minLength":1,"maxLength":8000}}}',
        '{"type":"object","required":["category","title","knowledgeAdvice","outcome","questions","citations"],"additionalProperties":false,"properties":{"category":{"type":"string"},"title":{"type":"string","maxLength":120},"knowledgeAdvice":{"type":"string","maxLength":2000},"outcome":{"type":"string"},"questions":{"type":"array"},"citations":{"type":"array"}}}',
        '21000000-0000-4000-8000-000000000018', '1.0.0', 'p21-service-request-batch-knowledge-v1', 'PUBLISHED'),
       ('53000000-0000-4000-8000-000000000019', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["requestText","scenarioKey","experienceRefsJson"],"additionalProperties":false,"properties":{"requestText":{"type":"string","minLength":1,"maxLength":8000},"scenarioKey":{"type":"string","minLength":0,"maxLength":64},"experienceRefsJson":{"type":"string","minLength":2,"maxLength":1000}}}',
        '{"type":"object","required":["outcome","experienceAdvice","cautions","usedExperienceRefs"],"additionalProperties":false,"properties":{"outcome":{"type":"string"},"experienceAdvice":{"type":"string","maxLength":2000},"cautions":{"type":"string","maxLength":1000},"usedExperienceRefs":{"type":"array"}}}',
        '21000000-0000-4000-8000-000000000019', '1.0.0', 'p21-service-request-batch-experience-v1', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000030', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000018', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
       ('53000000-0000-4000-8000-000000000031', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000019', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
