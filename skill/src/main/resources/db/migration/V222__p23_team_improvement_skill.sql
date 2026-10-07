insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-000000000020', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'team-experience-improvement', '约束 P23 单次 TEAM 经验修订候选的输入与输出。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-000000000020', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["card","sharedCorrection","sourceRefs"],"additionalProperties":false,"properties":{"card":{"type":"object"},"sharedCorrection":{"type":"string","minLength":1,"maxLength":4000},"sourceRefs":{"type":"array","minItems":1,"maxItems":5}}}',
        '{"type":"object","required":["title","appliesWhen","content"],"additionalProperties":false,"properties":{"title":{"type":"string","minLength":1,"maxLength":80},"appliesWhen":{"type":"string","minLength":1,"maxLength":300},"content":{"type":"string","minLength":1,"maxLength":800}}}',
        '21000000-0000-4000-8000-000000000020', '1.0.0', 'p23-team-experience-improvement-v1', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000032', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000020', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
