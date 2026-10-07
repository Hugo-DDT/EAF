insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-000000000011', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.1.0',
        '{"type":"object","required":["sharedBrief","scenarioKey","teamExperienceRefsJson"],"additionalProperties":false,"properties":{"sharedBrief":{"type":"string","minLength":1,"maxLength":2000},"scenarioKey":{"type":"string","minLength":1,"maxLength":64},"teamExperienceRefsJson":{"type":"string","minLength":2,"maxLength":1000}}}',
        '{"type":"object","required":["handlingAdvice","cautions","teamExperienceUsage"],"additionalProperties":false,"properties":{"handlingAdvice":{"type":"string","minLength":1,"maxLength":2000},"cautions":{"type":"string","maxLength":1000},"teamExperienceUsage":{"type":"object"}}}',
        '21000000-0000-4000-8000-000000000011', '1.1.0', 'p17-service-request-prepare-v2', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000018', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000011', '1.1.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
