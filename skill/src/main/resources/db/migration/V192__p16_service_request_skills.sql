insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-000000000011', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-prepare', '基于显式交接简报整理处理建议。'),
       ('53000000-0000-4000-8000-000000000012', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-summary', '基于人工提交的处理结果整理本轮摘要。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-000000000011', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["sharedBrief"],"additionalProperties":false,"properties":{"sharedBrief":{"type":"string","minLength":1,"maxLength":2000}}}',
        '{"type":"object","required":["handlingAdvice","cautions"],"additionalProperties":false,"properties":{"handlingAdvice":{"type":"string","minLength":1,"maxLength":2000},"cautions":{"type":"string","maxLength":1000}}}',
        '21000000-0000-4000-8000-000000000011', '1.0.0', 'p16-service-request-prepare-v1', 'PUBLISHED'),
       ('53000000-0000-4000-8000-000000000012', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["sharedBrief","outcome","summary","nextAction"],"additionalProperties":false,"properties":{"sharedBrief":{"type":"string","minLength":1,"maxLength":2000},"outcome":{"type":"string","enum":["COMPLETED","BLOCKED","NEEDS_FOLLOWUP"]},"summary":{"type":"string","minLength":1,"maxLength":2000},"nextAction":{"type":"string","maxLength":1000}}}',
        '{"type":"object","required":["resultSummary","remainingWork"],"additionalProperties":false,"properties":{"resultSummary":{"type":"string","minLength":1,"maxLength":2000},"remainingWork":{"type":"string","maxLength":1000}}}',
        '21000000-0000-4000-8000-000000000012', '1.0.0', 'p16-service-request-summary-v1', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000016', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000011', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
       ('53000000-0000-4000-8000-000000000017', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000012', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
