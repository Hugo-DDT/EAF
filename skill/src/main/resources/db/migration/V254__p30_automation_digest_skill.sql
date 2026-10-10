insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-000000000024', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'my-p16-work-digest', '只读整理本人明确订阅的 P16 工作项当前快照。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-000000000024', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["snapshotJson","snapshotHash"],"additionalProperties":false,"properties":{"snapshotJson":{"type":"string","minLength":2,"maxLength":7000},"snapshotHash":{"type":"string","minLength":64,"maxLength":64}}}',
        '{"type":"object","required":["overview","attentionItems"],"additionalProperties":false,"properties":{"overview":{"type":"string","maxLength":1000},"attentionItems":{"type":"array","maxItems":5,"items":{"type":"object","required":["text","evidenceIds"],"additionalProperties":false,"properties":{"text":{"type":"string","minLength":1,"maxLength":300},"evidenceIds":{"type":"array","maxItems":20,"items":{"type":"string","minLength":1,"maxLength":20}}}}}}}',
        '21000000-0000-4000-8000-000000000024', '1.0.0', 'p30-my-p16-work-digest-v1', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000039', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000024', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
