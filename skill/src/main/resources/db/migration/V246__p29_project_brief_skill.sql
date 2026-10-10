insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('53000000-0000-4000-8000-000000000023', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'project-brief-prepare', '根据已选证据整理可追溯的项目简报草稿。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
values ('53000000-0000-4000-8000-000000000023', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '{"type":"object","required":["evidenceBundleJson","evidenceBundleHash"],"additionalProperties":false,"properties":{"evidenceBundleJson":{"type":"string","minLength":2,"maxLength":24000},"evidenceBundleHash":{"type":"string","minLength":64,"maxLength":64}}}',
        '{"type":"object","required":["overview","attentionItems","citations"],"additionalProperties":false,"properties":{"overview":{"type":"string","maxLength":1500},"attentionItems":{"type":"array","maxItems":8,"items":{"type":"object","required":["text","evidenceIds"],"additionalProperties":false,"properties":{"text":{"type":"string","minLength":1,"maxLength":500},"evidenceIds":{"type":"array","maxItems":10,"items":{"type":"string","minLength":1,"maxLength":20}}}}},"citations":{"type":"array","maxItems":40,"items":{"type":"object","required":["evidenceId","reason"],"additionalProperties":false,"properties":{"evidenceId":{"type":"string","minLength":1,"maxLength":20},"reason":{"type":"string","maxLength":240}}}}}}',
        '21000000-0000-4000-8000-000000000023', '1.0.0', 'p29-project-brief-prepare-v1', 'PUBLISHED')
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
values ('53000000-0000-4000-8000-000000000038', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '53000000-0000-4000-8000-000000000023', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
