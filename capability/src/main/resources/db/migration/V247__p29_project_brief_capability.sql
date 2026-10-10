insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('54000000-0000-4000-8000-000000000023', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'project-brief-prepare', '只读整理用户明确选择的项目证据与待办。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
values ('54000000-0000-4000-8000-000000000023', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '20000000-0000-4000-8000-000000000023', '1.0.0',
        '53000000-0000-4000-8000-000000000023', '1.0.0',
        '21000000-0000-4000-8000-000000000023', '1.0.0', 'p29-project-brief-prepare-v1', 'PUBLISHED')
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
values ('54000000-0000-4000-8000-000000000038', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000023', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
