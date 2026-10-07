insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
values ('54000000-0000-4000-8000-000000000012', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.1.0',
        '20000000-0000-4000-8000-000000000010', '1.1.0',
        '53000000-0000-4000-8000-00000000000f', '1.0.0',
        '21000000-0000-4000-8000-00000000000f', '1.0.0', 'p24-service-request-plan-v1', 'PUBLISHED')
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
values ('54000000-0000-4000-8000-000000000033', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000012', '1.1.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
