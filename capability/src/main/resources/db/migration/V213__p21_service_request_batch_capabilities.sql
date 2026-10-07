insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('54000000-0000-4000-8000-000000000016', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-batch-knowledge', 'P21 固定正式知识只读查证与建议能力。'),
       ('54000000-0000-4000-8000-000000000017', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-batch-experience', 'P21 固定显式 TEAM 经验只读整理能力。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
values ('54000000-0000-4000-8000-000000000016', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0', '20000000-0000-4000-8000-000000000018', '1.0.0',
        '53000000-0000-4000-8000-000000000018', '1.0.0', '21000000-0000-4000-8000-000000000018', '1.0.0',
        'p21-service-request-batch-knowledge-v1', 'PUBLISHED'),
       ('54000000-0000-4000-8000-000000000017', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0', '20000000-0000-4000-8000-000000000019', '1.0.0',
        '53000000-0000-4000-8000-000000000019', '1.0.0', '21000000-0000-4000-8000-000000000019', '1.0.0',
        'p21-service-request-batch-experience-v1', 'PUBLISHED')
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
values ('54000000-0000-4000-8000-000000000030', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000016', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
       ('54000000-0000-4000-8000-000000000031', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000017', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
