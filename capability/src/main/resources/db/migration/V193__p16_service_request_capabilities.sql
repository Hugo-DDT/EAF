insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description)
values ('54000000-0000-4000-8000-000000000014', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-prepare', '只读整理用户明确提交的服务请求交接简报。'),
       ('54000000-0000-4000-8000-000000000015', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'service-request-summary', '只读整理本轮人工处理结果。')
on conflict (tenant_id, workspace_id, name) do nothing;

insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
values ('54000000-0000-4000-8000-000000000014', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '20000000-0000-4000-8000-000000000011', '1.0.0',
        '53000000-0000-4000-8000-000000000011', '1.0.0',
        '21000000-0000-4000-8000-000000000011', '1.0.0', 'p16-service-request-prepare-v1', 'PUBLISHED'),
       ('54000000-0000-4000-8000-000000000015', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        '20000000-0000-4000-8000-000000000012', '1.0.0',
        '53000000-0000-4000-8000-000000000012', '1.0.0',
        '21000000-0000-4000-8000-000000000012', '1.0.0', 'p16-service-request-summary-v1', 'PUBLISHED')
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
values ('54000000-0000-4000-8000-000000000018', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000014', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
       ('54000000-0000-4000-8000-000000000019', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '54000000-0000-4000-8000-000000000015', '1.0.0',
        'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
