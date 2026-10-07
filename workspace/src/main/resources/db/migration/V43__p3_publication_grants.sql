insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
values
    ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', 'knowledge:publish', 'ACTIVE'),
    ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', '80000000-0000-4000-8000-000000000002', 'knowledge:publish', 'ACTIVE'),
    ('70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', '80000000-0000-4000-8000-000000000003', 'knowledge:publish', 'ACTIVE')
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE', tenant_id = excluded.tenant_id;

