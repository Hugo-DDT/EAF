insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select s.tenant_id, s.workspace_id, s.actor_id, a.action, 'ACTIVE'
from (values
    ('70000000-0000-4000-8000-000000000001'::uuid, '10000000-0000-4000-8000-000000000001'::uuid, '80000000-0000-4000-8000-000000000001'::uuid),
    ('70000000-0000-4000-8000-000000000001'::uuid, '10000000-0000-4000-8000-000000000002'::uuid, '80000000-0000-4000-8000-000000000002'::uuid),
    ('70000000-0000-4000-8000-000000000002'::uuid, '10000000-0000-4000-8000-000000000003'::uuid, '80000000-0000-4000-8000-000000000003'::uuid)
) s(tenant_id, workspace_id, actor_id)
cross join unnest(array['knowledge:read', 'knowledge:write']) a(action)
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE', tenant_id = excluded.tenant_id;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', '80000000-0000-4000-8000-000000000001', 'knowledge:read', 'ACTIVE')
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE', tenant_id = excluded.tenant_id;

