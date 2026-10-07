insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'service-request:submit', 'ACTIVE'
from workspace."grant"
where workspace_id = '10000000-0000-4000-8000-000000000001'
  and actor_id = '80000000-0000-4000-8000-000000000001'
  and action = 'task:create' and status = 'ACTIVE'
on conflict (workspace_id, actor_id, action) do nothing;
