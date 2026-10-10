insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
       '80000000-0000-4000-8000-000000000001', action, 'ACTIVE'
from unnest(array['oa:todo:read', 'service-request:status:read', 'service-request:result:sync']) action
on conflict (workspace_id, actor_id, action) do nothing;
