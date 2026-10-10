insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select g.tenant_id, g.workspace_id, g.actor_id, 'knowledge:source:manage', 'ACTIVE'
from workspace."grant" g
join "identity".subject s on s.id = g.actor_id and s.type = 'HUMAN' and s.status = 'ACTIVE'
where g.action = 'knowledge:write' and g.status = 'ACTIVE'
on conflict (workspace_id, actor_id, action) do nothing;
