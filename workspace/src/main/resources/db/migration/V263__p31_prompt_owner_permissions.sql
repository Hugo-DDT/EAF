insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'prompt:read', 'ACTIVE'
from workspace."grant" where action = 'agent:read'
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE', tenant_id = excluded.tenant_id;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'prompt:manage', 'ACTIVE'
from workspace."grant" where action = 'knowledge:write'
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE', tenant_id = excluded.tenant_id;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'prompt:publish', 'ACTIVE'
from workspace."grant" where action = 'knowledge:publish'
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE', tenant_id = excluded.tenant_id;
