-- Memory 的动作与范围授权归 Workspace 持有；TEAM 范围仅授予既有知识发布角色。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'memory:read', 'ACTIVE'
from workspace."grant" where action = 'knowledge:read'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'memory:write', 'ACTIVE'
from workspace."grant" where action = 'knowledge:write'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'memory:publish', 'ACTIVE'
from workspace."grant" where action = 'knowledge:publish'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'memory:scope:team', 'ACTIVE'
from workspace."grant" where action = 'knowledge:publish'
on conflict (workspace_id, actor_id, action) do nothing;
