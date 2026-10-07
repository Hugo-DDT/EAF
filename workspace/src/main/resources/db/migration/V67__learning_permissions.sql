-- Learning 动作从现有 Task 权限派生；审核权限单独授予具备审计读取权的成员。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'learning:propose', 'ACTIVE'
from workspace."grant"
where action = 'feedback:create' and status = 'ACTIVE'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'learning:read', 'ACTIVE'
from workspace."grant"
where action = 'task:read' and status = 'ACTIVE'
on conflict (workspace_id, actor_id, action) do nothing;

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'learning:review', 'ACTIVE'
from workspace."grant"
where action = 'audit:read' and status = 'ACTIVE'
on conflict (workspace_id, actor_id, action) do nothing;
