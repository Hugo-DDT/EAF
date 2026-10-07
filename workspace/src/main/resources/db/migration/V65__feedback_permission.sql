-- 反馈写权限独立登记，只授予当前有权发起 Task 的 Workspace 成员。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'feedback:create', 'ACTIVE'
from workspace."grant"
where action = 'task:create' and status = 'ACTIVE'
on conflict (workspace_id, actor_id, action) do nothing;
