-- 为有限委托授予明确管理权，并仅给风险分析 Agent 所需的 Workspace 只读/任务创建权限。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select tenant_id, workspace_id, actor_id, 'identity:delegation:manage', 'ACTIVE'
from workspace."grant"
where action = 'task:create' and status = 'ACTIVE'
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE';

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
       '20000000-0000-4000-8000-000000000001', action, 'ACTIVE'
from unnest(array['agent:read','task:create','task:read','capability:read','skill:read','prompt:read',
                  'context:read','knowledge:read','memory:read','crm:customer:read']) action
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE';
