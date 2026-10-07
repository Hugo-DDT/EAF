-- P16 的本地协作仅预置 Alice 发起/指派、Bob 处理；其他成员须经既有授权流程单独授予。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select g.tenant_id, g.workspace_id, g.actor_id, a.action, 'ACTIVE'
from workspace."grant" g
cross join unnest(array['work-item:read', 'work-item:assign', 'work-item:complete', 'workflow:write']) a(action)
where g.workspace_id = '10000000-0000-4000-8000-000000000001'
  and g.actor_id = '80000000-0000-4000-8000-000000000001'
  and g.action = 'task:create' and g.status = 'ACTIVE'
on conflict (workspace_id, actor_id, action) do nothing;

-- Bob 只有处理权限，不要求也不派生普通 Task 创建/读取权限。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '80000000-0000-4000-8000-000000000002', 'work-item:read', 'ACTIVE'),
       ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '80000000-0000-4000-8000-000000000002', 'work-item:complete', 'ACTIVE')
on conflict (workspace_id, actor_id, action) do nothing;
