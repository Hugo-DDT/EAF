-- P17 本地处理人需能读取同 Workspace 的已发布团队经验。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '80000000-0000-4000-8000-000000000002', 'memory:read', 'ACTIVE')
on conflict (workspace_id, actor_id, action) do nothing;
