-- 仅为已登记的 Risk Agent 授予固定只读 reviewer 动作及解析其 Tool 元数据所需权限。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001', 'agent:risk-review', 'ACTIVE'),
       ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001', 'tool:read', 'ACTIVE')
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE';
