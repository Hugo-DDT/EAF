-- 风险分析 Agent 的合成 CRM 读取边界与 Alice 的授权客户一致。
insert into policy.customer_grant(tenant_id, workspace_id, actor_id, customer_id, status)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001', 'customer-001', 'ACTIVE')
on conflict (tenant_id, workspace_id, actor_id, customer_id) do update set status = 'ACTIVE';
