-- 受托 Agent 与其归属租户绑定；Identity 再核对 Owner、Agent 的租户一致性。
insert into organization.member(tenant_id, subject_id, status)
values ('70000000-0000-4000-8000-000000000001', '20000000-0000-4000-8000-000000000001', 'ACTIVE')
on conflict (tenant_id, subject_id) do update set status = 'ACTIVE';
