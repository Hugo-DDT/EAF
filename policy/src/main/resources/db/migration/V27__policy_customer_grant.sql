create schema if not exists policy;

create table policy.customer_grant (
    tenant_id uuid not null,
    workspace_id uuid not null,
    actor_id uuid not null,
    customer_id varchar(160) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'REVOKED')),
    primary key (tenant_id, workspace_id, actor_id, customer_id)
);

insert into policy.customer_grant(tenant_id, workspace_id, actor_id, customer_id, status) values
 ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', 'customer-001', 'ACTIVE'),
 ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', '80000000-0000-4000-8000-000000000002', 'customer-002', 'ACTIVE'),
 ('70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', '80000000-0000-4000-8000-000000000003', 'customer-003', 'ACTIVE');
-- 本文件负责 EAF 的 V27__policy_customer_grant.sql 相关定义。
