create schema if not exists connector;

create table connector.instance (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    provider varchar(80) not null,
    base_url varchar(300) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED', 'REVOKED')),
    unique (tenant_id, workspace_id, provider)
);

insert into connector.instance(id, tenant_id, workspace_id, provider, base_url, status) values
 ('23000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', 'TEST_CRM', 'http://127.0.0.1:19090', 'ACTIVE'),
 ('23000000-0000-4000-8000-000000000002', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', 'TEST_CRM', 'http://127.0.0.1:19090', 'ACTIVE'),
 ('23000000-0000-4000-8000-000000000003', '70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', 'TEST_CRM', 'http://127.0.0.1:19090', 'ACTIVE');
-- 本文件负责 EAF 的 V28__connector.sql 相关定义。
