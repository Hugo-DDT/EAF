create schema if not exists organization;

create table organization.tenant (
    id uuid primary key,
    name varchar(120) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'SUSPENDED'))
);

create table organization.member (
    tenant_id uuid not null,
    subject_id uuid not null,
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    primary key (tenant_id, subject_id)
);

insert into organization.tenant(id, name, status) values
 ('70000000-0000-4000-8000-000000000001', 'Tenant A', 'ACTIVE'),
 ('70000000-0000-4000-8000-000000000002', 'Tenant B', 'ACTIVE');

insert into organization.member(tenant_id, subject_id, status) values
 ('70000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', 'ACTIVE'),
 ('70000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000002', 'ACTIVE'),
 ('70000000-0000-4000-8000-000000000002', '80000000-0000-4000-8000-000000000003', 'ACTIVE');
-- 本文件负责 EAF 的 V2__organization.sql 相关定义。
