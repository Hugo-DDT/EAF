create schema if not exists workspace;

create table workspace.workspace (
    id uuid primary key,
    tenant_id uuid not null,
    name varchar(120) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    unique (tenant_id, id)
);

create table workspace."grant" (
    tenant_id uuid not null,
    workspace_id uuid not null,
    actor_id uuid not null,
    action varchar(80) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'REVOKED')),
    primary key (workspace_id, actor_id, action)
);

insert into workspace.workspace(id, tenant_id, name, status) values
 ('10000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001', 'Sales A', 'ACTIVE'),
 ('10000000-0000-4000-8000-000000000002', '70000000-0000-4000-8000-000000000001', 'Support A', 'ACTIVE'),
 ('10000000-0000-4000-8000-000000000003', '70000000-0000-4000-8000-000000000002', 'Sales B', 'ACTIVE'),
 ('10000000-0000-4000-8000-000000000004', '70000000-0000-4000-8000-000000000002', 'Support B', 'ACTIVE');

insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', action, 'ACTIVE'
from unnest(array['agent:read','task:create','task:read','task:cancel','audit:read','evaluation:run']) action;
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', '80000000-0000-4000-8000-000000000001', action, 'ACTIVE'
from unnest(array['agent:read','task:read','audit:read']) action;
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', '80000000-0000-4000-8000-000000000002', action, 'ACTIVE'
from unnest(array['agent:read','task:create','task:read','task:cancel','audit:read']) action;
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select '70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', '80000000-0000-4000-8000-000000000003', action, 'ACTIVE'
from unnest(array['agent:read','task:create','task:read','task:cancel','audit:read','evaluation:run']) action;
-- 本文件负责 EAF 的 V4__workspace.sql 相关定义。
