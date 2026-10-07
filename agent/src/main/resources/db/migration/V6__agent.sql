create schema if not exists agent;

create table agent.version (
    id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    name varchar(120) not null,
    asset_version varchar(40) not null,
    prompt_id uuid not null,
    prompt_version varchar(40) not null,
    model_profile_id uuid not null,
    status varchar(20) not null check (status in ('DRAFT', 'IN_REVIEW', 'PUBLISHED', 'DEPRECATED', 'REVOKED')),
    created_at timestamptz not null default now(),
    primary key (id, workspace_id, asset_version),
    unique (id, tenant_id, workspace_id, asset_version)
);

insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, model_profile_id, status) values
 ('20000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', 'customer-risk-analysis', '1.0.0', '21000000-0000-4000-8000-000000000001', '1.0.0', '22000000-0000-4000-8000-000000000001', 'PUBLISHED'),
 ('20000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', 'customer-risk-analysis', '1.0.0', '21000000-0000-4000-8000-000000000001', '1.0.0', '22000000-0000-4000-8000-000000000001', 'PUBLISHED'),
 ('20000000-0000-4000-8000-000000000002', '70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', 'customer-risk-analysis', '1.0.0', '21000000-0000-4000-8000-000000000002', '1.0.0', '22000000-0000-4000-8000-000000000001', 'PUBLISHED');
-- 本文件负责 EAF 的 V6__agent.sql 相关定义。
