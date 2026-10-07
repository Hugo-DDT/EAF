create schema if not exists model;

create table model.profile (
    id uuid primary key,
    provider varchar(80) not null,
    model_name varchar(120) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    input_output_budget integer not null,
    created_at timestamptz not null default now()
);

insert into model.profile(id, provider, model_name, status, input_output_budget) values
 ('22000000-0000-4000-8000-000000000001', 'deterministic-or-dashscope', 'p1-risk-analysis', 'ACTIVE', 8000);
-- 本文件负责 EAF 的 V7__model.sql 相关定义。
