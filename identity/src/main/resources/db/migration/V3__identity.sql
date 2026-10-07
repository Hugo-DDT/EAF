create schema if not exists "identity";

create table "identity".subject (
    id uuid primary key,
    type varchar(20) not null check (type in ('HUMAN', 'AGENT', 'SERVICE')),
    display_name varchar(120) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED'))
);

insert into "identity".subject(id, type, display_name, status) values
 ('80000000-0000-4000-8000-000000000001', 'HUMAN', 'Alice', 'ACTIVE'),
 ('80000000-0000-4000-8000-000000000002', 'HUMAN', 'Bob', 'ACTIVE'),
 ('80000000-0000-4000-8000-000000000003', 'HUMAN', 'Carol', 'ACTIVE'),
 ('20000000-0000-4000-8000-000000000001', 'AGENT', 'Risk Analysis Agent', 'ACTIVE');
-- 本文件负责 EAF 的 V3__identity.sql 相关定义。
