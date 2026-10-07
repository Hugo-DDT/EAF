alter table usage.model_usage add column source varchar(30) not null default 'USER';
alter table usage.model_usage add constraint model_usage_source_check check (source in ('USER', 'EVALUATION'));
-- 本文件负责 EAF 的 V15__usage_source.sql 相关定义。
