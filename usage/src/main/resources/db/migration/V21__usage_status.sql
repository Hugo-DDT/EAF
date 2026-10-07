alter table usage.model_usage add column usage_status varchar(20) not null default 'UNKNOWN';
alter table usage.model_usage add constraint model_usage_usage_status_check check (usage_status in ('KNOWN', 'UNKNOWN'));
-- 本文件负责 EAF 的 V21__usage_status.sql 相关定义。
