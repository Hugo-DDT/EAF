alter table usage.model_usage add column estimated_cost numeric(18, 8);
alter table usage.model_usage add column cost_currency varchar(3);
alter table usage.model_usage add column cost_status varchar(30) not null default 'UNKNOWN_PRICE';
alter table usage.model_usage add column cost_source varchar(120);
alter table usage.model_usage add constraint model_usage_cost_status_check check (cost_status in ('KNOWN', 'UNKNOWN_PRICE'));
-- 本文件负责 EAF 的 V22__usage_cost_status.sql 相关定义。
