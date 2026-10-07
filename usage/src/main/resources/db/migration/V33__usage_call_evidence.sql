alter table usage.model_usage add column call_no integer not null default 1;
alter table usage.model_usage drop constraint if exists model_usage_task_id_run_id_key;
alter table usage.model_usage add constraint model_usage_task_run_call_key unique (task_id, run_id, call_no);
-- 本文件负责 EAF 的 V33__usage_call_evidence.sql 相关定义。
