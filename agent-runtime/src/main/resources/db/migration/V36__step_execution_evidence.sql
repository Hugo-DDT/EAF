alter table agent_runtime.step add column call_no integer;
alter table agent_runtime.step add column execution_id uuid;
alter table agent_runtime.step add constraint step_call_no_check check (call_no is null or call_no > 0);
-- 本文件负责 EAF 的 V36__step_execution_evidence.sql 相关定义。
