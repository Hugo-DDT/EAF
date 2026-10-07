alter table evaluation.eval_run add column agent_id uuid;
alter table evaluation.eval_run add column agent_version varchar(40);
alter table evaluation.eval_run add column prompt_id uuid;
alter table evaluation.eval_run add column prompt_version varchar(40);
alter table evaluation.eval_run add column model_profile_id uuid;
alter table evaluation.eval_run add column model_calls integer not null default 0;
alter table evaluation.eval_run add column known_usage_samples integer not null default 0;
alter table evaluation.eval_run add column elapsed_ms bigint not null default 0;
alter table evaluation.eval_result add column task_id uuid;
alter table evaluation.eval_result add column input_tokens integer;
alter table evaluation.eval_result add column output_tokens integer;
alter table evaluation.eval_result add column elapsed_ms bigint;
-- 本文件负责 EAF 的 V16__evaluation_runtime_evidence.sql 相关定义。
