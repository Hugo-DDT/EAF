alter table evaluation.eval_run add column sample_count integer not null default 1;
alter table evaluation.eval_result add column sample_no integer not null default 1;
alter table evaluation.eval_result drop constraint if exists eval_result_pkey;
alter table evaluation.eval_result add primary key (run_id, case_id, sample_no);
-- 本文件负责 EAF 的 V17__evaluation_samples.sql 相关定义。
