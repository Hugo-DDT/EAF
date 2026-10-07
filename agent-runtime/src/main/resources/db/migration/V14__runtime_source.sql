alter table agent_runtime.run add column source varchar(30) not null default 'USER';
alter table agent_runtime.run add constraint run_source_check check (source in ('USER', 'EVALUATION'));
-- 本文件负责 EAF 的 V14__runtime_source.sql 相关定义。
