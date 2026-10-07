alter table task.task add column active_used_ms bigint not null default 0;
alter table task.task add constraint task_active_time_check check (active_used_ms >= 0);
-- 本文件负责 EAF 的 V35__task_active_time.sql 相关定义。
