-- V50/V51 兼容 PostgreSQL 按表名自动生成的旧约束名称，确保等待状态确实可落库。
alter table execution.execution drop constraint if exists execution_status_check;
alter table task.task drop constraint if exists task_status_check;
