-- Workflow 实例保留可信质量运行关联，使运行中的子 Task 继承原用途。
alter table workflow.instance add column quality_run_id uuid;
