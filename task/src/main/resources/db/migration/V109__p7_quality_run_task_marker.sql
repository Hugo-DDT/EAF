-- 质量运行标记由服务端写入 Task，用于计量归属、跨流程传播与 Learning 拒绝规则。
alter table task.task add column quality_run_id uuid;
