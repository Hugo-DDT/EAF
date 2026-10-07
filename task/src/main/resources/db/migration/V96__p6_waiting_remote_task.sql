-- P6 只增加远端等待这一持久 Task 状态；远端请求细节由 Execution 自有表保存。
alter table task.task drop constraint if exists task_task_status_check;
alter table task.task add constraint task_task_status_check
    check (status in ('QUEUED','RUNNING','WAITING_APPROVAL','WAITING_VERIFICATION','WAITING_REMOTE',
                      'SUCCEEDED','FAILED','TIMED_OUT','CANCELLED'));
comment on column task.task.status is 'Task 生命周期；WAITING_REMOTE 表示远端 A2A Task 已受理且本地 worker 已释放。';
