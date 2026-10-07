alter table task.task drop constraint if exists task_task_status_check;
alter table task.task add constraint task_task_status_check
    check (status in ('QUEUED','RUNNING','WAITING_APPROVAL','WAITING_VERIFICATION','WAITING_REMOTE',
                      'CANCELLING_REMOTE','SUCCEEDED','FAILED','TIMED_OUT','CANCELLED'));

alter table task.task drop constraint if exists task_external_effect_status_check;
alter table task.task drop constraint if exists task_task_external_effect_status_check;
alter table task.task add constraint task_remote_external_effect_status_check
    check (external_effect_status in ('NONE','IN_PROGRESS','UNKNOWN','SUCCEEDED','FAILED','VERIFICATION_FAILED',
                                      'REMOTE_PENDING','REMOTE_CANCEL_REQUESTED','REMOTE_CANCEL_UNKNOWN','REMOTE_CANCELLED'));
alter table task.task drop constraint if exists task_external_effect_binding;
alter table task.task add constraint task_external_effect_binding check (
    (external_effect_operation_id is null and external_effect_status in ('NONE','SUCCEEDED','FAILED','VERIFICATION_FAILED','REMOTE_CANCELLED'))
    or (external_effect_operation_id is not null and external_effect_status in ('IN_PROGRESS','UNKNOWN','REMOTE_PENDING','REMOTE_CANCEL_REQUESTED','REMOTE_CANCEL_UNKNOWN'))
);
comment on column task.task.status is 'Task 生命周期；CANCELLING_REMOTE 表示已请求远端取消，但尚未取得 peer 确认。';
