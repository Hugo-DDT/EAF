-- Workflow 实例在截止或撤回后先停止派生步骤，再等待已有子 Task 的安全收敛。
alter table workflow.instance
    add column deadline_at timestamptz,
    add column waiting_reason varchar(80),
    add column business_effect_status varchar(24) not null default 'NONE',
    add column cancel_target_status varchar(20);

update workflow.instance set deadline_at = created_at + interval '24 hours';

alter table workflow.instance
    alter column deadline_at set not null,
    drop constraint instance_status_check,
    add constraint workflow_instance_status_check check (status in (
        'QUEUED', 'RUNNING', 'WAITING_CHILD', 'CANCELLING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'
    )),
    add constraint workflow_instance_waiting_reason_check check (waiting_reason is null or waiting_reason ~ '^[A-Z0-9_]{1,80}$'),
    add constraint workflow_instance_effect_status_check check (business_effect_status in (
        'NONE', 'IN_PROGRESS', 'UNKNOWN', 'SUCCEEDED', 'FAILED', 'VERIFICATION_FAILED'
    )),
    add constraint workflow_instance_cancel_target_check check (
        cancel_target_status is null or cancel_target_status in ('FAILED', 'CANCELLED', 'TIMED_OUT')
    );

drop index workflow.workflow_instance_dispatch_idx;
create index workflow_instance_dispatch_idx
    on workflow.instance(status, next_poll_at, lease_until, created_at)
    where status in ('QUEUED', 'RUNNING', 'WAITING_CHILD', 'CANCELLING');
