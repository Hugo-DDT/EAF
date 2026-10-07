-- P16 人工步骤使用同一 Workflow 状态机与持久工作项；等待期间不占用 worker lease。
alter table workflow.instance
    drop constraint workflow_instance_status_check,
    add constraint workflow_instance_status_check check (status in (
        'QUEUED', 'RUNNING', 'WAITING_CHILD', 'WAITING_HUMAN', 'CANCELLING',
        'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'
    ));

drop index workflow.workflow_instance_dispatch_idx;
create index workflow_instance_dispatch_idx
    on workflow.instance(status, next_poll_at, lease_until, created_at)
    where status in ('QUEUED', 'RUNNING', 'WAITING_CHILD', 'WAITING_HUMAN', 'CANCELLING');

alter table workflow.step
    drop constraint if exists step_step_type_check,
    drop constraint if exists step_status_check,
    drop constraint if exists step_check,
    add constraint workflow_step_type_check check (step_type in (
        'RUN_CAPABILITY', 'BRANCH', 'RUN_TOOL', 'HUMAN_TASK', 'COMPLETE'
    )),
    add constraint workflow_step_status_check check (status in (
        'INTENT', 'WAITING_CHILD', 'WAITING_HUMAN', 'SUCCEEDED'
    )),
    add constraint workflow_step_state_check check (
        (step_type in ('RUN_CAPABILITY', 'RUN_TOOL') and status in ('INTENT', 'WAITING_CHILD', 'SUCCEEDED'))
        or (step_type = 'HUMAN_TASK' and status in ('INTENT', 'WAITING_HUMAN', 'SUCCEEDED'))
        or (step_type in ('BRANCH', 'COMPLETE') and status in ('INTENT', 'SUCCEEDED'))
    );

create table workflow.human_work_item (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    instance_id uuid not null,
    step_id varchar(64) not null,
    assignee_id uuid not null,
    status varchar(20) not null check (status in ('OPEN', 'COMPLETED', 'CANCELLED', 'TIMED_OUT', 'FAILED')),
    row_version bigint not null default 1 check (row_version > 0),
    result_json jsonb check (result_json is null or jsonb_typeof(result_json) = 'object'),
    completed_by uuid,
    completed_at timestamptz,
    completion_key_hash char(64) check (completion_key_hash is null or completion_key_hash ~ '^[0-9a-f]{64}$'),
    completion_request_hash char(64) check (completion_request_hash is null or completion_request_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (instance_id, step_id),
    foreign key (instance_id, tenant_id, workspace_id)
        references workflow.instance(id, tenant_id, workspace_id),
    foreign key (instance_id, step_id) references workflow.step(instance_id, step_id),
    check ((status = 'COMPLETED' and result_json is not null and completed_by is not null and completed_at is not null
                and completion_key_hash is not null and completion_request_hash is not null)
        or (status <> 'COMPLETED' and result_json is null and completed_by is null and completed_at is null
                and completion_key_hash is null and completion_request_hash is null))
);

create index human_work_item_assignee_idx
    on workflow.human_work_item(tenant_id, workspace_id, assignee_id, created_at desc, id desc);
