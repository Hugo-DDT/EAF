-- Task 域以稳定分发键封住 Workflow 创建与取消竞态，并记录外部写入是否仍需核验。
alter table task.task
    add column external_effect_operation_id uuid,
    add column external_effect_status varchar(24) not null default 'NONE'
        check (external_effect_status in ('NONE', 'IN_PROGRESS', 'UNKNOWN', 'SUCCEEDED', 'FAILED', 'VERIFICATION_FAILED')),
    add constraint task_external_effect_binding check (
        (external_effect_operation_id is null and external_effect_status in ('NONE', 'SUCCEEDED', 'FAILED', 'VERIFICATION_FAILED'))
        or (external_effect_operation_id is not null and external_effect_status in ('IN_PROGRESS', 'UNKNOWN'))
    );

create table task.workflow_dispatch (
    tenant_id uuid not null,
    workspace_id uuid not null,
    actor_id uuid not null,
    dispatch_key varchar(200) not null,
    request_hash varchar(64),
    state varchar(24) not null check (state in ('ACTIVE', 'CANCEL_REQUESTED', 'CANCELLED')),
    task_id uuid unique,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, workspace_id, actor_id, dispatch_key),
    check ((request_hash is null and task_id is null and state = 'CANCELLED')
        or request_hash ~ '^[0-9a-f]{64}$')
);

create index task_workflow_dispatch_task_idx on task.workflow_dispatch(task_id) where task_id is not null;
