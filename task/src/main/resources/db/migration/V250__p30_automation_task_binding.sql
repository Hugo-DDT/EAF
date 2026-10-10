create table task.automation_task_binding (
    task_id uuid primary key references task.task(id),
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    subscription_id uuid not null,
    run_id uuid not null unique,
    authorization_epoch bigint not null,
    input_hash char(64) not null,
    profile_hash char(64) not null,
    created_at timestamptz not null default now()
);
create index automation_task_owner_idx on task.automation_task_binding(tenant_id, workspace_id, owner_id, task_id);
