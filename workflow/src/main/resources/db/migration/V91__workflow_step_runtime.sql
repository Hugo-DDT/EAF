-- Workflow 域保存可恢复的步骤意图、子 Task 关联和实例租约；Task ID 仅作为跨域关联值。
alter table workflow.instance
    add column authorization_hash varchar(64),
    add column current_step_id varchar(64),
    add column root_task_id uuid,
    add column lease_owner_id uuid,
    add column lease_fence bigint not null default 0 check (lease_fence >= 0),
    add column lease_until timestamptz,
    add column next_poll_at timestamptz,
    add column row_version bigint not null default 1 check (row_version > 0),
    add column error_code varchar(80),
    add column updated_at timestamptz not null default now(),
    add constraint workflow_instance_identity_unique unique (id, tenant_id, workspace_id),
    add constraint workflow_instance_lease_check check (
        (lease_owner_id is null and lease_until is null)
        or (lease_owner_id is not null and lease_until is not null)
    ),
    add constraint workflow_instance_authorization_hash_check check (
        (delegation_id is null and authorization_hash is null)
        or (delegation_id is not null and authorization_hash ~ '^[0-9a-f]{64}$')
    );

create index workflow_instance_dispatch_idx
    on workflow.instance(status, next_poll_at, lease_until, created_at)
    where status in ('QUEUED', 'RUNNING', 'WAITING_CHILD');

create table workflow.step (
    instance_id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    step_id varchar(64) not null,
    step_type varchar(24) not null check (step_type in ('RUN_CAPABILITY', 'BRANCH', 'RUN_TOOL', 'COMPLETE')),
    status varchar(20) not null check (status in ('INTENT', 'WAITING_CHILD', 'SUCCEEDED')),
    input_json jsonb not null check (jsonb_typeof(input_json) = 'object'),
    input_hash varchar(64) not null check (input_hash ~ '^[0-9a-f]{64}$'),
    dispatch_key varchar(200) not null,
    child_task_id uuid,
    output_json jsonb,
    selected_next_step_id varchar(64),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (instance_id, step_id),
    unique (dispatch_key),
    unique (child_task_id),
    foreign key (instance_id, tenant_id, workspace_id)
        references workflow.instance(id, tenant_id, workspace_id),
    check ((step_type in ('RUN_CAPABILITY', 'RUN_TOOL') and status in ('INTENT', 'WAITING_CHILD', 'SUCCEEDED'))
        or (step_type in ('BRANCH', 'COMPLETE') and status in ('INTENT', 'SUCCEEDED')))
);

create index workflow_step_child_idx on workflow.step(child_task_id) where child_task_id is not null;
