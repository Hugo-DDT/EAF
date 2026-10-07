-- P21 批次和固定双分支由 Workflow Owner 持久化，Task 正文仍由 Task Owner 管理。
alter table workflow.step drop constraint workflow_step_type_check;
alter table workflow.step add constraint workflow_step_type_check
    check (step_type in ('RUN_CAPABILITY', 'PARALLEL_READ', 'BRANCH', 'RUN_TOOL', 'HUMAN_TASK', 'COMPLETE'));
alter table workflow.step drop constraint workflow_step_state_check;
alter table workflow.step add constraint workflow_step_state_check check (
    (step_type in ('RUN_CAPABILITY', 'RUN_TOOL', 'PARALLEL_READ') and status in ('INTENT', 'WAITING_CHILD', 'SUCCEEDED'))
    or (step_type = 'HUMAN_TASK' and status in ('INTENT', 'WAITING_HUMAN', 'SUCCEEDED'))
    or (step_type in ('BRANCH', 'COMPLETE') and status in ('INTENT', 'SUCCEEDED'))
);

create table workflow.parallel_branch (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    instance_id uuid not null,
    group_step_id varchar(64) not null,
    role varchar(24) not null check (role in ('knowledge', 'experience')),
    effective_step_id varchar(120) not null,
    dispatch_key varchar(200) not null unique,
    input_json jsonb not null check (jsonb_typeof(input_json) = 'object'),
    input_hash varchar(64) not null check (input_hash ~ '^[0-9a-f]{64}$'),
    capability_id uuid not null,
    capability_version varchar(40) not null,
    child_task_id uuid unique,
    status varchar(24) not null check (status in ('INTENT', 'WAITING_CHILD', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')),
    error_code varchar(80),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (instance_id, group_step_id, role),
    foreign key (instance_id, tenant_id, workspace_id)
        references workflow.instance(id, tenant_id, workspace_id),
    foreign key (instance_id, group_step_id) references workflow.step(instance_id, step_id)
);
create index workflow_parallel_branch_instance_idx
    on workflow.parallel_branch(instance_id, group_step_id, role);

create table workflow.batch (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    initiator_id uuid not null,
    idempotency_key_hash varchar(64) not null check (idempotency_key_hash ~ '^[0-9a-f]{64}$'),
    payload_hash varchar(64) not null check (payload_hash ~ '^[0-9a-f]{64}$'),
    definition_id uuid not null,
    definition_version varchar(40) not null,
    definition_hash varchar(64) not null check (definition_hash ~ '^[0-9a-f]{64}$'),
    item_count integer not null check (item_count between 1 and 20),
    max_active_items integer not null check (max_active_items between 1 and 4),
    deadline_at timestamptz not null,
    status varchar(24) not null check (status in ('PENDING', 'RUNNING', 'CANCELLING', 'SUCCEEDED', 'PARTIAL', 'FAILED', 'CANCELLED', 'TIMED_OUT')),
    termination_target varchar(24) check (termination_target in ('CANCELLED', 'TIMED_OUT')),
    cancel_requested boolean not null default false,
    row_version bigint not null default 1 check (row_version > 0),
    lease_owner_id uuid,
    lease_fence bigint not null default 0 check (lease_fence >= 0),
    lease_until timestamptz,
    next_poll_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, initiator_id, idempotency_key_hash),
    check ((lease_owner_id is null and lease_until is null) or (lease_owner_id is not null and lease_until is not null))
);
create index workflow_batch_dispatch_idx on workflow.batch(next_poll_at, lease_until, created_at)
    where status in ('PENDING', 'RUNNING', 'CANCELLING');

create table workflow.batch_item (
    batch_id uuid not null references workflow.batch(id),
    tenant_id uuid not null,
    workspace_id uuid not null,
    item_key varchar(64) not null check (item_key ~ '^[A-Za-z0-9_-]{1,64}$'),
    ordinal integer not null check (ordinal between 1 and 20),
    request_text varchar(8000) not null,
    scenario_key varchar(64),
    experience_refs_json jsonb not null default '[]'::jsonb check (jsonb_typeof(experience_refs_json) = 'array'),
    input_hash varchar(64) not null check (input_hash ~ '^[0-9a-f]{64}$'),
    stable_create_key varchar(200) not null,
    workflow_instance_id uuid,
    status varchar(24) not null check (status in ('PENDING', 'ACTIVE', 'CANCELLING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')),
    error_code varchar(80),
    cancel_requested boolean not null default false,
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (batch_id, item_key),
    unique (batch_id, ordinal),
    unique (workflow_instance_id),
    foreign key (workflow_instance_id, tenant_id, workspace_id)
        references workflow.instance(id, tenant_id, workspace_id)
);
create index workflow_batch_item_order_idx on workflow.batch_item(batch_id, ordinal);
