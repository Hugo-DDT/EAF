create table workflow.automation_subscription (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    name varchar(120) not null,
    trigger_kind varchar(32) not null check (trigger_kind in ('WEEKLY', 'P16_WORK_ITEM_CHANGED')),
    day_of_week varchar(16),
    local_time time,
    time_zone varchar(80),
    work_item_id uuid,
    max_items integer not null check (max_items between 1 and 20),
    expires_at timestamptz not null,
    max_runs integer not null check (max_runs between 1 and 50),
    admitted_runs integer not null default 0 check (admitted_runs between 0 and max_runs),
    next_fire_at timestamptz,
    status varchar(16) not null check (status in ('ACTIVE','PAUSED','BLOCKED','EXPIRED','EXHAUSTED','DELETED')),
    reason varchar(80),
    authorization_epoch bigint not null default 1 check (authorization_epoch > 0),
    epoch_started_at timestamptz not null default now(),
    active_run_id uuid,
    last_observed_source_row_version bigint,
    request_hash char(64) not null,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    capability_hash char(64) not null,
    skill_id uuid not null,
    skill_version varchar(40) not null,
    skill_hash char(64) not null,
    profile_hash char(64) not null,
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check ((trigger_kind = 'WEEKLY' and day_of_week is not null and local_time is not null
            and time_zone is not null and work_item_id is null)
        or (trigger_kind = 'P16_WORK_ITEM_CHANGED' and day_of_week is null and local_time is null
            and time_zone is null and work_item_id is not null and max_items = 1))
);
create index automation_subscription_due_idx on workflow.automation_subscription(status, next_fire_at, id);
create index automation_subscription_owner_idx on workflow.automation_subscription(tenant_id, workspace_id, owner_id, created_at desc, id desc);
create index automation_subscription_work_item_idx on workflow.automation_subscription(tenant_id, workspace_id, work_item_id, status);

create table workflow.automation_run (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    subscription_id uuid not null references workflow.automation_subscription(id),
    authorization_epoch bigint not null,
    trigger_kind varchar(32) not null check (trigger_kind in ('WEEKLY','P16_WORK_ITEM_CHANGED')),
    trigger_key varchar(240) not null,
    planned_at timestamptz not null,
    occurred_at timestamptz,
    event_source varchar(120),
    event_id varchar(240),
    source_row_version bigint,
    status varchar(16) not null check (status in ('READY','ADMITTED','SUCCEEDED','FAILED','CANCELLED','SKIPPED')),
    reason varchar(80),
    input_snapshot jsonb,
    input_hash char(64),
    task_id uuid unique,
    task_status varchar(24),
    attempts integer not null default 0 check (attempts between 0 and 3),
    next_attempt_at timestamptz,
    generation_attempted boolean not null default false,
    generation_task_id uuid,
    generation_attempt integer,
    result_json jsonb,
    model_result_json jsonb,
    result_markdown text,
    admitted_at timestamptz,
    finished_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(subscription_id, trigger_key)
);
create index automation_run_subscription_idx on workflow.automation_run(subscription_id, created_at desc, id desc);
create index automation_run_recovery_idx on workflow.automation_run(status, next_attempt_at, created_at);

create table workflow.automation_action (
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    key_hash char(64) not null,
    request_hash char(64) not null,
    subscription_id uuid not null,
    response_json jsonb not null,
    created_at timestamptz not null default now(),
    primary key(tenant_id, workspace_id, owner_id, key_hash)
);

create table workflow.p16_work_item_event (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    source varchar(120) not null,
    event_id varchar(240) not null,
    event_type varchar(120) not null,
    work_item_id uuid not null,
    source_row_version bigint not null,
    status varchar(24) not null,
    old_assignee_id uuid,
    new_assignee_id uuid,
    occurred_at timestamptz not null,
    processed_at timestamptz,
    outcome varchar(32),
    unique(source, event_id)
);
create index p16_work_item_event_pending_idx on workflow.p16_work_item_event(occurred_at, id) where processed_at is null;
