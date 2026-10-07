create table task.customer_followup (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    customer_id varchar(160) not null,
    creator_id uuid not null,
    assignee_id uuid not null,
    summary varchar(2000) not null check (length(btrim(summary)) between 1 and 2000),
    due_at timestamptz,
    business_status varchar(20) not null check (business_status in ('OPEN', 'IN_PROGRESS', 'CLOSED')),
    source_conversation_id uuid not null references task.conversation(id),
    source_task_id uuid not null references task.task(id),
    source_task_version bigint not null check (source_task_version > 0),
    source_brief_revision integer not null check (source_brief_revision >= 0),
    create_workflow_id uuid,
    create_operation_id uuid,
    create_external_id varchar(160),
    last_result_no integer not null default 0 check (last_result_no >= 0),
    row_version bigint not null default 1 check (row_version > 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (creator_id <> '00000000-0000-0000-0000-000000000000'),
    unique (id, tenant_id, workspace_id)
);

create index customer_followup_workspace_update_idx
    on task.customer_followup(tenant_id, workspace_id, updated_at desc, id desc);
create index customer_followup_assignee_idx
    on task.customer_followup(tenant_id, workspace_id, assignee_id, business_status, updated_at desc, id desc);
create index customer_followup_customer_idx
    on task.customer_followup(tenant_id, workspace_id, customer_id, updated_at desc, id desc);

create table task.customer_followup_result (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    followup_id uuid not null,
    result_no integer not null check (result_no > 0),
    recorded_by uuid not null,
    outcome_code varchar(24) not null check (outcome_code in ('CONTACTED', 'NO_RESPONSE', 'RESOLVED', 'OTHER')),
    summary varchar(2000) not null check (length(btrim(summary)) between 1 and 2000),
    next_action varchar(500) check (next_action is null or length(next_action) <= 500),
    next_contact_at timestamptz,
    disposition varchar(12) not null check (disposition in ('CONTINUE', 'CLOSE')),
    corrects_result_id uuid,
    created_at timestamptz not null default now(),
    foreign key (followup_id, tenant_id, workspace_id)
        references task.customer_followup(id, tenant_id, workspace_id),
    foreign key (corrects_result_id) references task.customer_followup_result(id),
    unique (followup_id, result_no),
    unique (id, followup_id, tenant_id, workspace_id)
);

create index customer_followup_result_timeline_idx
    on task.customer_followup_result(followup_id, result_no desc);

create table task.customer_followup_command (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    actor_id uuid not null,
    action varchar(20) not null check (action in ('CREATE', 'RESULT', 'SYNC')),
    request_key varchar(200) not null,
    request_hash char(64) not null,
    followup_id uuid,
    result_id uuid,
    sync_attempt_id uuid,
    created_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, actor_id, action, request_key),
    foreign key (followup_id, tenant_id, workspace_id)
        references task.customer_followup(id, tenant_id, workspace_id),
    foreign key (result_id, followup_id, tenant_id, workspace_id)
        references task.customer_followup_result(id, followup_id, tenant_id, workspace_id)
);

create table task.customer_followup_sync (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    followup_id uuid not null,
    result_id uuid not null,
    attempt_no integer not null check (attempt_no > 0),
    submitted_by uuid not null,
    workflow_instance_id uuid,
    admission_open boolean not null default true,
    terminal_state varchar(20) not null default 'PENDING' check (terminal_state in ('PENDING', 'SUCCEEDED', 'FAILED_SAFE')),
    created_at timestamptz not null default now(),
    foreign key (followup_id, tenant_id, workspace_id)
        references task.customer_followup(id, tenant_id, workspace_id),
    foreign key (result_id, followup_id, tenant_id, workspace_id)
        references task.customer_followup_result(id, followup_id, tenant_id, workspace_id),
    unique (followup_id, result_id, attempt_no),
    unique (workflow_instance_id)
);

create index customer_followup_sync_workflow_idx
    on task.customer_followup_sync(workflow_instance_id) where workflow_instance_id is not null;
create unique index customer_followup_sync_one_open_per_card_idx
    on task.customer_followup_sync(followup_id) where admission_open;

alter table task.customer_followup_command
    add constraint customer_followup_command_sync_fk
        foreign key (sync_attempt_id) references task.customer_followup_sync(id);
