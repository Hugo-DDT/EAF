create table workflow.p27_request_lock (
    tenant_id uuid not null,
    workspace_id uuid not null,
    request_id varchar(160) not null,
    primary key (tenant_id, workspace_id, request_id)
);

create table workflow.p27_result_sync (
    sync_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    actor_id uuid not null,
    work_item_id uuid not null,
    work_item_version bigint not null check (work_item_version > 0),
    source_result_hash varchar(64) not null check (source_result_hash ~ '^[0-9a-f]{64}$'),
    request_id varchar(160) not null,
    registration_operation_id varchar(160) not null,
    state_query_id uuid not null,
    expected_external_version varchar(120) not null,
    binding_version varchar(64) not null check (binding_version ~ '^[0-9a-f]{64}$'),
    external_subject_id varchar(160) not null,
    completed_by uuid not null,
    completed_at timestamptz not null,
    outcome varchar(30) not null check (outcome in ('COMPLETED', 'BLOCKED', 'NEEDS_FOLLOWUP')),
    summary varchar(2000) not null,
    next_action varchar(1000),
    workflow_instance_id uuid not null,
    idempotency_key_hash varchar(64) not null check (idempotency_key_hash ~ '^[0-9a-f]{64}$'),
    request_hash varchar(64) not null check (request_hash ~ '^[0-9a-f]{64}$'),
    status varchar(20) not null check (status in ('PENDING', 'UNKNOWN', 'SUCCEEDED', 'FAILED_SAFE')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, actor_id, idempotency_key_hash),
    unique (workflow_instance_id)
);

create index p27_result_sync_work_item_page on workflow.p27_result_sync
    (tenant_id, workspace_id, work_item_id, created_at desc, sync_id desc);
create unique index p27_result_sync_request_admission on workflow.p27_result_sync
    (tenant_id, workspace_id, request_id) where status in ('PENDING', 'UNKNOWN', 'SUCCEEDED');
