create table task.service_request_submission (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    source_task_id uuid not null,
    source_task_version bigint not null check (source_task_version > 0),
    source_result_hash char(64) not null check (source_result_hash ~ '^[0-9a-f]{64}$'),
    request_key_hash char(64) not null check (request_key_hash ~ '^[0-9a-f]{64}$'),
    request_hash char(64) not null check (request_hash ~ '^[0-9a-f]{64}$'),
    payload_json jsonb not null check (jsonb_typeof(payload_json) = 'object'),
    context_refs_json jsonb not null check (jsonb_typeof(context_refs_json) = 'array'),
    workflow_instance_id uuid,
    created_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, source_task_id),
    unique (tenant_id, workspace_id, owner_id, request_key_hash),
    foreign key (tenant_id, workspace_id, source_task_id)
        references task.task(tenant_id, workspace_id, id)
);
