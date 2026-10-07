create table memory.experience_card (
    memory_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    applicability varchar(16) not null check (applicability in ('GENERAL', 'CUSTOMER')),
    customer_id varchar(160),
    latest_revision integer not null check (latest_revision > 0),
    active_revision integer,
    row_version bigint not null default 1 check (row_version > 0),
    active_since timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check ((applicability = 'GENERAL' and customer_id is null)
        or (applicability = 'CUSTOMER' and customer_id is not null and length(trim(customer_id)) > 0)),
    check (active_revision is null or active_revision > 0),
    unique (memory_id, tenant_id, workspace_id),
    foreign key (memory_id, tenant_id, workspace_id)
        references memory.definition(id, tenant_id, workspace_id)
);

create table memory.experience_card_revision (
    memory_id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    revision integer not null check (revision > 0),
    memory_version varchar(40) not null,
    title varchar(80) not null check (length(trim(title)) > 0),
    source_task_id uuid,
    source_feedback_id uuid,
    draft_task_id uuid,
    created_at timestamptz not null default now(),
    primary key (memory_id, workspace_id, revision),
    unique (memory_id, tenant_id, workspace_id, revision),
    unique (memory_id, workspace_id, memory_version),
    foreign key (memory_id, tenant_id, workspace_id)
        references memory.definition(id, tenant_id, workspace_id),
    foreign key (memory_id, tenant_id, workspace_id, memory_version)
        references memory.version(memory_id, tenant_id, workspace_id, asset_version)
);

alter table memory.experience_card add constraint memory_experience_active_revision_fk
    foreign key (memory_id, tenant_id, workspace_id, active_revision)
    references memory.experience_card_revision(memory_id, tenant_id, workspace_id, revision)
    deferrable initially deferred;

alter table memory.experience_card add constraint memory_experience_latest_revision_fk
    foreign key (memory_id, tenant_id, workspace_id, latest_revision)
    references memory.experience_card_revision(memory_id, tenant_id, workspace_id, revision)
    deferrable initially deferred;

create table memory.experience_command (
    command_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    request_key_hash char(64) not null,
    request_hash char(64) not null,
    action varchar(16) not null check (action in ('CREATE', 'SAVE', 'PUBLISH', 'REVOKE')),
    card_id uuid not null,
    result_revision integer not null check (result_revision > 0),
    result_memory_version varchar(40) not null,
    result_card_version bigint not null check (result_card_version > 0),
    created_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, owner_id, request_key_hash),
    foreign key (card_id, tenant_id, workspace_id)
        references memory.experience_card(memory_id, tenant_id, workspace_id)
);

create index memory_experience_owner_updated_idx
    on memory.experience_card(tenant_id, workspace_id, owner_id, updated_at desc, memory_id);
create index memory_experience_applicable_idx
    on memory.experience_card(tenant_id, workspace_id, owner_id, applicability, customer_id, active_since desc)
    where active_revision is not null;
