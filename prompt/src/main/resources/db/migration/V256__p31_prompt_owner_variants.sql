create table prompt.owner_target (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    base_prompt_id uuid not null,
    base_version varchar(40) not null,
    base_hash char(64) not null,
    idempotency_key_hash char(64) not null,
    row_version bigint not null default 1 check (row_version > 0),
    status varchar(20) not null check (status in ('READY', 'STAGED', 'PUBLISHED', 'WITHDRAWN')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, owner_id, idempotency_key_hash),
    unique (id, tenant_id, workspace_id)
);

create table prompt.analysis_candidate (
    tenant_id uuid not null,
    workspace_id uuid not null,
    target_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null check (candidate_revision > 0),
    base_hash char(64) not null,
    system_template text not null,
    user_template text not null,
    content_hash char(64) not null,
    status varchar(20) not null check (status in ('STAGED', 'RELEASED', 'REVOKED', 'SUPERSEDED')),
    created_at timestamptz not null default now(),
    primary key (tenant_id, workspace_id, candidate_id, candidate_revision),
    foreign key (target_id, tenant_id, workspace_id)
        references prompt.owner_target(id, tenant_id, workspace_id)
);

create table prompt.analysis_release (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    target_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null,
    prompt_id uuid not null,
    prompt_version varchar(40) not null,
    content_hash char(64) not null,
    approval_id uuid not null,
    report_id uuid not null,
    report_hash char(64) not null,
    status varchar(20) not null check (status in ('RELEASED', 'REVOKED')),
    created_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, candidate_id, candidate_revision),
    foreign key (target_id, tenant_id, workspace_id)
        references prompt.owner_target(id, tenant_id, workspace_id),
    foreign key (tenant_id, workspace_id, candidate_id, candidate_revision)
        references prompt.analysis_candidate(tenant_id, workspace_id, candidate_id, candidate_revision)
);

create table prompt.analysis_withdrawal (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    release_id uuid not null references prompt.analysis_release(id),
    candidate_id uuid not null,
    candidate_revision integer not null,
    prompt_id uuid not null,
    prompt_version varchar(40) not null,
    content_hash char(64) not null,
    status varchar(20) not null check (status = 'REVOKED'),
    created_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, candidate_id, candidate_revision)
);
