create table learning.candidate_approval (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null,
    approver_id uuid not null,
    decision varchar(20) not null check (decision in ('APPROVED', 'REJECTED')),
    target_type varchar(30) not null check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT')),
    target_id uuid,
    base_version varchar(80),
    owner_id uuid not null,
    scope varchar(30) not null,
    candidate_content_hash char(64) not null,
    evidence_hash char(64) not null,
    evaluation_report_id uuid,
    evaluation_report_hash char(64),
    evaluation_configuration_hash char(64),
    dataset_hash char(64),
    evaluation_summary jsonb,
    authorization_action varchar(40) not null check (authorization_action = 'learning:approve'),
    valid_until timestamptz,
    reason varchar(1000) not null check (length(trim(reason)) > 0),
    created_at timestamptz not null,
    unique (candidate_id, candidate_revision),
    foreign key (candidate_id, candidate_revision)
        references learning.candidate_revision(candidate_id, revision),
    check ((decision = 'APPROVED' and evaluation_report_id is not null
            and evaluation_report_hash is not null and evaluation_configuration_hash is not null
            and dataset_hash is not null and evaluation_summary is not null and valid_until is not null)
        or (decision = 'REJECTED' and evaluation_report_id is null
            and evaluation_report_hash is null and evaluation_configuration_hash is null
            and dataset_hash is null and evaluation_summary is null and valid_until is null))
);

create index candidate_approval_history_idx
    on learning.candidate_approval(tenant_id, workspace_id, candidate_id, candidate_revision, created_at desc);
