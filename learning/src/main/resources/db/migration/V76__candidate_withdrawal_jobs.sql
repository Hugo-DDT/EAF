-- 撤回只变更目标版本可用性；Candidate 和原发布回执保留作历史事实。
create table learning.candidate_withdrawal (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null,
    candidate_release_id uuid not null unique references learning.candidate_release(id),
    target_type varchar(30) not null check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT')),
    target_id uuid,
    target_version varchar(80) not null,
    expected_target_row_version bigint not null check (expected_target_row_version > 0),
    target_withdrawal_id uuid,
    request_hash char(64) not null,
    reason_ref varchar(500) not null,
    requested_by uuid not null,
    status varchar(20) not null check (status in ('WITHDRAWING', 'WITHDRAWN', 'FAILED')),
    failure_code varchar(80),
    attempts integer not null default 1 check (attempts > 0),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    unique (candidate_id, candidate_revision),
    foreign key (candidate_id, candidate_revision)
        references learning.candidate_revision(candidate_id, revision),
    check ((status = 'WITHDRAWN' and target_withdrawal_id is not null and failure_code is null)
        or (status = 'FAILED' and failure_code is not null)
        or (status = 'WITHDRAWING' and failure_code is null))
);

create index candidate_withdrawal_recovery_idx
    on learning.candidate_withdrawal(status, updated_at, id);
