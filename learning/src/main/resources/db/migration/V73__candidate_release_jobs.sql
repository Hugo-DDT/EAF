-- 发布期间冻结候选当前修订；Learning 只保存意图和目标发布回执，不写目标域表。
alter table learning.candidate drop constraint candidate_status_check;
alter table learning.candidate add constraint candidate_status_check
    check (status in ('PROPOSED', 'IN_REVIEW', 'EVALUATING', 'AWAITING_APPROVAL', 'APPROVED', 'PUBLISHING', 'PUBLISHED', 'REJECTED'));

create table learning.candidate_release (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null,
    approval_id uuid not null references learning.candidate_approval(id),
    target_type varchar(30) not null check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT')),
    target_id uuid,
    base_version varchar(80),
    owner_id uuid not null,
    candidate_content_hash char(64) not null,
    request_hash char(64) not null,
    status varchar(20) not null check (status in ('PUBLISHING', 'RELEASED', 'FAILED')),
    target_release_id uuid,
    target_version varchar(80),
    target_content_hash char(64),
    failure_code varchar(80),
    attempts integer not null default 1 check (attempts > 0),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    unique (candidate_id, candidate_revision),
    foreign key (candidate_id, candidate_revision)
        references learning.candidate_revision(candidate_id, revision),
    check ((target_type = 'KNOWLEDGE_UPDATE' and target_id is not null and base_version ~ '^[1-9][0-9]*$')
        or (target_type = 'MEMORY_UPSERT' and (target_id is null or base_version is not null))),
    check ((status = 'RELEASED' and target_release_id is not null and target_version is not null
            and target_content_hash is not null and failure_code is null)
        or (status = 'FAILED' and failure_code is not null)
        or (status = 'PUBLISHING' and failure_code is null))
);

create index candidate_release_recovery_idx
    on learning.candidate_release(status, updated_at, id);
