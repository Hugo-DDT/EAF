-- Learning 保存一次改进运行及候选绑定；TEAM 正式版本仍只由 Memory Owner 发布。
create table learning.improvement_run (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    card_id uuid not null,
    base_revision integer not null check (base_revision > 0),
    base_memory_version varchar(40) not null,
    expected_card_version bigint not null check (expected_card_version > 0),
    source_feedbacks jsonb not null check (jsonb_typeof(source_feedbacks) = 'array'
        and jsonb_array_length(source_feedbacks) between 1 and 5),
    shared_correction varchar(4000) not null check (length(trim(shared_correction)) between 1 and 4000),
    shared_correction_hash char(64) not null,
    generation_input_hash char(64) not null,
    model_mode varchar(20) not null check (model_mode in ('DETERMINISTIC')),
    dataset_key varchar(80) not null,
    dataset_version varchar(40) not null,
    request_hash char(64) not null,
    idempotency_key_hash char(64) not null,
    quality_run_id uuid,
    generation_task_id uuid,
    candidate_id uuid,
    candidate_revision integer,
    dev_report_id uuid,
    held_out_report_id uuid,
    status varchar(32) not null check (status in ('CREATED', 'GENERATING', 'AWAITING_REVIEW', 'DEV_EVALUATING',
        'HELD_OUT_EVALUATING', 'AWAITING_EVIDENCE', 'READY', 'APPROVED', 'PUBLISHING', 'PUBLISHED',
        'NO_CHANGE', 'NOT_SELECTED', 'REJECTED', 'STOPPING', 'STOPPED', 'TIMED_OUT', 'STALE', 'FAILED')),
    stop_reason varchar(80),
    stop_requested boolean not null default false,
    deadline_at timestamptz not null,
    row_version bigint not null check (row_version > 0),
    next_poll_at timestamptz not null,
    lease_owner_id varchar(120),
    lease_until timestamptz,
    lease_fence bigint not null default 0,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    unique (tenant_id, workspace_id, owner_id, idempotency_key_hash),
    check ((candidate_id is null and candidate_revision is null)
        or (candidate_id is not null and candidate_revision > 0))
);

create index improvement_run_dispatch_idx on learning.improvement_run(status, next_poll_at, created_at)
    where status in ('CREATED', 'GENERATING', 'DEV_EVALUATING', 'HELD_OUT_EVALUATING', 'AWAITING_EVIDENCE', 'STOPPING');

create table learning.team_candidate_binding (
    candidate_id uuid not null,
    candidate_revision integer not null,
    improvement_run_id uuid not null references learning.improvement_run(id),
    card_id uuid not null,
    base_revision integer not null,
    base_memory_version varchar(40) not null,
    expected_card_version bigint not null,
    source_work_item_id uuid not null,
    source_proof jsonb not null check (jsonb_typeof(source_proof) = 'object'),
    source_feedbacks jsonb not null check (jsonb_typeof(source_feedbacks) = 'array'),
    evidence_hash char(64) not null,
    created_at timestamptz not null,
    primary key (candidate_id, candidate_revision),
    unique (improvement_run_id),
    foreign key (candidate_id, candidate_revision)
        references learning.candidate_revision(candidate_id, revision)
);

alter table learning.candidate_revision drop constraint if exists candidate_revision_target_type_check;
alter table learning.candidate_revision add constraint candidate_revision_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE'));
alter table learning.candidate_revision drop constraint if exists candidate_revision_check;
alter table learning.candidate_revision add constraint candidate_revision_target_binding_check
    check ((target_type = 'KNOWLEDGE_UPDATE' and target_id is not null and base_version ~ '^[1-9][0-9]*$')
        or (target_type = 'MEMORY_UPSERT' and (target_id is null or base_version is not null))
        or (target_type = 'TEAM_EXPERIENCE_UPDATE' and target_id is not null and base_version is not null));

alter table learning.candidate_approval drop constraint if exists candidate_approval_target_type_check;
alter table learning.candidate_approval add constraint candidate_approval_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE'));
alter table learning.candidate_approval add column evaluation_report_kind varchar(40) not null
    default 'LEGACY_CANDIDATE_RISK';

alter table learning.candidate_release drop constraint if exists candidate_release_target_type_check;
alter table learning.candidate_release add constraint candidate_release_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE'));
alter table learning.candidate_release drop constraint if exists candidate_release_check;
alter table learning.candidate_release add constraint candidate_release_target_binding_check
    check ((target_type = 'KNOWLEDGE_UPDATE' and target_id is not null and base_version ~ '^[1-9][0-9]*$')
        or (target_type = 'MEMORY_UPSERT' and (target_id is null or base_version is not null))
        or (target_type = 'TEAM_EXPERIENCE_UPDATE' and target_id is not null and base_version is not null));

alter table learning.candidate_withdrawal drop constraint if exists candidate_withdrawal_target_type_check;
alter table learning.candidate_withdrawal add constraint candidate_withdrawal_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE'));
