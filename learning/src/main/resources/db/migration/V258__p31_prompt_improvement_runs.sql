-- Prompt 改进编排只保存 Learning 状态和精确来源；Prompt/Evaluation Owner 保留各自数据权威。
alter table learning.candidate_revision drop constraint if exists candidate_revision_target_type_check;
alter table learning.candidate_revision add constraint candidate_revision_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE', 'PROMPT_UPDATE'));
alter table learning.candidate_revision drop constraint if exists candidate_revision_target_binding_check;
alter table learning.candidate_revision add constraint candidate_revision_target_binding_check
    check ((target_type = 'KNOWLEDGE_UPDATE' and target_id is not null and base_version ~ '^[1-9][0-9]*$')
        or (target_type = 'MEMORY_UPSERT' and (target_id is null or base_version is not null))
        or (target_type = 'TEAM_EXPERIENCE_UPDATE' and target_id is not null and base_version is not null)
        or (target_type = 'PROMPT_UPDATE' and target_id is not null and base_version ~ '^[0-9a-f]{64}$'));

alter table learning.candidate_approval drop constraint if exists candidate_approval_target_type_check;
alter table learning.candidate_approval add constraint candidate_approval_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE', 'PROMPT_UPDATE'));
alter table learning.candidate_release drop constraint if exists candidate_release_target_type_check;
alter table learning.candidate_release add constraint candidate_release_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE', 'PROMPT_UPDATE'));
alter table learning.candidate_release drop constraint if exists candidate_release_target_binding_check;
alter table learning.candidate_release add constraint candidate_release_target_binding_check
    check ((target_type = 'KNOWLEDGE_UPDATE' and target_id is not null and base_version ~ '^[1-9][0-9]*$')
        or (target_type = 'MEMORY_UPSERT' and (target_id is null or base_version is not null))
        or (target_type = 'TEAM_EXPERIENCE_UPDATE' and target_id is not null and base_version is not null)
        or (target_type = 'PROMPT_UPDATE' and target_id is not null and base_version ~ '^[0-9a-f]{64}$'));
alter table learning.candidate_withdrawal drop constraint if exists candidate_withdrawal_target_type_check;
alter table learning.candidate_withdrawal add constraint candidate_withdrawal_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE', 'PROMPT_UPDATE'));
alter table learning.candidate_iteration drop constraint if exists candidate_iteration_target_type_check;
alter table learning.candidate_iteration add constraint candidate_iteration_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE', 'PROMPT_UPDATE'));

create table learning.prompt_improvement_run (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    target_id uuid not null,
    expected_target_version bigint not null check (expected_target_version > 0),
    base_hash char(64) not null,
    source_feedbacks jsonb not null check (jsonb_typeof(source_feedbacks) = 'array'
        and jsonb_array_length(source_feedbacks) between 0 and 3),
    change_note varchar(500) not null check (length(trim(change_note)) between 1 and 500),
    instruction_appendix varchar(1000) not null check (length(trim(instruction_appendix)) between 1 and 1000),
    request_hash char(64) not null,
    idempotency_key_hash char(64) not null,
    candidate_id uuid,
    candidate_revision integer,
    dev_report_id uuid,
    held_out_report_id uuid,
    status varchar(32) not null check (status in ('AWAITING_REVIEW','DEV_EVALUATING','HELD_OUT_EVALUATING',
        'READY','NOT_SELECTED','REJECTED','STOPPING','STOPPED','TIMED_OUT','STALE','PUBLISHED','WITHDRAWN','FAILED')),
    reason_code varchar(80),
    row_version bigint not null default 1 check (row_version > 0),
    deadline_at timestamptz not null,
    next_poll_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, owner_id, idempotency_key_hash),
    check ((candidate_id is null and candidate_revision is null)
        or (candidate_id is not null and candidate_revision > 0))
);

create index prompt_improvement_run_poll_idx
    on learning.prompt_improvement_run(next_poll_at, created_at)
    where status in ('DEV_EVALUATING','HELD_OUT_EVALUATING');

create unique index prompt_improvement_candidate_revision_uq
    on learning.prompt_improvement_run(candidate_id, candidate_revision)
    where candidate_id is not null;
