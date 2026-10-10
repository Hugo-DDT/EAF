create table learning.prompt_adoption (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null check (candidate_revision > 0),
    owner_id uuid not null,
    idempotency_key_hash char(64) not null,
    prompt_id uuid not null,
    prompt_version varchar(40) not null,
    prompt_hash char(64) not null,
    agent_id uuid,
    agent_version varchar(40),
    agent_hash char(64),
    skill_id uuid,
    skill_version varchar(40),
    skill_hash char(64),
    capability_id uuid,
    capability_version varchar(40),
    capability_hash char(64),
    approval_id uuid not null,
    report_id uuid not null,
    report_hash char(64) not null,
    status varchar(20) not null check (status in ('ADOPTING','ADOPTED','WITHDRAWN')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (candidate_id, candidate_revision)
        references learning.candidate_revision(candidate_id, revision),
    unique (tenant_id, workspace_id, candidate_id, candidate_revision),
    unique (tenant_id, workspace_id, owner_id, idempotency_key_hash)
);

alter table learning.candidate_iteration drop constraint if exists candidate_iteration_target_type_check;
alter table learning.candidate_iteration add constraint candidate_iteration_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE', 'PROMPT_UPDATE'));
alter table learning.candidate_iteration drop constraint if exists candidate_iteration_usage_status_check;
alter table learning.candidate_iteration add constraint candidate_iteration_usage_status_check
    check (usage_status in ('CITED', 'AVAILABLE_NOT_CITED', 'NOT_AVAILABLE', 'TASK_NOT_SUCCEEDED', 'VARIANT_USED'));
alter table learning.candidate_iteration alter column source_feedback_id drop not null;
alter table learning.candidate_iteration alter column source_task_id drop not null;
