alter table evaluation.quality_run_registration
    drop constraint if exists quality_run_registration_purpose_check;
alter table evaluation.quality_run_registration
    add constraint quality_run_registration_purpose_check
    check (purpose in ('CRM_INTEGRATION_ACCEPTANCE', 'MODEL_PROVIDER_SMOKE', 'RAG_HELD_OUT',
                       'COLLABORATION_HELD_OUT', 'SERVICE_REQUEST_SCENARIO', 'TEAM_EXPERIENCE_IMPROVEMENT'));

create table evaluation.team_improvement_generation (
    quality_run_id uuid primary key references evaluation.quality_run_registration(id),
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    improvement_run_id uuid not null,
    request_hash char(64) not null,
    input_hash char(64) not null,
    task_key varchar(200) not null,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    prompt_id uuid not null,
    prompt_version varchar(40) not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    capability_hash char(64) not null,
    skill_id uuid not null,
    skill_version varchar(40) not null,
    skill_hash char(64) not null,
    task_id uuid unique,
    task_attempt integer,
    status varchar(20) not null check (status in ('READY', 'ACTIVE', 'SUCCEEDED', 'FAILED', 'STOPPING', 'STOPPED')),
    deadline_at timestamptz not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, owner_id, improvement_run_id),
    check ((task_id is null and task_attempt is null) or (task_id is not null and task_attempt > 0))
);
