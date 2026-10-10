create table evaluation.prompt_analysis_run (
    run_id uuid primary key references evaluation.scenario_run(id),
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    target_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null check (candidate_revision > 0),
    split varchar(20) not null check (split in ('DEV', 'HELD_OUT')),
    base_hash char(64) not null,
    candidate_hash char(64) not null,
    dataset_hash char(64) not null,
    configuration_hash char(64) not null,
    model_mode varchar(20) not null,
    created_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, candidate_id, candidate_revision, split)
);

create index prompt_analysis_candidate_reports_idx
    on evaluation.prompt_analysis_run(tenant_id, workspace_id, candidate_id, candidate_revision, split);
