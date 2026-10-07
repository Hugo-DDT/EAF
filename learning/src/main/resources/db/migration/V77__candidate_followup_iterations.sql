-- Learning 只记录 Task API 和 Runtime API 核实过的发布后使用证据，不复制 Task 正文或结果。
create table learning.candidate_iteration (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null,
    source_feedback_id uuid not null references learning.feedback(id),
    source_task_id uuid not null,
    evaluation_report_id uuid not null,
    approval_id uuid not null references learning.candidate_approval(id),
    release_id uuid not null references learning.candidate_release(id),
    target_release_id uuid not null,
    target_type varchar(30) not null check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT')),
    target_id uuid not null,
    target_version varchar(80) not null,
    followup_task_id uuid not null,
    followup_attempt integer not null check (followup_attempt > 0),
    task_status varchar(30) not null,
    usage_status varchar(30) not null check (usage_status in ('CITED', 'AVAILABLE_NOT_CITED', 'NOT_AVAILABLE', 'TASK_NOT_SUCCEEDED')),
    target_source jsonb,
    input_hash char(64) not null,
    result_hash char(64),
    reported_risk_level varchar(20),
    fact_outcome varchar(30) not null check (fact_outcome = 'UNVERIFIED'),
    request_hash char(64) not null,
    task_created_at timestamptz not null,
    recorded_at timestamptz not null,
    foreign key (candidate_id, candidate_revision)
        references learning.candidate_revision(candidate_id, revision),
    unique (candidate_id, candidate_revision, followup_task_id, followup_attempt)
);

create index candidate_iteration_history_idx
    on learning.candidate_iteration(tenant_id, workspace_id, candidate_id, candidate_revision, recorded_at, id);
