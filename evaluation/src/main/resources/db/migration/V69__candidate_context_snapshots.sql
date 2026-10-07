-- 评测域持有隔离的基线/候选输入；候选答案不在此处保存或暴露给 Learning。
create table evaluation.candidate_context_snapshot (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null check (candidate_revision > 0),
    target_type varchar(30) not null check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT')),
    target_id uuid,
    base_version varchar(80),
    owner_id uuid not null,
    scope varchar(30) not null,
    candidate_content_hash char(64) not null,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    baseline_context jsonb not null,
    baseline_context_hash char(64) not null,
    candidate_context jsonb not null,
    candidate_context_hash char(64) not null,
    created_at timestamptz not null,
    invalidated_at timestamptz,
    unique (tenant_id, workspace_id, candidate_id, candidate_revision)
);

create table evaluation.candidate_pair_run (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    snapshot_id uuid not null references evaluation.candidate_context_snapshot(id),
    input_text text not null check (length(input_text) between 1 and 8000),
    input_hash char(64) not null,
    status varchar(20) not null check (status in ('PREPARING', 'RUNNING', 'COMPLETED', 'FAILED')),
    baseline_task_id uuid,
    candidate_task_id uuid,
    baseline_status varchar(30),
    baseline_error_code varchar(80),
    candidate_status varchar(30),
    candidate_error_code varchar(80),
    created_at timestamptz not null,
    completed_at timestamptz
);

create table evaluation.candidate_pair_task (
    task_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    snapshot_id uuid not null references evaluation.candidate_context_snapshot(id),
    pair_run_id uuid not null references evaluation.candidate_pair_run(id),
    variant varchar(12) not null check (variant in ('BASELINE', 'CANDIDATE')),
    created_at timestamptz not null,
    unique (pair_run_id, variant)
);

create index evaluation_candidate_pair_snapshot_idx
    on evaluation.candidate_pair_run(tenant_id, workspace_id, snapshot_id, created_at desc);
