-- 候选、不可变修订、人工审核和反馈来源消费状态只归 Learning 所有。
create table learning.candidate (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    proposer_id uuid not null,
    source_feedback_id uuid references learning.feedback(id),
    current_revision integer not null check (current_revision > 0),
    status varchar(30) not null check (status in ('PROPOSED', 'IN_REVIEW', 'EVALUATING', 'AWAITING_APPROVAL', 'APPROVED', 'PUBLISHED', 'REJECTED')),
    row_version bigint not null check (row_version > 0),
    idempotency_key varchar(200) not null,
    request_hash char(64) not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    unique (tenant_id, workspace_id, proposer_id, idempotency_key)
);

create table learning.candidate_revision (
    candidate_id uuid not null references learning.candidate(id),
    revision integer not null check (revision > 0),
    target_type varchar(30) not null check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT')),
    target_id uuid,
    base_version varchar(80),
    owner_id uuid not null,
    scope varchar(30) not null,
    base_snapshot jsonb,
    proposed_content jsonb not null,
    evidence_refs jsonb not null check (jsonb_typeof(evidence_refs) = 'array'),
    evidence_gap boolean not null,
    content_hash char(64) not null,
    created_at timestamptz not null,
    primary key (candidate_id, revision),
    check ((target_type = 'KNOWLEDGE_UPDATE' and target_id is not null and base_version ~ '^[1-9][0-9]*$')
        or (target_type = 'MEMORY_UPSERT' and (target_id is null or base_version is not null)))
);

create table learning.candidate_review (
    id uuid primary key,
    candidate_id uuid not null,
    candidate_revision integer not null,
    reviewer_id uuid not null,
    decision varchar(20) not null check (decision in ('ACCEPTED', 'REJECTED')),
    reason varchar(1000) not null check (length(trim(reason)) > 0),
    fact_evidence_refs jsonb not null check (jsonb_typeof(fact_evidence_refs) = 'array'),
    created_at timestamptz not null,
    foreign key (candidate_id, candidate_revision) references learning.candidate_revision(candidate_id, revision),
    unique (candidate_id, candidate_revision, reviewer_id)
);

-- 规则草拟按每个反馈/目标只处理一次，并持久记录跳过原因以供排查。
create table learning.candidate_source (
    tenant_id uuid not null,
    workspace_id uuid not null,
    feedback_id uuid not null references learning.feedback(id),
    target_type varchar(30) not null check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT')),
    target_id uuid not null,
    outcome varchar(20) not null check (outcome in ('PROCESSING', 'CREATED', 'SKIPPED')),
    reason_code varchar(80),
    candidate_id uuid references learning.candidate(id),
    processed_at timestamptz not null,
    primary key (tenant_id, workspace_id, feedback_id, target_type, target_id)
);

create index learning_candidate_workspace_idx
    on learning.candidate(tenant_id, workspace_id, updated_at desc, id);
