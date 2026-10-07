-- 反馈只作为带来源的待审学习输入；Learning 不自动修改 Knowledge 或 Memory。
create schema if not exists learning;

create table learning.feedback (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    task_id uuid not null,
    task_attempt integer not null check (task_attempt > 0),
    task_status varchar(30) not null check (task_status in ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED')),
    actor_id uuid not null,
    source_type varchar(20) not null check (source_type = 'USER'),
    correction varchar(4000) not null check (length(trim(correction)) > 0),
    evidence varchar(4000) not null check (length(trim(evidence)) > 0),
    source_snapshot jsonb not null check (jsonb_typeof(source_snapshot) = 'object'),
    idempotency_key varchar(200) not null,
    request_hash char(64) not null,
    created_at timestamptz not null,
    unique (tenant_id, workspace_id, task_id, actor_id, idempotency_key)
);

create index learning_feedback_task_idx
    on learning.feedback(tenant_id, workspace_id, task_id, created_at, id);

-- 事件 ID 与反馈 ID 一致，重放同一反馈不会派生出第二条学习输入。
create table learning.outbox (
    event_id uuid primary key references learning.feedback(id),
    event_type varchar(80) not null check (event_type = 'eaf.feedback.recorded.v1'),
    payload jsonb not null check (jsonb_typeof(payload) = 'object'),
    status varchar(20) not null default 'PENDING' check (status in ('PENDING', 'DELIVERED', 'FAILED')),
    created_at timestamptz not null default now()
);
