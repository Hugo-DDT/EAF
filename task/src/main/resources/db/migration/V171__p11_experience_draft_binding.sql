create table task.experience_draft_binding (
    task_id uuid primary key references task.task(id),
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    source_task_id uuid not null references task.task(id),
    source_feedback_id uuid not null,
    source_conversation_id uuid references task.conversation(id),
    input_hash char(64) not null,
    created_at timestamptz not null default now(),
    check (task_id <> source_task_id)
);

create index task_experience_draft_owner_idx
    on task.experience_draft_binding(tenant_id, workspace_id, owner_id, created_at desc, task_id);
