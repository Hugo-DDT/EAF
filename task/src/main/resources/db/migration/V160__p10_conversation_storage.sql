create table task.conversation (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    mode varchar(40) not null check (mode in ('KNOWLEDGE_QA', 'CUSTOMER_ASSISTANT')),
    title varchar(200) not null,
    customer_id varchar(160),
    capability_id uuid not null,
    capability_version varchar(40) not null,
    capability_hash varchar(64) not null check (length(capability_hash) = 64),
    agent_id uuid not null,
    agent_version varchar(40) not null,
    skill_id uuid not null,
    skill_version varchar(40) not null,
    skill_hash varchar(64) not null check (length(skill_hash) = 64),
    status varchar(20) not null check (status in ('ACTIVE', 'ARCHIVED')),
    current_brief_revision integer not null default 0 check (current_brief_revision >= 0),
    row_version bigint not null default 1 check (row_version > 0),
    last_turn_no integer not null default 0 check (last_turn_no >= 0),
    create_key varchar(64) not null,
    create_hash varchar(64) not null check (length(create_hash) = 64),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    check ((mode = 'KNOWLEDGE_QA' and customer_id is null)
        or (mode = 'CUSTOMER_ASSISTANT' and customer_id is not null and length(customer_id) between 1 and 160)),
    unique (tenant_id, workspace_id, owner_id, create_key)
);

create index conversation_owner_updated_idx
    on task.conversation (tenant_id, workspace_id, owner_id, updated_at desc, id desc);

create table task.conversation_brief_revision (
    conversation_id uuid not null references task.conversation(id),
    revision integer not null check (revision >= 0),
    content varchar(2000) not null,
    source_task_id uuid,
    source_turn_ids text not null default '',
    confirmed_by uuid not null,
    created_at timestamptz not null,
    primary key (conversation_id, revision)
);

create table task.conversation_turn (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    conversation_id uuid not null references task.conversation(id),
    task_id uuid not null unique references task.task(id),
    turn_no integer not null check (turn_no > 0),
    input_text varchar(8000) not null check (length(input_text) between 1 and 8000),
    brief_revision integer not null,
    history_through_turn_no integer not null,
    idempotency_key varchar(64) not null,
    request_hash varchar(64) not null check (length(request_hash) = 64),
    context_snapshot_json jsonb not null,
    included_task_ids text not null default '',
    created_at timestamptz not null,
    unique (conversation_id, turn_no),
    unique (conversation_id, idempotency_key)
);

create index conversation_turn_history_idx
    on task.conversation_turn (conversation_id, turn_no desc);

create table task.conversation_brief_save (
    conversation_id uuid not null references task.conversation(id),
    save_key varchar(64) not null,
    request_hash varchar(64) not null check (length(request_hash) = 64),
    result_revision integer not null,
    created_at timestamptz not null,
    primary key (conversation_id, save_key)
);

insert into task.conversation_brief_revision(conversation_id, revision, content, confirmed_by, created_at)
select id, 0, '', owner_id, created_at from task.conversation;
