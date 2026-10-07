-- Memory Owner 记录人工重放命令；事件仍由 Memory 发布器按原 eventId 投递。
create table memory.outbox_replay_command (
    command_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    event_id uuid not null references memory.outbox(event_id),
    actor_id uuid not null,
    request_key_hash char(64) not null,
    request_hash char(64) not null,
    reason varchar(500) not null,
    result_status varchar(20) not null check (result_status = 'PENDING'),
    attempts integer not null check (attempts >= 0),
    created_at timestamptz not null,
    unique (tenant_id, workspace_id, request_key_hash)
);
