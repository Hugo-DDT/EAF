-- UNKNOWN 核验命令由 Execution Owner 记账，并固定到原 operationId。
create table execution.operations_command (
    command_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    execution_id uuid not null references execution.execution(id),
    operation_id uuid not null,
    actor_id uuid not null,
    request_key_hash char(64) not null,
    request_hash char(64) not null,
    reason varchar(500) not null,
    state varchar(20) not null check (state in ('PENDING', 'COMPLETED')),
    result_status varchar(30),
    result_version bigint,
    created_at timestamptz not null,
    completed_at timestamptz,
    unique (tenant_id, workspace_id, request_key_hash),
    check ((state = 'PENDING' and completed_at is null)
        or (state = 'COMPLETED' and completed_at is not null and result_status is not null and result_version is not null))
);
create index execution_operations_command_target_idx
    on execution.operations_command(tenant_id, workspace_id, execution_id, created_at desc);
comment on table execution.operations_command is 'Execution Owner 的只读 UNKNOWN 核验命令与可恢复回执；不记录写入正文。';

-- Outbox 重放与核验共用 Execution 运维命令域，但独立账本保留不同目标类型及历史结果。
create table execution.outbox_replay_command (
    command_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    event_id uuid not null references execution.outbox(event_id),
    actor_id uuid not null,
    request_key_hash char(64) not null,
    request_hash char(64) not null,
    reason varchar(500) not null,
    result_status varchar(20) not null check (result_status = 'PENDING'),
    attempts integer not null check (attempts >= 0),
    created_at timestamptz not null,
    unique (tenant_id, workspace_id, request_key_hash)
);
