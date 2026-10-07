-- 本迁移为 Workspace 新任务闸门保存可重放的运维命令和状态版本。
create table workspace.operational_gate (
    tenant_id uuid not null,
    workspace_id uuid not null,
    gate_name varchar(40) not null check (gate_name in ('TASK_ADMISSION')),
    enabled boolean not null,
    changed_by uuid,
    changed_at timestamptz not null,
    row_version bigint not null check (row_version > 0),
    command_id uuid,
    primary key (tenant_id, workspace_id, gate_name),
    foreign key (tenant_id, workspace_id) references workspace.workspace(tenant_id, id)
);

create table workspace.operations_command (
    command_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    actor_id uuid not null,
    request_key_hash char(64) not null,
    request_hash char(64) not null,
    gate_name varchar(40) not null check (gate_name in ('TASK_ADMISSION')),
    enabled boolean not null,
    reason varchar(500) not null,
    applied_version bigint not null default 0 check (applied_version >= 0),
    created_at timestamptz not null,
    unique (tenant_id, workspace_id, request_key_hash),
    foreign key (tenant_id, workspace_id) references workspace.workspace(tenant_id, id)
);

-- 既有 Workspace 默认开放；只有运维命令可写入显式停止状态。
insert into workspace.operational_gate(tenant_id, workspace_id, gate_name, enabled, changed_by, changed_at, row_version, command_id)
select tenant_id, id, 'TASK_ADMISSION', true, null, now(), 1, null
from workspace.workspace;
