-- Connector Owner 维护独立状态版本与停用命令账本，避免 Workspace 或协议层直接改连接表。
alter table connector.instance add column row_version bigint not null default 1 check (row_version > 0);
alter table connector.instance add constraint connector_instance_id_scope_unique unique (id, tenant_id, workspace_id);
comment on column connector.instance.row_version is 'Connector 配置及状态的 Owner 版本；受控运维改变状态时递增。';

create table connector.operations_command (
    command_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    connector_id uuid not null,
    actor_id uuid not null,
    request_key_hash char(64) not null,
    request_hash char(64) not null,
    reason varchar(500) not null,
    result_status varchar(20) not null check (result_status in ('DISABLED', 'REVOKED')),
    applied_version bigint not null default 0 check (applied_version >= 0),
    created_at timestamptz not null,
    unique (tenant_id, workspace_id, request_key_hash),
    foreign key (tenant_id, workspace_id) references workspace.workspace(tenant_id, id),
    foreign key (connector_id, tenant_id, workspace_id)
        references connector.instance(id, tenant_id, workspace_id)
);
create index connector_operations_command_target_idx
    on connector.operations_command(tenant_id, workspace_id, connector_id, created_at desc);
comment on table connector.operations_command is 'Connector Owner 的 HUMAN 停用命令、幂等结果和受控原因。';
