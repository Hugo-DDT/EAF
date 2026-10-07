-- Task 域持有父子关系、入口/运行类型及共享预算账本；历史 Task 各自成为根。
alter table task.task
    add column root_task_id uuid,
    add column parent_task_id uuid,
    add column entry_protocol varchar(20) not null default 'REST',
    add column run_kind varchar(30) not null default 'AGENT',
    add column tool_name varchar(80),
    add column tool_version varchar(40),
    add column tool_binding_ref varchar(120),
    add column tool_arguments_json jsonb,
    add column active_budget_reservation_key varchar(200),
    add column active_reserved_ms bigint not null default 0;

update task.task set root_task_id = id;

alter table task.task
    alter column root_task_id set not null,
    add constraint task_identity_scope_unique unique (tenant_id, workspace_id, id),
    add constraint task_parent_identity_unique unique (tenant_id, workspace_id, id, root_task_id),
    add constraint task_root_scope_fk foreign key (tenant_id, workspace_id, root_task_id)
        references task.task(tenant_id, workspace_id, id),
    add constraint task_parent_scope_fk foreign key (tenant_id, workspace_id, parent_task_id, root_task_id)
        references task.task(tenant_id, workspace_id, id, root_task_id),
    add constraint task_parent_root_check check (
        (parent_task_id is null and root_task_id = id)
        or (parent_task_id is not null and root_task_id <> id)
    ),
    add constraint task_entry_protocol_check check (entry_protocol in ('REST', 'MCP', 'A2A')),
    add constraint task_run_kind_check check (
        (run_kind = 'AGENT' and tool_name is null and tool_version is null and tool_binding_ref is null and tool_arguments_json is null)
        or (run_kind = 'TOOL_EXECUTION' and tool_name is not null and tool_version is not null and tool_binding_ref is not null and tool_arguments_json is not null)
    ),
    add constraint task_active_reservation_check check (active_reserved_ms >= 0);

create index task_parent_idx on task.task(tenant_id, workspace_id, parent_task_id, created_at);
create index task_root_idx on task.task(tenant_id, workspace_id, root_task_id, created_at);

create table task.budget_scope (
    tenant_id uuid not null,
    workspace_id uuid not null,
    root_task_id uuid primary key,
    max_steps integer not null,
    steps_used integer not null,
    max_model_calls integer not null,
    model_calls integer not null,
    max_tool_calls integer not null,
    tool_calls integer not null,
    max_tokens integer not null,
    token_used bigint not null,
    token_reserved bigint not null,
    max_active_ms bigint not null,
    active_used_ms bigint not null,
    active_reserved_ms bigint not null default 0,
    tool_executions integer not null default 0,
    created_at timestamptz not null default now(),
    constraint budget_scope_root_fk foreign key (tenant_id, workspace_id, root_task_id)
        references task.task(tenant_id, workspace_id, id),
    constraint budget_scope_nonnegative check (
        max_steps > 0 and steps_used >= 0 and max_model_calls > 0 and model_calls >= 0
        and max_tool_calls > 0 and tool_calls >= 0 and max_tokens > 0 and token_used >= 0
        and token_reserved >= 0 and max_active_ms > 0 and active_used_ms >= 0
        and active_reserved_ms >= 0 and tool_executions >= 0
    )
);

-- 迁移前的 Task 预算累计值回填到独立根账本，避免升级或 retry 重置已有消耗。
insert into task.budget_scope(tenant_id, workspace_id, root_task_id, max_steps, steps_used,
    max_model_calls, model_calls, max_tool_calls, tool_calls, max_tokens, token_used,
    token_reserved, max_active_ms, active_used_ms, tool_executions)
select tenant_id, workspace_id, id, max_steps, steps_used, 8, model_calls, 16, tool_calls,
    8000, token_used, token_reserved, 60000, active_used_ms, tool_executions
from task.task;

create table task.budget_reservation (
    root_task_id uuid not null references task.budget_scope(root_task_id),
    reservation_key varchar(200) not null,
    task_id uuid not null references task.task(id),
    attempt integer not null,
    kind varchar(16) not null check (kind in ('MODEL', 'TOOL', 'ACTIVE')),
    reserved_amount bigint not null,
    settled_amount bigint,
    state varchar(16) not null check (state in ('RESERVED', 'SETTLED')),
    created_at timestamptz not null default now(),
    settled_at timestamptz,
    primary key (root_task_id, reservation_key),
    constraint budget_reservation_amount_check check (reserved_amount >= 0 and (settled_amount is null or settled_amount >= 0)),
    constraint budget_reservation_settlement_check check (
        (state = 'RESERVED' and settled_amount is null and settled_at is null)
        or (state = 'SETTLED' and settled_amount is not null and settled_at is not null)
    )
);

create index budget_reservation_task_idx on task.budget_reservation(task_id, attempt, kind);
