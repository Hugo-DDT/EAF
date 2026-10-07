-- 集群准入配置及持久工作区轮转位置；启动节点先校验共享值，不用环境配置互相覆盖。
create table task.cluster_capacity_config (
    singleton_id smallint primary key check (singleton_id = 1),
    queued_limit integer not null check (queued_limit > 0),
    workspace_queued_limit integer not null check (workspace_queued_limit > 0 and workspace_queued_limit <= queued_limit),
    fair_dispatch_enabled boolean not null,
    revision bigint not null default 1 check (revision > 0),
    updated_at timestamptz not null default now()
);

create sequence task.dispatch_turn_seq;

create table task.dispatch_scope (
    tenant_id uuid not null,
    workspace_id uuid not null,
    last_turn bigint not null default 0,
    candidate_created_at timestamptz,
    candidate_id uuid,
    updated_at timestamptz not null default now(),
    primary key (tenant_id, workspace_id),
    check ((candidate_created_at is null) = (candidate_id is null))
);

insert into task.dispatch_scope(tenant_id, workspace_id)
select tenant_id, workspace_id from task.task
where status = 'QUEUED' and source = 'USER'
group by tenant_id, workspace_id;

create index task_user_dispatch_candidates
    on task.task(tenant_id, workspace_id, created_at, id)
    where status = 'QUEUED' and source = 'USER';

create index task_queued_workspace
    on task.task(tenant_id, workspace_id)
    where status = 'QUEUED';
