create table workspace.workspace_profile (
    tenant_id uuid not null,
    workspace_id uuid not null,
    kind varchar(20) not null check (kind in ('PERSONAL', 'PROJECT', 'DEPARTMENT', 'ENTERPRISE')),
    owner_id uuid,
    parent_workspace_id uuid,
    created_by uuid not null,
    creation_hash char(64) not null,
    created_at timestamptz not null,
    primary key (tenant_id, workspace_id),
    unique (tenant_id, workspace_id, kind),
    foreign key (tenant_id, workspace_id) references workspace.workspace(tenant_id, id),
    foreign key (tenant_id, parent_workspace_id) references workspace.workspace(tenant_id, id),
    check ((kind = 'PERSONAL' and owner_id is not null and parent_workspace_id is null)
        or (kind <> 'PERSONAL' and owner_id is null)),
    check ((kind in ('PERSONAL', 'ENTERPRISE') and parent_workspace_id is null)
        or kind in ('PROJECT', 'DEPARTMENT'))
);

create unique index workspace_personal_owner_unique
    on workspace.workspace_profile(tenant_id, owner_id) where kind = 'PERSONAL';

create table workspace.context_source_preference (
    tenant_id uuid not null,
    target_workspace_id uuid not null,
    actor_id uuid not null,
    source_workspace_id uuid not null,
    position smallint not null check (position between 1 and 3),
    primary key (tenant_id, target_workspace_id, actor_id, source_workspace_id),
    unique (tenant_id, target_workspace_id, actor_id, position),
    foreign key (tenant_id, target_workspace_id) references workspace.workspace(tenant_id, id),
    foreign key (tenant_id, source_workspace_id) references workspace.workspace(tenant_id, id),
    check (target_workspace_id <> source_workspace_id)
);

create index context_source_preference_actor_idx
    on workspace.context_source_preference(tenant_id, actor_id, target_workspace_id);
