create table agent.prompt_variant_origin (
    tenant_id uuid not null,
    workspace_id uuid not null,
    candidate_id uuid not null,
    candidate_revision integer not null check (candidate_revision > 0),
    adoption_id uuid not null,
    owner_id uuid not null,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    base_agent_id uuid not null,
    base_agent_version varchar(40) not null,
    prompt_id uuid not null,
    prompt_version varchar(40) not null,
    prompt_hash char(64) not null,
    approval_id uuid not null,
    report_id uuid not null,
    report_hash char(64) not null,
    status varchar(20) not null check (status in ('PUBLISHED','REVOKED')),
    created_at timestamptz not null default now(),
    primary key (tenant_id, workspace_id, candidate_id, candidate_revision, adoption_id),
    unique (tenant_id, workspace_id, agent_id, agent_version),
    foreign key (agent_id, tenant_id, workspace_id, agent_version)
        references agent.version(id, tenant_id, workspace_id, asset_version)
);
