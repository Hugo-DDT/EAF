create table workflow.project_brief_state (
    instance_id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    evidence_bundle_hash char(64) not null check (evidence_bundle_hash ~ '^[0-9a-f]{64}$'),
    generation_attempted boolean not null default false,
    generation_task_id uuid,
    generation_attempt integer,
    prepared_result_json jsonb check (prepared_result_json is null or jsonb_typeof(prepared_result_json) = 'object'),
    created_at timestamptz not null default now(),
    foreign key (instance_id, tenant_id, workspace_id)
        references workflow.instance(id, tenant_id, workspace_id),
    check ((generation_task_id is null and generation_attempt is null)
        or (generation_task_id is not null and generation_attempt > 0))
);

create table workflow.project_brief_artifact (
    instance_id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    artifact_version integer not null check (artifact_version in (1, 2)),
    artifact_kind varchar(24) not null check (artifact_kind in ('GENERATED', 'HUMAN_REVISION')),
    template_version varchar(40) not null,
    generation_task_id uuid not null,
    generation_attempt integer not null check (generation_attempt > 0),
    evidence_bundle_hash char(64) not null check (evidence_bundle_hash ~ '^[0-9a-f]{64}$'),
    agent_id uuid not null,
    agent_version varchar(40) not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    author_id uuid not null,
    markdown text not null check (octet_length(markdown) between 1 and 65536),
    content_hash char(64) not null check (content_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz not null default now(),
    primary key (instance_id, artifact_version),
    foreign key (instance_id, tenant_id, workspace_id)
        references workflow.instance(id, tenant_id, workspace_id)
);

create index project_brief_artifact_created_idx
    on workflow.project_brief_artifact(tenant_id, workspace_id, created_at desc, instance_id);
