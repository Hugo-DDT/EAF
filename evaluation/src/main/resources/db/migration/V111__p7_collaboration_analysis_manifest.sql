alter table evaluation.quality_run_registration
    drop constraint if exists quality_run_registration_purpose_check;
alter table evaluation.quality_run_registration
    add constraint quality_run_registration_purpose_check
        check (purpose in ('CRM_INTEGRATION_ACCEPTANCE', 'MODEL_PROVIDER_SMOKE', 'RAG_HELD_OUT', 'COLLABORATION_HELD_OUT'));
alter table evaluation.quality_run_registration
    add column collaboration_manifest_id uuid;

create table evaluation.collaboration_analysis_manifest (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    manifest_version varchar(40) not null,
    purpose varchar(60) not null check (purpose = 'COLLABORATION_HELD_OUT'),
    source varchar(20) not null check (source = 'EVALUATION'),
    baseline_workflow_id uuid not null,
    baseline_workflow_version varchar(40) not null,
    baseline_step_ids text[] not null check (cardinality(baseline_step_ids) > 0),
    reviewer_workflow_id uuid not null,
    reviewer_workflow_version varchar(40) not null,
    reviewer_step_ids text[] not null check (cardinality(reviewer_step_ids) > 0),
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    created_at timestamptz not null default now(),
    unique (id, tenant_id, workspace_id),
    unique (tenant_id, workspace_id, manifest_version)
);

-- 清单内容一经登记即作为运行授权边界；变更必须创建新版本，不能改写历史运行范围。
create function evaluation.reject_collaboration_manifest_mutation()
returns trigger
language plpgsql
as $$
begin
    if tg_op = 'DELETE' then
        raise exception 'collaboration evaluation manifest cannot be deleted';
    end if;
    if new.id is distinct from old.id
       or new.tenant_id is distinct from old.tenant_id
       or new.workspace_id is distinct from old.workspace_id
       or new.manifest_version is distinct from old.manifest_version
       or new.purpose is distinct from old.purpose
       or new.source is distinct from old.source
       or new.baseline_workflow_id is distinct from old.baseline_workflow_id
       or new.baseline_workflow_version is distinct from old.baseline_workflow_version
       or new.baseline_step_ids is distinct from old.baseline_step_ids
       or new.reviewer_workflow_id is distinct from old.reviewer_workflow_id
       or new.reviewer_workflow_version is distinct from old.reviewer_workflow_version
       or new.reviewer_step_ids is distinct from old.reviewer_step_ids
       or old.status <> 'ACTIVE'
       or new.status <> 'DISABLED' then
        raise exception 'collaboration evaluation manifest is immutable except for disable';
    end if;
    return new;
end;
$$;

create trigger collaboration_analysis_manifest_immutable
    before update or delete on evaluation.collaboration_analysis_manifest
    for each row execute function evaluation.reject_collaboration_manifest_mutation();

insert into evaluation.collaboration_analysis_manifest(
    id, tenant_id, workspace_id, manifest_version, purpose, source,
    baseline_workflow_id, baseline_workflow_version, baseline_step_ids,
    reviewer_workflow_id, reviewer_workflow_version, reviewer_step_ids, status)
values (
    '25000000-0000-4000-8000-000000000101', '70000000-0000-4000-8000-000000000001',
    '10000000-0000-4000-8000-000000000001', '1.0.0', 'COLLABORATION_HELD_OUT', 'EVALUATION',
    '58000000-0000-4000-8000-000000000002', '1.0.0', array['analyze', 'complete'],
    '58000000-0000-4000-8000-000000000003', '1.0.0', array['analyze', 'peer-review', 'complete'], 'ACTIVE');

alter table evaluation.quality_run_registration
    add constraint quality_run_collaboration_manifest_fk
        foreign key (collaboration_manifest_id, tenant_id, workspace_id)
        references evaluation.collaboration_analysis_manifest(id, tenant_id, workspace_id);
alter table evaluation.quality_run_registration
    add constraint quality_run_collaboration_manifest_check
        check ((purpose = 'COLLABORATION_HELD_OUT' and source = 'EVALUATION' and collaboration_manifest_id is not null)
            or (purpose <> 'COLLABORATION_HELD_OUT' and collaboration_manifest_id is null));
