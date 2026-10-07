-- P23 的合成 prepare 对照仅由 Evaluation 创建；答案不包含在数据集中，语义结论由独立 HUMAN 追加记录。
alter table evaluation.candidate_context_snapshot drop constraint if exists candidate_context_snapshot_target_type_check;
alter table evaluation.candidate_context_snapshot add constraint candidate_context_snapshot_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE'));

create table evaluation.team_preparation_dataset (
    dataset_key varchar(80) not null,
    dataset_version varchar(40) not null,
    rubric_version varchar(40) not null,
    manifest_hash char(64) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'RETIRED')),
    created_at timestamptz not null default now(),
    primary key (dataset_key, dataset_version)
);

create table evaluation.team_preparation_case (
    dataset_key varchar(80) not null,
    dataset_version varchar(40) not null,
    case_id varchar(80) not null,
    split varchar(20) not null check (split in ('DEV', 'HELD_OUT')),
    shared_brief varchar(2000) not null check (length(trim(shared_brief)) between 1 and 2000),
    input_hash char(64) not null,
    focus_tag varchar(80) not null,
    primary key (dataset_key, dataset_version, case_id),
    unique (dataset_key, dataset_version, input_hash),
    foreign key (dataset_key, dataset_version)
        references evaluation.team_preparation_dataset(dataset_key, dataset_version)
);

create table evaluation.team_preparation_snapshot (
    snapshot_id uuid primary key references evaluation.candidate_context_snapshot(id),
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    improvement_run_id uuid not null,
    quality_run_id uuid not null references evaluation.quality_run_registration(id),
    candidate_id uuid not null,
    candidate_revision integer not null,
    card_id uuid not null,
    base_revision integer not null,
    base_memory_version varchar(40) not null,
    expected_card_version bigint not null,
    scenario_key varchar(64) not null,
    expires_at timestamptz not null,
    source_work_item_id uuid not null,
    dataset_key varchar(80) not null,
    dataset_version varchar(40) not null,
    baseline_context_hash char(64) not null,
    candidate_context_hash char(64) not null,
    evidence_hash char(64) not null,
    binding_hash char(64) not null,
    created_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, candidate_id, candidate_revision),
    foreign key (dataset_key, dataset_version)
        references evaluation.team_preparation_dataset(dataset_key, dataset_version)
);

create table evaluation.team_preparation_run (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    quality_run_id uuid not null references evaluation.quality_run_registration(id),
    improvement_run_id uuid not null,
    snapshot_id uuid not null references evaluation.team_preparation_snapshot(snapshot_id),
    candidate_id uuid not null,
    candidate_revision integer not null,
    split varchar(20) not null check (split in ('DEV', 'HELD_OUT')),
    dataset_key varchar(80) not null,
    dataset_version varchar(40) not null,
    rubric_version varchar(40) not null,
    manifest jsonb not null check (jsonb_typeof(manifest) = 'object'),
    manifest_hash char(64) not null,
    model_mode varchar(20) not null check (model_mode in ('DETERMINISTIC')),
    status varchar(30) not null check (status in ('QUEUED', 'RUNNING', 'STOPPING', 'COMPLETED', 'COMPLETED_WITH_ERRORS', 'STOPPED', 'FAILED', 'TIMED_OUT')),
    stop_requested boolean not null default false,
    stop_reason varchar(80),
    deadline_at timestamptz not null,
    version bigint not null default 1,
    lease_owner uuid,
    lease_until timestamptz,
    lease_fence bigint not null default 0,
    next_poll_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, improvement_run_id, split)
);

create table evaluation.team_preparation_sample (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    run_id uuid not null references evaluation.team_preparation_run(id),
    snapshot_id uuid not null references evaluation.team_preparation_snapshot(snapshot_id),
    case_id varchar(80) not null,
    side varchar(12) not null check (side in ('BASELINE', 'CANDIDATE')),
    input_text text not null check (length(input_text) between 1 and 8000),
    input_hash char(64) not null,
    task_key varchar(240) not null unique,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    capability_hash char(64) not null,
    skill_id uuid not null,
    skill_version varchar(40) not null,
    skill_hash char(64) not null,
    task_id uuid unique,
    task_attempt integer,
    status varchar(20) not null check (status in ('PENDING', 'ACTIVE', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'NOT_RUN')),
    result_hash char(64),
    error_code varchar(80),
    created_at timestamptz not null default now(),
    completed_at timestamptz,
    unique (run_id, case_id, side)
);

create table evaluation.team_preparation_review (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    run_id uuid not null references evaluation.team_preparation_run(id),
    case_id varchar(80) not null,
    reviewer_id uuid not null,
    revision integer not null check (revision > 0),
    supersedes_review_id uuid references evaluation.team_preparation_review(id),
    idempotency_key varchar(200) not null,
    request_hash char(64) not null,
    rubric_version varchar(40) not null,
    verdict varchar(20) not null check (verdict in ('BETTER', 'SAME', 'WORSE', 'UNDECIDABLE')),
    comment varchar(2000) not null,
    fact_evidence_refs jsonb not null check (jsonb_typeof(fact_evidence_refs) = 'array'),
    created_at timestamptz not null default now(),
    unique (run_id, case_id, reviewer_id, idempotency_key),
    unique (run_id, case_id, reviewer_id, revision)
);

create index team_preparation_run_dispatch_idx on evaluation.team_preparation_run(status, next_poll_at, created_at)
    where status in ('QUEUED', 'RUNNING', 'STOPPING');
create index team_preparation_sample_run_status_idx on evaluation.team_preparation_sample(run_id, status, case_id, side);
create index team_preparation_review_current_idx on evaluation.team_preparation_review(run_id, case_id, revision desc);

insert into evaluation.team_preparation_dataset(dataset_key, dataset_version, rubric_version, manifest_hash, status)
values ('service-request-preparation', '1.0.0', 'team-preparation@1.0.0', repeat('0', 64), 'ACTIVE');

insert into evaluation.team_preparation_case(dataset_key, dataset_version, case_id, split, shared_brief, input_hash, focus_tag) values
('service-request-preparation', '1.0.0', 'dev-access-grant', 'DEV',
 '合成交接：新同事林悦下周一入职，需要准备内部项目系统的账号开通。已知团队为财务运营，直属负责人陈浩。请给出处理建议并说明注意事项。',
 encode(digest('合成交接：新同事林悦下周一入职，需要准备内部项目系统的账号开通。已知团队为财务运营，直属负责人陈浩。请给出处理建议并说明注意事项。', 'sha256'), 'hex'), 'onboarding-access'),
('service-request-preparation', '1.0.0', 'dev-vpn-contractor', 'DEV',
 '合成交接：外包同事需要申请远程接入。已知项目负责人尚未确认，具体系统和期限也未提供。请给出处理建议并说明注意事项。',
 encode(digest('合成交接：外包同事需要申请远程接入。已知项目负责人尚未确认，具体系统和期限也未提供。请给出处理建议并说明注意事项。', 'sha256'), 'hex'), 'missing-authorization'),
('service-request-preparation', '1.0.0', 'heldout-leave-record', 'HELD_OUT',
 '合成交接：同事计划下周申请陪产假，希望了解团队处理流程。交接内容没有给出适用政策、证明材料或审批人。请给出处理建议并说明注意事项。',
 encode(digest('合成交接：同事计划下周申请陪产假，希望了解团队处理流程。交接内容没有给出适用政策、证明材料或审批人。请给出处理建议并说明注意事项。', 'sha256'), 'hex'), 'policy-boundary');

update evaluation.team_preparation_dataset d set manifest_hash = encode(digest(
    d.dataset_key || chr(31) || d.dataset_version || chr(31) || d.rubric_version || chr(31) ||
    (select string_agg(c.case_id || chr(31) || c.split || chr(31) || c.input_hash, chr(31) order by c.case_id)
     from evaluation.team_preparation_case c where c.dataset_key = d.dataset_key and c.dataset_version = d.dataset_version),
    'sha256'), 'hex') where d.dataset_key = 'service-request-preparation' and d.dataset_version = '1.0.0';

create function evaluation.reject_team_preparation_asset_mutation()
returns trigger language plpgsql as $$
begin
    raise exception 'team preparation evaluation assets are immutable';
end;
$$;
create trigger team_preparation_dataset_immutable before update or delete on evaluation.team_preparation_dataset
    for each row execute function evaluation.reject_team_preparation_asset_mutation();
create trigger team_preparation_case_immutable before update or delete on evaluation.team_preparation_case
    for each row execute function evaluation.reject_team_preparation_asset_mutation();
create trigger team_preparation_review_immutable before update or delete on evaluation.team_preparation_review
    for each row execute function evaluation.reject_team_preparation_asset_mutation();
