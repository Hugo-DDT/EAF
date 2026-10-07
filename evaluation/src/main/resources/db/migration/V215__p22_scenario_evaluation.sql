create table evaluation.scenario_dataset (
    dataset_key varchar(80) not null,
    dataset_version varchar(40) not null,
    scene_key varchar(80) not null,
    workspace_id uuid not null,
    scoring_version varchar(40) not null,
    status varchar(20) not null check (status in ('ACTIVE', 'RETIRED')),
    manifest_hash char(64) not null,
    created_at timestamptz not null default now(),
    primary key (dataset_key, dataset_version)
);

create table evaluation.scenario_case (
    dataset_key varchar(80) not null,
    dataset_version varchar(40) not null,
    case_id varchar(80) not null,
    split varchar(20) not null check (split in ('DEV', 'HELD_OUT')),
    request_text text not null,
    input_hash char(64) not null,
    tags jsonb not null default '[]'::jsonb check (jsonb_typeof(tags) = 'array'),
    primary key (dataset_key, dataset_version, case_id),
    unique (dataset_key, dataset_version, input_hash),
    foreign key (dataset_key, dataset_version)
        references evaluation.scenario_dataset(dataset_key, dataset_version)
);

create table evaluation.scenario_answer (
    dataset_key varchar(80) not null,
    dataset_version varchar(40) not null,
    case_id varchar(80) not null,
    expected_category varchar(20),
    expected_outcome varchar(30) not null,
    expected_evidence_refs jsonb not null default '[]'::jsonb check (jsonb_typeof(expected_evidence_refs) = 'array'),
    answer_hash char(64) not null,
    primary key (dataset_key, dataset_version, case_id),
    foreign key (dataset_key, dataset_version, case_id)
        references evaluation.scenario_case(dataset_key, dataset_version, case_id),
    check (expected_category is null or expected_category in ('IT', 'FACILITIES', 'HR', 'OTHER')),
    check (expected_outcome is null or expected_outcome in ('READY', 'NEEDS_INPUT', 'INSUFFICIENT_EVIDENCE'))
);

create table evaluation.scenario_run (
    id uuid primary key references evaluation.quality_run_registration(id),
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    idempotency_key varchar(200) not null,
    request_hash char(64) not null,
    dataset_key varchar(80) not null,
    dataset_version varchar(40) not null,
    split varchar(20) not null,
    mode varchar(20) not null check (mode in ('SINGLE', 'PAIRED')),
    baseline_capability_id uuid not null,
    baseline_capability_version varchar(40) not null,
    comparison_capability_id uuid,
    comparison_capability_version varchar(40),
    manifest jsonb not null,
    manifest_hash char(64) not null,
    status varchar(30) not null check (status in ('QUEUED', 'RUNNING', 'STOPPING', 'COMPLETED', 'COMPLETED_WITH_ERRORS', 'STOPPED', 'FAILED', 'TIMED_OUT')),
    stop_reason varchar(80),
    stop_requested boolean not null default false,
    version bigint not null default 1,
    lease_fence bigint not null default 0,
    deadline_at timestamptz not null,
    next_poll_at timestamptz not null default now(),
    lease_owner uuid,
    lease_until timestamptz,
    report_snapshot jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, owner_id, idempotency_key),
    foreign key (dataset_key, dataset_version)
        references evaluation.scenario_dataset(dataset_key, dataset_version),
    check ((mode = 'SINGLE' and comparison_capability_id is null and comparison_capability_version is null)
        or (mode = 'PAIRED' and comparison_capability_id is not null and comparison_capability_version is not null))
);

create table evaluation.scenario_sample (
    id uuid primary key,
    run_id uuid not null references evaluation.scenario_run(id),
    case_id varchar(80) not null,
    side varchar(20) not null check (side in ('BASELINE', 'COMPARISON')),
    input_hash char(64) not null,
    task_key varchar(240) not null unique,
    task_id uuid unique,
    task_attempt integer,
    agent_id uuid not null,
    agent_version varchar(40) not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    capability_hash char(64) not null,
    skill_id uuid not null,
    skill_version varchar(40) not null,
    skill_hash char(64) not null,
    status varchar(20) not null check (status in ('PENDING', 'ACTIVE', 'SCORED', 'ERROR', 'NOT_RUN', 'CANCELLED', 'TIMED_OUT')),
    result_hash char(64),
    scores jsonb,
    error_code varchar(80),
    queue_ms bigint,
    execution_ms bigint,
    completed_at timestamptz,
    unique (run_id, case_id, side)
);

create table evaluation.scenario_review (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    run_id uuid not null references evaluation.scenario_run(id),
    sample_id uuid not null references evaluation.scenario_sample(id),
    reviewer_id uuid not null,
    revision integer not null,
    supersedes_review_id uuid references evaluation.scenario_review(id),
    idempotency_key varchar(200) not null,
    request_hash char(64) not null,
    task_attempt integer not null,
    result_hash char(64) not null,
    rubric_version varchar(40) not null,
    verdict varchar(20) not null check (verdict in ('CORRECT', 'INCORRECT', 'UNCERTAIN')),
    issue_type varchar(20) not null check (issue_type in ('CATEGORY', 'OUTCOME', 'CITATION', 'ADVICE', 'OTHER')),
    comment varchar(2000) not null,
    self_reviewed boolean not null,
    created_at timestamptz not null default now(),
    unique (run_id, sample_id, reviewer_id, idempotency_key),
    unique (run_id, sample_id, reviewer_id, revision)
);

create index scenario_run_dispatch_idx on evaluation.scenario_run(status, next_poll_at, created_at)
    where status in ('QUEUED', 'RUNNING', 'STOPPING');
create index scenario_sample_run_status_idx on evaluation.scenario_sample(run_id, status, case_id, side);
create index scenario_review_current_idx on evaluation.scenario_review(run_id, sample_id, reviewer_id, revision desc);

alter table evaluation.quality_run_registration
    drop constraint if exists quality_run_registration_purpose_check;
alter table evaluation.quality_run_registration
    add constraint quality_run_registration_purpose_check
    check (purpose in ('CRM_INTEGRATION_ACCEPTANCE', 'MODEL_PROVIDER_SMOKE', 'RAG_HELD_OUT',
                       'COLLABORATION_HELD_OUT', 'SERVICE_REQUEST_SCENARIO'));

-- P22 的本地样例复用已发布 P15 只读能力所在的合成工作区；其他 Workspace 需显式部署版本化数据集。
insert into evaluation.scenario_dataset(dataset_key, dataset_version, scene_key, workspace_id,
                                         scoring_version, status, manifest_hash)
values ('service-request-synthetic', '1.0.0', 'service-request-analysis',
        '10000000-0000-4000-8000-000000000001', 'service-request-rubric-v1', 'ACTIVE',
        '9c5e03dd7860ca97d2d6f54ae74689421e156f6737c77db4eea7cf1e36d98d62');

insert into evaluation.scenario_case(dataset_key, dataset_version, case_id, split, request_text, input_hash, tags) values
 ('service-request-synthetic', '1.0.0', 'dev-it-access', 'DEV', '新员工无法访问项目管理系统，请确认应如何开通账号。',
  'a6084134ddaf0c3c4f60865fa2cd54b1717792264a82aed6f25562a881115f6a', '["account-access"]'),
 ('service-request-synthetic', '1.0.0', 'dev-facility-light', 'DEV', '会议室 B201 的顶灯闪烁，影响会议，请协助处理。',
  'b11384693e1cc2f2d9d9783c723140091ed2ee9fa306b49d796887a6036ff761', '["facilities"]'),
 ('service-request-synthetic', '1.0.0', 'dev-needs-owner', 'DEV', '我需要访问一个系统，账号好像不行。',
  'c2ef0cbf488eb8f16d874128c81c30bc41f495d435738b4823ef75e95d0a7823', '["clarification"]'),
 ('service-request-synthetic', '1.0.0', 'dev-no-evidence', 'DEV', '请核实天王星基地的量子导航设备采购合同编号。',
  '4f7a970369501d5a867c6327a1ffe5b6436d8f75f4a63ba31979b085b51d35a4', '["unsupported-request"]'),
 ('service-request-synthetic', '1.0.0', 'heldout-hr-leave', 'HELD_OUT', '请说明新增育儿假申请需要哪些材料。',
  '27a317838f19b5f5e5508aa88f879cadf36bd82131300a0a8156b1122d03a0d7', '["leave-policy"]');

insert into evaluation.scenario_answer(dataset_key, dataset_version, case_id, expected_category, expected_outcome,
                                      expected_evidence_refs, answer_hash) values
 ('service-request-synthetic', '1.0.0', 'dev-it-access', 'IT', 'READY', '[]',
  'bd5cd327a1f43de36120843e933c3f41cbe301706c558e60b52ad6fbfdeede9c'),
 ('service-request-synthetic', '1.0.0', 'dev-facility-light', 'FACILITIES', 'READY', '[]',
  '33ddf39defd125ce044b6956f1a5f18475bfc757143bfc1dbab4dbdd3c391c3b'),
 ('service-request-synthetic', '1.0.0', 'dev-needs-owner', null, 'NEEDS_INPUT', '[]',
  '8838cd657686d6c89b681c24c3c87fbe13141ea86aad6fb866acf4fc586a1637'),
 ('service-request-synthetic', '1.0.0', 'dev-no-evidence', 'OTHER', 'INSUFFICIENT_EVIDENCE', '[]',
  '38b577160b9f66f63fe6f7b1ec0c91cf67d6551d0d10683793642c7263003090'),
 ('service-request-synthetic', '1.0.0', 'heldout-hr-leave', 'HR', 'READY', '[]',
  'b7ac37d23259e0e0157db98c3e10b7079eb85c86876b6af6095bf67a2592ee5c');

create function evaluation.reject_scenario_asset_mutation()
returns trigger language plpgsql as $$
begin
    if tg_op = 'DELETE' then raise exception 'scenario assets are immutable'; end if;
    if tg_table_name in ('scenario_case', 'scenario_answer') then
        raise exception 'scenario cases and answers are immutable; create a new version';
    end if;
    if new.dataset_key is distinct from old.dataset_key
       or new.dataset_version is distinct from old.dataset_version
       or new.scene_key is distinct from old.scene_key
       or new.workspace_id is distinct from old.workspace_id
       or new.scoring_version is distinct from old.scoring_version
       or new.manifest_hash is distinct from old.manifest_hash
       or old.status <> 'ACTIVE' or new.status <> 'RETIRED' then
        raise exception 'scenario dataset is immutable except for retirement';
    end if;
    return new;
end;
$$;

create trigger scenario_dataset_immutable before update or delete on evaluation.scenario_dataset
    for each row execute function evaluation.reject_scenario_asset_mutation();
create trigger scenario_case_immutable before update or delete on evaluation.scenario_case
    for each row execute function evaluation.reject_scenario_asset_mutation();
create trigger scenario_answer_immutable before update or delete on evaluation.scenario_answer
    for each row execute function evaluation.reject_scenario_asset_mutation();

create function evaluation.reject_scenario_review_mutation()
returns trigger language plpgsql as $$
begin
    raise exception 'scenario reviews are append-only';
end;
$$;

create trigger scenario_review_immutable before update or delete on evaluation.scenario_review
    for each row execute function evaluation.reject_scenario_review_mutation();

