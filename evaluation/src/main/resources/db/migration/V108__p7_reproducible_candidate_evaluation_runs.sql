alter table evaluation.candidate_eval_run add column started_by uuid;
alter table evaluation.candidate_eval_run add column idempotency_key varchar(200);
alter table evaluation.candidate_eval_run add column purpose varchar(60) not null default 'LEARNING_CANDIDATE_HELD_OUT';
alter table evaluation.candidate_eval_run add column source varchar(20) not null default 'EVALUATION';
alter table evaluation.candidate_eval_run add column stop_requested boolean not null default false;
alter table evaluation.candidate_eval_run add column lease_owner uuid;
alter table evaluation.candidate_eval_run add column lease_until timestamptz;
alter table evaluation.candidate_eval_run add column failed_samples integer not null default 0;
alter table evaluation.candidate_eval_run add column unknown_usage_samples integer not null default 0;
alter table evaluation.candidate_eval_run add column candidate_accuracy_variance numeric(12, 8);
alter table evaluation.candidate_eval_run add column latency_p50_ms bigint;
alter table evaluation.candidate_eval_run add column latency_p95_ms bigint;
update evaluation.candidate_eval_run set started_by = tenant_id, idempotency_key = id::text;
alter table evaluation.candidate_eval_run alter column started_by set not null;
alter table evaluation.candidate_eval_run alter column idempotency_key set not null;
alter table evaluation.candidate_eval_run drop constraint candidate_eval_run_status_check;
alter table evaluation.candidate_eval_run add constraint candidate_eval_run_status_check
    check (status in ('RUNNING', 'STOP_REQUESTED', 'STOPPED', 'PASSED', 'FAILED', 'INCOMPLETE'));
alter table evaluation.candidate_eval_run add constraint candidate_eval_run_failed_samples_check
    check (failed_samples >= 0 and unknown_usage_samples >= 0);
create unique index candidate_eval_run_idempotency_idx
    on evaluation.candidate_eval_run(tenant_id, workspace_id, started_by, idempotency_key);

-- 成对 Task 按质量运行、用例、重复号唯一化，进程恢复时复用相同 Task 与 Usage 键。
alter table evaluation.candidate_pair_run add column quality_run_id uuid;
alter table evaluation.candidate_pair_run add column case_id varchar(80);
alter table evaluation.candidate_pair_run add column sample_no integer;
create unique index candidate_pair_run_sample_idx
    on evaluation.candidate_pair_run(quality_run_id, case_id, sample_no)
    where quality_run_id is not null and case_id is not null and sample_no is not null;
alter table evaluation.candidate_eval_sample add column elapsed_ms bigint not null default 0;
alter table evaluation.candidate_eval_sample add column baseline_execution_evidence jsonb not null default '{}'::jsonb;
alter table evaluation.candidate_eval_sample add column candidate_execution_evidence jsonb not null default '{}'::jsonb;

-- 开发集与保留集分表；本迁移不把保留集复制进开发集，避免评测答案泄漏。
create table evaluation.candidate_dev_case (
    dataset_version varchar(40) not null,
    case_id varchar(80) not null,
    cohort varchar(20) not null check (cohort in ('TARGET', 'NON_TARGET', 'SAFETY')),
    input_text text not null check (length(input_text) between 1 and 8000),
    primary key (dataset_version, case_id)
);

create table evaluation.candidate_dev_answer (
    dataset_version varchar(40) not null,
    case_id varchar(80) not null,
    expected_risk varchar(20) not null check (expected_risk in ('LOW', 'MEDIUM', 'HIGH', 'UNKNOWN')),
    primary key (dataset_version, case_id),
    foreign key (dataset_version, case_id) references evaluation.candidate_dev_case(dataset_version, case_id)
);

-- 模糊事实与引用语义由独立人员复核；人工决定作为证据保存，不覆盖冻结评分结果。
create table evaluation.candidate_eval_manual_review (
    run_id uuid not null references evaluation.candidate_eval_run(id),
    case_id varchar(80) not null,
    sample_no integer not null check (sample_no between 1 and 3),
    reviewer_id uuid not null,
    factual_decision varchar(20) not null check (factual_decision in ('CONFIRMED', 'REJECTED', 'AMBIGUOUS')),
    citation_semantic_support varchar(20) not null check (citation_semantic_support in ('SUPPORTED', 'UNSUPPORTED', 'NOT_APPLICABLE', 'AMBIGUOUS')),
    rationale varchar(2000) not null,
    created_at timestamptz not null,
    primary key (run_id, case_id, sample_no, reviewer_id)
);

-- 通用质量运行注册表给获授权的 USER 集成验收链分配服务器生成且不可伪造的用途标记。
create table evaluation.quality_run_registration (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    idempotency_key varchar(200) not null,
    purpose varchar(60) not null check (purpose in ('CRM_INTEGRATION_ACCEPTANCE', 'MODEL_PROVIDER_SMOKE', 'RAG_HELD_OUT')),
    source varchar(20) not null check (source in ('USER', 'EVALUATION')),
    request_hash char(64) not null,
    status varchar(20) not null check (status in ('REGISTERED', 'RUNNING', 'COMPLETED', 'FAILED')),
    created_at timestamptz not null,
    unique (tenant_id, workspace_id, owner_id, idempotency_key)
);
