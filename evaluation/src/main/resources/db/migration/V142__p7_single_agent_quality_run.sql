alter table evaluation.eval_run
    add column if not exists started_by uuid,
    add column if not exists configuration_hash char(64),
    add column if not exists manifest_snapshot jsonb,
    add column if not exists lease_owner uuid,
    add column if not exists lease_until timestamptz,
    add column if not exists stop_requested boolean not null default false,
    add column if not exists failed_samples integer not null default 0,
    add column if not exists unknown_usage_samples integer not null default 0,
    add column if not exists input_tokens bigint,
    add column if not exists output_tokens bigint,
    add column if not exists estimated_cost numeric(20, 8),
    add column if not exists cost_status varchar(30),
    add column if not exists cost_currency varchar(3),
    add column if not exists actual_cost numeric(20, 8),
    add column if not exists actual_cost_currency varchar(3),
    add column if not exists billing_status varchar(30),
    add column if not exists latency_p50_ms bigint,
    add column if not exists latency_p95_ms bigint,
    add column if not exists failure_reason varchar(80);

alter table evaluation.eval_result
    add column if not exists outcome_status varchar(30) not null default 'SUCCEEDED',
    add column if not exists model_calls integer not null default 0;

create index if not exists eval_run_active_lease_idx
    on evaluation.eval_run (lease_until)
    where lease_owner is not null;
