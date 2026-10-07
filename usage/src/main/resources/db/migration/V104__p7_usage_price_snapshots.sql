-- P7 Usage 以稳定调用键归并 Provider/Runtime 事实，并为每次估算固定价格版本与单价。
alter table usage.model_usage alter column task_id drop not null;
alter table usage.model_usage alter column run_id drop not null;

alter table usage.model_usage add column call_key varchar(240);
update usage.model_usage set call_key = 'task:' || task_id || ':run:' || run_id || ':call:' || call_no;
alter table usage.model_usage alter column call_key set not null;
alter table usage.model_usage add constraint model_usage_call_key_unique unique (call_key);

alter table usage.model_usage add column call_type varchar(30) not null default 'CHAT';
alter table usage.model_usage add constraint model_usage_call_type_check
    check (call_type in ('CHAT', 'EMBEDDING_QUERY', 'EMBEDDING_INDEX', 'EMBEDDING_BATCH', 'PEER'));
alter table usage.model_usage add column scope_type varchar(30);
alter table usage.model_usage add column scope_id uuid;
update usage.model_usage set scope_type = case when source = 'EVALUATION' then 'EVALUATION' else 'TASK' end,
                             scope_id = task_id;
alter table usage.model_usage alter column scope_type set not null;
alter table usage.model_usage alter column scope_id set not null;
alter table usage.model_usage add constraint model_usage_scope_type_check
    check (scope_type in ('TASK', 'EVALUATION', 'WORKFLOW', 'JOB'));

alter table usage.model_usage add column price_version varchar(80);
alter table usage.model_usage add column price_source_version varchar(120);
alter table usage.model_usage add column price_effective_at timestamptz;
alter table usage.model_usage add column billing_unit varchar(20);
alter table usage.model_usage add column input_price_per_million numeric(18, 8);
alter table usage.model_usage add column output_price_per_million numeric(18, 8);
alter table usage.model_usage add column request_price numeric(18, 8);
alter table usage.model_usage add column actual_cost numeric(18, 8);
alter table usage.model_usage add column actual_cost_currency varchar(3);
alter table usage.model_usage add column billing_source varchar(120);
alter table usage.model_usage drop constraint model_usage_cost_status_check;
alter table usage.model_usage add constraint model_usage_cost_status_check
    check (cost_status in ('KNOWN', 'ESTIMATED', 'BILLED', 'UNKNOWN_USAGE', 'UNKNOWN_PRICE'));
alter table usage.model_usage add constraint model_usage_price_values_check
    check ((input_price_per_million is null or input_price_per_million >= 0)
       and (output_price_per_million is null or output_price_per_million >= 0)
       and (request_price is null or request_price >= 0)
       and (actual_cost is null or actual_cost >= 0));
alter table usage.model_usage add constraint model_usage_billing_evidence_check
    check ((cost_status = 'BILLED' and actual_cost is not null and actual_cost_currency is not null and billing_source is not null)
        or (cost_status <> 'BILLED' and actual_cost is null and actual_cost_currency is null and billing_source is null));
create index model_usage_scope_idx on usage.model_usage(tenant_id, workspace_id, scope_type, scope_id);

-- 价格表仅追加版本；各调用行还保存适用单价，避免后续价格变更改写历史估算。
create table usage.price_schedule (
    provider varchar(100) not null,
    model varchar(150) not null,
    call_type varchar(30) not null check (call_type in ('CHAT', 'EMBEDDING_QUERY', 'EMBEDDING_INDEX', 'EMBEDDING_BATCH', 'PEER')),
    price_version varchar(80) not null,
    source varchar(120) not null,
    source_version varchar(120) not null,
    currency varchar(3) not null check (currency ~ '^[A-Z]{3}$'),
    billing_unit varchar(20) not null check (billing_unit in ('TOKEN_MILLION', 'REQUEST')),
    input_price_per_million numeric(18, 8),
    output_price_per_million numeric(18, 8),
    request_price numeric(18, 8),
    effective_at timestamptz not null,
    expires_at timestamptz,
    created_at timestamptz not null default now(),
    primary key (provider, model, call_type, price_version),
    unique (provider, model, call_type, effective_at),
    check (expires_at is null or expires_at > effective_at),
    check ((billing_unit = 'TOKEN_MILLION' and input_price_per_million is not null and input_price_per_million >= 0
            and output_price_per_million is not null and output_price_per_million >= 0 and request_price is null)
        or (billing_unit = 'REQUEST' and input_price_per_million is null and output_price_per_million is null
            and request_price is not null and request_price >= 0))
);

create function usage.reject_price_schedule_mutation() returns trigger language plpgsql as $$
begin
    raise exception '价格表版本仅允许追加，不可修改或删除';
end;
$$;
create trigger price_schedule_immutable before update or delete on usage.price_schedule
    for each row execute function usage.reject_price_schedule_mutation();
