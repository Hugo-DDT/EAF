-- P8 将 Typesafe Choice 作为独立 DECISION 用量；单价取 2026-10-02 官方 Jev 1.13 页面。
alter table usage.model_usage drop constraint model_usage_call_type_check;
alter table usage.model_usage add constraint model_usage_call_type_check
    check (call_type in ('CHAT', 'DECISION', 'EMBEDDING_QUERY', 'EMBEDDING_INDEX', 'EMBEDDING_BATCH', 'PEER'));

alter table usage.price_schedule drop constraint price_schedule_call_type_check;
alter table usage.price_schedule add constraint price_schedule_call_type_check
    check (call_type in ('CHAT', 'DECISION', 'EMBEDDING_QUERY', 'EMBEDDING_INDEX', 'EMBEDDING_BATCH', 'PEER'));

alter table usage.spend_reservation drop constraint spend_reservation_call_type_check;
alter table usage.spend_reservation add constraint spend_reservation_call_type_check
    check (call_type in ('CHAT', 'DECISION', 'EMBEDDING_QUERY', 'EMBEDDING_INDEX', 'EMBEDDING_BATCH', 'PEER'));

insert into usage.price_schedule(provider, model, call_type, price_version, source, source_version,
                                currency, billing_unit, input_price_per_million, output_price_per_million,
                                request_price, effective_at)
values ('typesafe', 'jev-1.13.0', 'DECISION', 'typesafe-jev-1.13.0-2026-10-02',
        'https://docs.typesafe.ai/models', '2026-10-02', 'USD', 'TOKEN_MILLION', 0.04200000, 0.00000000,
        null, '2026-10-02T00:00:00Z');
