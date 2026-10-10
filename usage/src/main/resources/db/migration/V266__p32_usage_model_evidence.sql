alter table usage.model_usage add column model_profile_id uuid;
alter table usage.model_usage add column model_profile_version varchar(40);
alter table usage.model_usage add column model_configuration_hash char(64);
alter table usage.model_usage add column requested_model varchar(120);
alter table usage.model_usage add column effective_output_token_limit integer;
alter table usage.model_usage add column reported_response_model varchar(120);
alter table usage.model_usage add column finish_reason varchar(80);
alter table usage.model_usage add column model_operation_ms bigint;

alter table usage.model_usage add constraint model_usage_output_limit_nonnegative
    check (effective_output_token_limit is null or effective_output_token_limit >= 0);
alter table usage.model_usage add constraint model_usage_operation_ms_nonnegative
    check (model_operation_ms is null or model_operation_ms >= 0);
