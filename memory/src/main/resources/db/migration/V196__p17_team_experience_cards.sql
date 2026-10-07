alter table memory.experience_card
    add column card_kind varchar(16) not null default 'PERSONAL' check (card_kind in ('PERSONAL', 'TEAM')),
    add column scenario_key varchar(64);

alter table memory.experience_card drop constraint experience_card_applicability_check;
alter table memory.experience_card drop constraint experience_card_check;
alter table memory.experience_card add constraint experience_card_applicability_check
    check (applicability in ('GENERAL', 'CUSTOMER', 'SERVICE_REQUEST'));
alter table memory.experience_card add constraint experience_card_scope_check check (
    (card_kind = 'PERSONAL' and scenario_key is null and applicability in ('GENERAL', 'CUSTOMER')
        and ((applicability = 'GENERAL' and customer_id is null)
            or (applicability = 'CUSTOMER' and customer_id is not null and length(trim(customer_id)) > 0)))
    or (card_kind = 'TEAM' and scenario_key is not null and scenario_key ~ '^[a-z][a-z0-9-]{0,63}$'
        and applicability = 'SERVICE_REQUEST' and customer_id is null)
);

alter table memory.experience_card_revision
    add column applies_when varchar(300),
    add column experience_content varchar(800),
    add column source_work_item_id uuid,
    add column source_proof_json jsonb;
alter table memory.experience_card_revision add constraint experience_revision_kind_fields_check check (
    (source_work_item_id is null and source_proof_json is null and applies_when is null and experience_content is null)
    or (source_work_item_id is not null and source_proof_json is not null
        and applies_when is not null and length(trim(applies_when)) > 0
        and experience_content is not null and length(trim(experience_content)) > 0
        and jsonb_typeof(source_proof_json) = 'object')
);

create index memory_team_experience_scenario_idx
    on memory.experience_card(tenant_id, workspace_id, scenario_key, active_since desc, memory_id desc)
    where card_kind = 'TEAM' and active_revision is not null;
