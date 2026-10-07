alter table agent.version drop constraint version_response_profile_check;
alter table agent.version add constraint version_response_profile_check
    check (response_profile in ('CUSTOMER_RISK_V1', 'CUSTOMER_FOLLOWUP_V1', 'KNOWLEDGE_QA_V1',
        'CONVERSATIONAL_KNOWLEDGE_QA_V1', 'CONVERSATIONAL_CUSTOMER_FOLLOWUP_V1',
        'CONVERSATIONAL_KNOWLEDGE_QA_V2', 'CONVERSATIONAL_CUSTOMER_FOLLOWUP_V2', 'EXPERIENCE_DRAFT_V1'));

insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version,
                          model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy)
values ('20000000-0000-4000-8000-00000000000b', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'conversational-knowledge-answer', '1.1.0',
        '21000000-0000-4000-8000-00000000000b', '1.1.0', '22000000-0000-4000-8000-000000000001',
        'PUBLISHED', true, 'CONVERSATIONAL_KNOWLEDGE_QA_V2', 'HYBRID', 'PASSAGE_CHOICE_V1'),
       ('20000000-0000-4000-8000-00000000000c', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'conversational-customer-assistant', '1.1.0',
        '21000000-0000-4000-8000-00000000000c', '1.1.0', '22000000-0000-4000-8000-000000000001',
        'PUBLISHED', true, 'CONVERSATIONAL_CUSTOMER_FOLLOWUP_V2', 'HYBRID', 'PASSAGE_CHOICE_V1'),
       ('20000000-0000-4000-8000-00000000000d', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'experience-draft-assistant', '1.0.0',
        '21000000-0000-4000-8000-00000000000d', '1.0.0', '22000000-0000-4000-8000-000000000001',
        'PUBLISHED', false, 'EXPERIENCE_DRAFT_V1', 'VECTOR', 'NONE')
on conflict (id, workspace_id, asset_version) do nothing;
