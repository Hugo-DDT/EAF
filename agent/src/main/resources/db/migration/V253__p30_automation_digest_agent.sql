alter table agent.version drop constraint version_response_profile_check;
alter table agent.version add constraint version_response_profile_check
    check (response_profile in ('CUSTOMER_RISK_V1', 'CUSTOMER_FOLLOWUP_V1', 'KNOWLEDGE_QA_V1',
        'CONVERSATIONAL_KNOWLEDGE_QA_V1', 'CONVERSATIONAL_CUSTOMER_FOLLOWUP_V1',
        'CONVERSATIONAL_KNOWLEDGE_QA_V2', 'CONVERSATIONAL_CUSTOMER_FOLLOWUP_V2', 'EXPERIENCE_DRAFT_V1',
        'CUSTOMER_ASSISTANT_V3', 'SERVICE_REQUEST_PLAN_V1', 'SERVICE_REQUEST_REGISTRATION_V1',
        'SERVICE_REQUEST_PREPARE_V1', 'SERVICE_REQUEST_PREPARE_V2', 'SERVICE_REQUEST_SUMMARY_V1',
        'SERVICE_REQUEST_BATCH_KNOWLEDGE_V1', 'SERVICE_REQUEST_BATCH_EXPERIENCE_V1',
        'TEAM_EXPERIENCE_IMPROVEMENT_V1', 'P27_BUSINESS_TOOL_V1', 'PROJECT_BRIEF_PREPARE_V1',
        'MY_P16_WORK_DIGEST_RESPONSE_V1'));

insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version,
                          model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy)
values ('20000000-0000-4000-8000-000000000024', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'my-p16-work-digest', '1.0.0',
        '21000000-0000-4000-8000-000000000024', '1.0.0', '22000000-0000-4000-8000-000000000001',
        'PUBLISHED', false, 'MY_P16_WORK_DIGEST_RESPONSE_V1', 'VECTOR', 'NONE')
on conflict (id, workspace_id, asset_version) do nothing;
