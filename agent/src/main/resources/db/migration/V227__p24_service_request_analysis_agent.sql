insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version,
                          model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy)
values ('20000000-0000-4000-8000-000000000010', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'service-request-plan', '1.1.0',
        '21000000-0000-4000-8000-00000000000f', '1.0.0', '22000000-0000-4000-8000-000000000001',
        'PUBLISHED', true, 'SERVICE_REQUEST_PLAN_V1', 'HYBRID', 'NONE')
on conflict (id, workspace_id, asset_version) do nothing;
