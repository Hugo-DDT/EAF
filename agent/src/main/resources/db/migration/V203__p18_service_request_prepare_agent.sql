insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version,
                          model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy)
values ('20000000-0000-4000-8000-000000000011', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'service-request-prepare', '1.2.0',
        '21000000-0000-4000-8000-000000000011', '1.1.0', '22000000-0000-4000-8000-000000000001',
        'PUBLISHED', false, 'SERVICE_REQUEST_PREPARE_V2', 'VECTOR', 'NONE')
on conflict (id, workspace_id, asset_version) do nothing;
