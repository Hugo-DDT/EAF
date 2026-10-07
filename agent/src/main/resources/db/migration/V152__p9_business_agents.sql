insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version,
                          model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy)
select '20000000-0000-4000-8000-000000000009', tenant_id, id, 'knowledge-question-answer', '1.0.0',
       '21000000-0000-4000-8000-000000000009', '1.0.0', '22000000-0000-4000-8000-000000000001',
       'PUBLISHED', true, 'KNOWLEDGE_QA_V1', 'HYBRID', 'PASSAGE_CHOICE_V1'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (id, workspace_id, asset_version) do nothing;

insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version,
                          model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy)
select '20000000-0000-4000-8000-00000000000a', tenant_id, id, 'customer-followup-draft', '1.0.0',
       '21000000-0000-4000-8000-00000000000a', '1.0.0', '22000000-0000-4000-8000-000000000001',
       'PUBLISHED', true, 'CUSTOMER_FOLLOWUP_V1', 'HYBRID', 'PASSAGE_CHOICE_V1'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (id, workspace_id, asset_version) do nothing;
