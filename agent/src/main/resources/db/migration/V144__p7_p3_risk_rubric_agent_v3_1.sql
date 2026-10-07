-- 质量评测使用新的不可变 Agent/Prompt 版本；已运行版本继续保留以供审计。
insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, model_profile_id, status, rag_enabled)
select '20000000-0000-4000-8000-000000000001', tenant_id, id, 'customer-risk-analysis-rag', '3.1.0',
       '21000000-0000-4000-8000-000000000001', '3.1.0', '22000000-0000-4000-8000-000000000001', 'PUBLISHED', true
from workspace.workspace where id in ('10000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002')
on conflict (id, workspace_id, asset_version) do nothing;

insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, model_profile_id, status, rag_enabled)
select '20000000-0000-4000-8000-000000000002', tenant_id, id, 'customer-risk-analysis-rag', '3.1.0',
       '21000000-0000-4000-8000-000000000002', '3.1.0', '22000000-0000-4000-8000-000000000001', 'PUBLISHED', true
from workspace.workspace where id = '10000000-0000-4000-8000-000000000003'
on conflict (id, workspace_id, asset_version) do nothing;
