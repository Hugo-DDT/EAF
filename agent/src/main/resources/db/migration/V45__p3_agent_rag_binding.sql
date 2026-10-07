alter table agent.version add column if not exists rag_enabled boolean not null default false;

insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, model_profile_id, status, rag_enabled)
select '20000000-0000-4000-8000-000000000001', tenant_id, id, 'customer-risk-analysis-rag', '3.0.0',
       '21000000-0000-4000-8000-000000000001', '3.0.0', '22000000-0000-4000-8000-000000000001', 'PUBLISHED', true
from workspace.workspace where id in ('10000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002')
on conflict (id, workspace_id, asset_version) do update set rag_enabled = excluded.rag_enabled, status = excluded.status;

insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, model_profile_id, status, rag_enabled)
select '20000000-0000-4000-8000-000000000002', tenant_id, id, 'customer-risk-analysis-rag', '3.0.0',
       '21000000-0000-4000-8000-000000000002', '3.0.0', '22000000-0000-4000-8000-000000000001', 'PUBLISHED', true
from workspace.workspace where id = '10000000-0000-0000-0000-000000000003'
on conflict (id, workspace_id, asset_version) do update set rag_enabled = excluded.rag_enabled, status = excluded.status;

-- 仅显式绑定的 Agent 版本启用 RAG；旧 P1/P2 版本保持原有无知识行为。
