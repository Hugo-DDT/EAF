-- Agent 新版本显式允许同一写工具的新输出契约版本；P7 历史 Agent 保持不变。
insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version,
                          model_profile_id, status, rag_enabled, response_profile, retrieval_mode, evidence_policy)
select id, tenant_id, workspace_id, name, '2.3.0', prompt_id, prompt_version,
       model_profile_id, 'PUBLISHED', rag_enabled, response_profile, retrieval_mode, evidence_policy
from agent.version
where id = '20000000-0000-4000-8000-000000000001'
  and workspace_id = '10000000-0000-4000-8000-000000000001'
  and asset_version = '2.2.0' and status = 'PUBLISHED'
on conflict (id, workspace_id, asset_version) do nothing;

insert into agent.tool_binding(tenant_id, workspace_id, agent_id, agent_version, tool_name, tool_version)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001', '2.3.0', 'crm.customer.query', '1.1.0'),
       ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001', '2.3.0', 'crm.followup.create', '1.2.0')
on conflict do nothing;
