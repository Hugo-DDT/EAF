insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description)
select '54000000-0000-4000-8000-000000000009', tenant_id, id, '80000000-0000-4000-8000-000000000001',
       'knowledge-question-answer', '基于正式知识片段回答并列出引用。'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (tenant_id, workspace_id, name) do nothing;

insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
select '54000000-0000-4000-8000-000000000009', tenant_id, id, '1.0.0',
       '20000000-0000-4000-8000-000000000009', '1.0.0', '53000000-0000-4000-8000-000000000009', '1.0.0',
       '21000000-0000-4000-8000-000000000009', '1.0.0', 'p9-qa-v1', 'PUBLISHED'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.definition(id, tenant_id, workspace_id, owner_id, name, description)
select '54000000-0000-4000-8000-00000000000a', tenant_id, id, '80000000-0000-4000-8000-000000000001',
       'customer-followup-draft', '形成只读客户风险分析及待确认跟进草稿。'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (tenant_id, workspace_id, name) do nothing;

insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
select '54000000-0000-4000-8000-00000000000a', tenant_id, id, '1.0.0',
       '20000000-0000-4000-8000-00000000000a', '1.0.0', '53000000-0000-4000-8000-00000000000a', '1.0.0',
       '21000000-0000-4000-8000-00000000000a', '1.0.0', 'p9-followup-v1', 'PUBLISHED'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (capability_id, workspace_id, asset_version) do nothing;

insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
select '54000000-0000-4000-8000-000000000009', tenant_id, id, '54000000-0000-4000-8000-000000000009', '1.0.0',
       'PUBLISHED', '80000000-0000-4000-8000-000000000001'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
insert into capability.release(release_id, tenant_id, workspace_id, capability_id, capability_version, action, actor_id)
select '54000000-0000-4000-8000-00000000000b', tenant_id, id, '54000000-0000-4000-8000-00000000000a', '1.0.0',
       'PUBLISHED', '80000000-0000-4000-8000-000000000001'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (tenant_id, workspace_id, capability_id, capability_version, action) do nothing;
