insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
select '53000000-0000-4000-8000-000000000009', tenant_id, id, '80000000-0000-4000-8000-000000000001',
       'knowledge-question-answer', '基于正式知识片段回答并列出引用。'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
select '53000000-0000-4000-8000-000000000009', tenant_id, id, '1.0.0',
       '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":8000}}}',
       '{"type":"object","required":["answer","answerStatus","citations","missingInformation"],"additionalProperties":false,"properties":{"answer":{"type":"string"},"answerStatus":{"type":"string"},"citations":{"type":"array","items":{"type":"string"}},"missingInformation":{"type":"array","items":{"type":"string"}}}}',
       '21000000-0000-4000-8000-000000000009', '1.0.0', 'p9-qa-v1', 'PUBLISHED'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.definition(id, tenant_id, workspace_id, owner_id, name, description)
select '53000000-0000-4000-8000-00000000000a', tenant_id, id, '80000000-0000-4000-8000-000000000001',
       'customer-followup-draft', '形成只读客户风险分析及待确认跟进草稿。'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (tenant_id, workspace_id, name) do nothing;

insert into skill.version(skill_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                          prompt_id, prompt_version, evaluation_ref, status)
select '53000000-0000-4000-8000-00000000000a', tenant_id, id, '1.0.0',
       '{"type":"object","required":["input"],"additionalProperties":false,"properties":{"input":{"type":"string","minLength":1,"maxLength":8000}}}',
       '{"type":"object","required":["riskLevel","summary","reasons","uncertainties","followupDraft"],"additionalProperties":false,"properties":{"riskLevel":{"type":"string","enum":["LOW","MEDIUM","HIGH","UNKNOWN"]},"summary":{"type":"string"},"reasons":{"type":"array","items":{"type":"string"}},"uncertainties":{"type":"array","items":{"type":"string"}},"followupDraft":{"type":"object"}}}',
       '21000000-0000-4000-8000-00000000000a', '1.0.0', 'p9-followup-v1', 'PUBLISHED'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (skill_id, workspace_id, asset_version) do nothing;

insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
select '53000000-0000-4000-8000-00000000000b', tenant_id, id, '53000000-0000-4000-8000-000000000009', '1.0.0',
       'PUBLISHED', '80000000-0000-4000-8000-000000000001'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
insert into skill.release(release_id, tenant_id, workspace_id, skill_id, skill_version, action, actor_id)
select '53000000-0000-4000-8000-00000000000c', tenant_id, id, '53000000-0000-4000-8000-00000000000a', '1.0.0',
       'PUBLISHED', '80000000-0000-4000-8000-000000000001'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (tenant_id, workspace_id, skill_id, skill_version, action) do nothing;
