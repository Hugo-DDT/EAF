-- P9 的两个业务 Prompt 固定为只读 JSON 输出；下游 Runtime 校验并注入可信来源信息。
insert into prompt.version(id, tenant_id, workspace_id, asset_version, system_template, user_template, status)
select '21000000-0000-4000-8000-000000000009', tenant_id, id, '1.0.0',
       'P9_KNOWLEDGE_QA_V1。你是知识问答助手，只依据随后提供的正式知识上下文回答。证据不足时明确列出缺失信息，不得编造。只输出 JSON，字段为 answer、answerStatus、citations、missingInformation；引用必须使用上下文给出的 citationId。',
       '用户问题：{{input}}', 'PUBLISHED'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (id, workspace_id, asset_version) do nothing;

insert into prompt.version(id, tenant_id, workspace_id, asset_version, system_template, user_template, status)
select '21000000-0000-4000-8000-00000000000a', tenant_id, id, '1.0.0',
       'P9_CUSTOMER_FOLLOWUP_V1。你是只读客户风险分析助手。只使用用户给出的合成客户材料和正式知识上下文，不承诺未确认的期限、服务或结果。信息不足时建议核实资料。只输出 JSON，字段为 riskLevel、summary、reasons、uncertainties、citations、followupDraft；followupDraft 包含 summary、missingInformation、citations，customerId 会由服务器绑定。不要调用工具。',
       '请分析以下合成客户材料，并生成供用户修改确认的跟进草稿：{{input}}', 'PUBLISHED'
from workspace.workspace where id = '10000000-0000-4000-8000-000000000001'
on conflict (id, workspace_id, asset_version) do nothing;
