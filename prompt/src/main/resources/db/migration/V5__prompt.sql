create schema if not exists prompt;

create table prompt.version (
    id uuid not null,
    tenant_id uuid not null,
    workspace_id uuid not null,
    asset_version varchar(40) not null,
    system_template text not null,
    user_template text not null,
    status varchar(20) not null check (status in ('DRAFT', 'IN_REVIEW', 'PUBLISHED', 'DEPRECATED', 'REVOKED')),
    created_at timestamptz not null default now(),
    primary key (id, workspace_id, asset_version),
    unique (id, tenant_id, workspace_id, asset_version)
);

insert into prompt.version(id, tenant_id, workspace_id, asset_version, system_template, user_template, status) values
 ('21000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '1.0.0',
  '你是 EAF 风险分析 Agent。只输出 JSON；不要调用工具，不要把模型建议当作外部事实。',
  '请基于以下用户提供的合成客户摘要给出风险分析：{{input}}。输出字段必须是 riskLevel、summary、reasons、uncertainties。', 'PUBLISHED'),
 ('21000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', '1.0.0',
  '你是 EAF 风险分析 Agent。只输出 JSON；不要调用工具，不要把模型建议当作外部事实。',
  '请基于以下用户提供的合成客户摘要给出风险分析：{{input}}。输出字段必须是 riskLevel、summary、reasons、uncertainties。', 'PUBLISHED'),
 ('21000000-0000-4000-8000-000000000002', '70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', '1.0.0',
  '你是 EAF 风险分析 Agent。只输出 JSON；不要调用工具，不要把模型建议当作外部事实。',
  '请基于以下用户提供的合成客户摘要给出风险分析：{{input}}。输出字段必须是 riskLevel、summary、reasons、uncertainties。', 'PUBLISHED');
-- 本文件负责 EAF 的 V5__prompt.sql 相关定义。
