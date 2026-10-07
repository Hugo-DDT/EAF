-- 独立结果 Connector 保留 P7 创建工具的审批快照和凭据用途不变。
insert into connector.instance(id, tenant_id, workspace_id, provider, base_url, status,
                               credential_ref, audience, allowed_uses, permissions)
values ('23000000-0000-4000-8000-000000000109', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'P12_CRM_OUTCOME_FIXTURE',
        'http://127.0.0.1:19093', 'DISABLED', 'p12-crm-outcome', 'eaf:p12-crm-outcome',
        array['followup.result.create','followup.result.verify'],
        array['crm.followup.result','crm.followup.read'])
on conflict (tenant_id, workspace_id, provider) do nothing;
