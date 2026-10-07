-- 预置默认禁用的写入契约夹具；生产环境不得将该 Provider 当成真实 CRM。
insert into connector.instance(id, tenant_id, workspace_id, provider, base_url, status,
                               credential_ref, audience, allowed_uses, permissions)
values ('23000000-0000-4000-8000-000000000108', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'P7_CRM_WRITE_CONTRACT_FIXTURE',
        'http://127.0.0.1:19093', 'DISABLED', 'p7-crm-write', 'eaf:p7-crm-write',
        array['customer.read','followup.create','followup.verify'],
        array['crm.customer.read','crm.followup.create','crm.followup.read'])
on conflict (tenant_id, workspace_id, provider) do nothing;
