-- 仅在指定 Workspace 预置禁用的只读契约夹具；部署必须显式启用并替换为获准的本地 fixture URL。
insert into connector.instance(id, tenant_id, workspace_id, provider, base_url, status,
                               credential_ref, audience, allowed_uses, permissions)
values ('23000000-0000-4000-8000-000000000107', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'P7_CRM_READ_CONTRACT_FIXTURE',
        'http://127.0.0.1:19092', 'DISABLED', 'p7-crm-read', 'eaf:p7-crm-read',
        array['customer.read'], array['crm.customer.read'])
on conflict (tenant_id, workspace_id, provider) do nothing;
