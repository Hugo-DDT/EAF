-- Credential 精确登记 CRM 契约夹具的只读用途与最小客户读取权限。
insert into credential.binding(id, scope_type, tenant_id, workspace_id, credential_ref, audience,
                               allowed_uses, permissions, status, current_version)
values ('a7000000-0000-4000-8000-000000000012', 'WORKSPACE',
        '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        'p7-crm-read', 'eaf:p7-crm-read', array['customer.read'], array['crm.customer.read'], 'ACTIVE', 1)
on conflict (id) do nothing;

insert into credential.secret_version(binding_id, version, secret_ref, status, valid_from)
values ('a7000000-0000-4000-8000-000000000012', 1,
        'env://eaf.credentials.p7-crm-read.token', 'ACTIVE', now())
on conflict (binding_id, version) do nothing;
