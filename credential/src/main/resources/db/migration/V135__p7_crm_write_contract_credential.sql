-- Credential 只保存秘密引用，并将写入夹具限制到客户读取、跟进创建和结果核验三种用途。
insert into credential.binding(id, scope_type, tenant_id, workspace_id, credential_ref, audience,
                               allowed_uses, permissions, status, current_version)
values ('a7000000-0000-4000-8000-000000000013', 'WORKSPACE',
        '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        'p7-crm-write', 'eaf:p7-crm-write',
        array['customer.read','followup.create','followup.verify'],
        array['crm.customer.read','crm.followup.create','crm.followup.read'], 'ACTIVE', 1)
on conflict (id) do nothing;

insert into credential.secret_version(binding_id, version, secret_ref, status, valid_from)
values ('a7000000-0000-4000-8000-000000000013', 1,
        'env://eaf.credentials.p7-crm-write.token', 'ACTIVE', now())
on conflict (binding_id, version) do nothing;
