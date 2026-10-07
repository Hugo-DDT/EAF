-- P12 结果写入和核验使用独立的最小凭据用途，不继承 P7 创建用途。
insert into credential.binding(id, scope_type, tenant_id, workspace_id, credential_ref, audience,
                               allowed_uses, permissions, status, current_version)
values ('a7000000-0000-4000-8000-000000000014', 'WORKSPACE',
        '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        'p12-crm-outcome', 'eaf:p12-crm-outcome',
        array['followup.result.create','followup.result.verify'],
        array['crm.followup.result','crm.followup.read'], 'ACTIVE', 1)
on conflict (id) do nothing;

insert into credential.secret_version(binding_id, version, secret_ref, status, valid_from)
values ('a7000000-0000-4000-8000-000000000014', 1,
        'env://eaf.credentials.p12-crm-outcome.token', 'ACTIVE', now())
on conflict (binding_id, version) do nothing;
