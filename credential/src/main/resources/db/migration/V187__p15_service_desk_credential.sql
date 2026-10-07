insert into credential.binding(id, scope_type, tenant_id, workspace_id, credential_ref, audience,
                               allowed_uses, permissions, status, current_version)
values ('a7000000-0000-4000-8000-000000000016', 'WORKSPACE',
        '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        'p15-service-desk', 'eaf:p15-service-desk',
        array['service.request.register','service.request.verify'],
        array['service.request.register','service.request.read'], 'ACTIVE', 1)
on conflict (id) do nothing;

insert into credential.secret_version(binding_id, version, secret_ref, status, valid_from)
values ('a7000000-0000-4000-8000-000000000016', 1,
        'env://eaf.credentials.p15-service-desk.token', 'ACTIVE', now())
on conflict (binding_id, version) do nothing;
