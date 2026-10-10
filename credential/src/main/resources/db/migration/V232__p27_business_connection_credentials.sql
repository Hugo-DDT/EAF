insert into credential.binding(id, scope_type, tenant_id, workspace_id, credential_ref, audience,
                               allowed_uses, permissions, status, current_version)
values ('a7000000-0000-4000-8000-000000000020', 'WORKSPACE',
        '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        'p27-oa', 'eaf:p27-oa', array['oa.todo.read'], array['oa.todo.read'], 'ACTIVE', 1),
       ('a7000000-0000-4000-8000-000000000021', 'WORKSPACE',
        '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        'p27-service-desk-result', 'eaf:p27-service-desk-result',
        array['service.request.status.read','service.request.result.write','service.request.result.verify'],
        array['service.request.status.read','service.request.result.write','service.request.result.read'], 'ACTIVE', 1)
on conflict (id) do nothing;

insert into credential.secret_version(binding_id, version, secret_ref, status, valid_from)
values ('a7000000-0000-4000-8000-000000000020', 1, 'env://eaf.credentials.p27-oa.token', 'ACTIVE', now()),
       ('a7000000-0000-4000-8000-000000000021', 1, 'env://eaf.credentials.p27-service-desk-result.token', 'ACTIVE', now())
on conflict (binding_id, version) do nothing;
