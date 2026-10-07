insert into connector.instance(id, tenant_id, workspace_id, provider, base_url, status,
                               credential_ref, audience, allowed_uses, permissions)
values ('23000000-0000-4000-8000-000000000110', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'P15_INTERNAL_SERVICE_DESK_FIXTURE',
        'http://127.0.0.1:19094', 'DISABLED', 'p15-service-desk', 'eaf:p15-service-desk',
        array['service.request.register','service.request.verify'],
        array['service.request.register','service.request.read'])
on conflict (tenant_id, workspace_id, provider) do nothing;
