insert into credential.binding(id, scope_type, tenant_id, workspace_id, credential_ref, audience,
                               allowed_uses, permissions, status, current_version)
values ('a7000000-0000-4000-8000-000000000014', 'SYSTEM', null, null, 'model-typesafe', 'typesafe',
        array['model.decision'], array['provider:typesafe:invoke'], 'ACTIVE', 1);

insert into credential.secret_version(binding_id, version, secret_ref, status, valid_from)
values ('a7000000-0000-4000-8000-000000000014', 1, 'env://TYPESAFE_API_KEY', 'ACTIVE', now());
