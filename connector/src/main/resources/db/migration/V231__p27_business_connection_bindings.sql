create table connector.employee_mapping (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    connector_id uuid not null,
    actor_id uuid not null,
    external_subject_id varchar(160) not null check (external_subject_id ~ '^[A-Za-z0-9._:-]{1,160}$'),
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED', 'REVOKED')),
    row_version bigint not null default 1 check (row_version > 0),
    updated_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, connector_id, actor_id),
    foreign key (connector_id, tenant_id, workspace_id)
        references connector.instance(id, tenant_id, workspace_id)
);

insert into connector.instance(id, tenant_id, workspace_id, provider, base_url, status,
                               credential_ref, audience, allowed_uses, permissions)
values ('23000000-0000-4000-8000-000000000120', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'P27_OA_TODO_FIXTURE', 'http://127.0.0.1:19095',
        'DISABLED', 'p27-oa', 'eaf:p27-oa', array['oa.todo.read'], array['oa.todo.read']),
       ('23000000-0000-4000-8000-000000000121', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', 'P27_SERVICE_DESK_RESULT_FIXTURE', 'http://127.0.0.1:19094',
        'DISABLED', 'p27-service-desk-result', 'eaf:p27-service-desk-result',
        array['service.request.status.read','service.request.result.write','service.request.result.verify'],
        array['service.request.status.read','service.request.result.write','service.request.result.read'])
on conflict (tenant_id, workspace_id, provider) do nothing;

insert into connector.employee_mapping(id, tenant_id, workspace_id, connector_id, actor_id,
                                        external_subject_id, status)
values ('23000000-0000-4000-8000-000000000122', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '23000000-0000-4000-8000-000000000120',
        '80000000-0000-4000-8000-000000000001', 'alice', 'ACTIVE'),
       ('23000000-0000-4000-8000-000000000123', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '23000000-0000-4000-8000-000000000120',
        '80000000-0000-4000-8000-000000000002', 'bob', 'ACTIVE'),
       ('23000000-0000-4000-8000-000000000124', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '23000000-0000-4000-8000-000000000121',
        '80000000-0000-4000-8000-000000000001', 'alice', 'ACTIVE'),
       ('23000000-0000-4000-8000-000000000125', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '23000000-0000-4000-8000-000000000121',
        '80000000-0000-4000-8000-000000000002', 'bob', 'ACTIVE')
on conflict (tenant_id, workspace_id, connector_id, actor_id) do nothing;
