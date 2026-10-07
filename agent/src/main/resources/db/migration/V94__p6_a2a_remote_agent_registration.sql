-- 只登记一个同信任域只读复核 Agent；跨域 ID 由各自公共 API 校验，不建立跨模块外键。
create table agent.remote_registration (
    id uuid primary key,
    tenant_id uuid not null,
    workspace_id uuid not null,
    owner_id uuid not null,
    agent_key varchar(120) not null,
    agent_version varchar(40) not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    connector_id uuid not null,
    peer_skill_id varchar(120) not null,
    allowed_input_fields text[] not null check (cardinality(allowed_input_fields) > 0),
    allowed_output_fields text[] not null check (cardinality(allowed_output_fields) > 0),
    delegation_actions text[] not null check (cardinality(delegation_actions) > 0),
    resource_scope varchar(40) not null check (resource_scope = 'OWNER_GRANTED_CUSTOMER'),
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    created_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, agent_key, agent_version)
);

insert into agent.remote_registration(id, tenant_id, workspace_id, owner_id, agent_key, agent_version,
                                      capability_id, capability_version, connector_id, peer_skill_id,
                                      allowed_input_fields, allowed_output_fields, delegation_actions,
                                      resource_scope, status)
values ('24000000-0000-4000-8000-000000000101', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'risk-review', '1.0.0', '54000000-0000-4000-8000-000000000001', '1.0.0',
        '23000000-0000-4000-8000-000000000101', 'agent.risk.review',
        array['customerId', 'riskSummary'], array['riskLevel', 'rationale', 'citations'],
        array['agent:risk-review'], 'OWNER_GRANTED_CUSTOMER', 'ACTIVE');
