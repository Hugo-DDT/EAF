-- 独立登记合成评测 reviewer；运行时仍逐次复核 Capability、Tool、Connector 与 Workflow 来源。
alter table agent.remote_registration drop constraint remote_registration_resource_scope_check;
alter table agent.remote_registration add constraint remote_registration_resource_scope_check
    check (resource_scope in ('OWNER_GRANTED_CUSTOMER', 'SYNTHETIC_EVALUATION_ONLY'));

insert into agent.remote_registration(id, tenant_id, workspace_id, owner_id, agent_key, agent_version,
                                      capability_id, capability_version, connector_id, peer_skill_id,
                                      allowed_input_fields, allowed_output_fields, delegation_actions,
                                      resource_scope, status)
values ('24000000-0000-4000-8000-000000000102', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001',
        'risk-review-evaluation', '1.0.0', '54000000-0000-4000-8000-000000000001', '1.2.0',
        '23000000-0000-4000-8000-000000000104', 'agent.risk.review.evaluation',
        array['customerId', 'riskSummary'], array['riskLevel', 'rationale', 'citations'],
        array['agent:risk-review-evaluation'], 'SYNTHETIC_EVALUATION_ONLY', 'ACTIVE');

comment on column agent.remote_registration.resource_scope is '限制远端 reviewer 的资源类别；评测登记仅限合成评测数据。';
