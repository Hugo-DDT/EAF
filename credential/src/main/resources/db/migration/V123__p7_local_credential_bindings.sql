insert into credential.binding(id, scope_type, tenant_id, workspace_id, credential_ref, audience,
                               allowed_uses, permissions, status, current_version) values
 ('a7000000-0000-4000-8000-000000000001', 'WORKSPACE', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', 'test-crm', 'eaf:test-crm', array['customer.read','followup.create','followup.verify'], array['crm.customer.read','crm.followup.create','crm.followup.read'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000002', 'WORKSPACE', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', 'test-crm', 'eaf:test-crm', array['customer.read','followup.create','followup.verify'], array['crm.customer.read','crm.followup.create','crm.followup.read'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000003', 'WORKSPACE', '70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', 'test-crm', 'eaf:test-crm', array['customer.read','followup.create','followup.verify'], array['crm.customer.read','crm.followup.create','crm.followup.read'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000004', 'WORKSPACE', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', 'a2a-peer-review', 'eaf:a2a:peer', array['a2a.send','a2a.get','a2a.cancel'], array['agent:risk-review'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000005', 'WORKSPACE', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', 'a2a-peer-review', 'eaf:a2a:peer', array['a2a.send','a2a.get','a2a.cancel'], array['agent:risk-review'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000006', 'WORKSPACE', '70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', 'a2a-peer-review', 'eaf:a2a:peer', array['a2a.send','a2a.get','a2a.cancel'], array['agent:risk-review'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000007', 'WORKSPACE', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', 'a2a-evaluation-review', 'eaf:evaluation-peer', array['a2a.send','a2a.get','a2a.cancel'], array['agent:risk-review-evaluation'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000008', 'WORKSPACE', '70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', 'a2a-evaluation-review', 'eaf:evaluation-peer', array['a2a.send','a2a.get','a2a.cancel'], array['agent:risk-review-evaluation'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000009', 'WORKSPACE', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', 'a2a-evaluation-review', 'eaf:evaluation-peer', array['a2a.send','a2a.get','a2a.cancel'], array['agent:risk-review-evaluation'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000010', 'SYSTEM', null, null, 'model-dashscope', 'dashscope', array['model.chat','model.embedding'], array['provider:dashscope:invoke'], 'ACTIVE', 1),
 ('a7000000-0000-4000-8000-000000000011', 'SYSTEM', null, null, 'model-deepseek', 'deepseek', array['model.chat'], array['provider:deepseek:invoke'], 'ACTIVE', 1);

insert into credential.secret_version(binding_id, version, secret_ref, status, valid_from) values
 ('a7000000-0000-4000-8000-000000000001', 1, 'env://eaf.credentials.test-crm.token', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000002', 1, 'env://eaf.credentials.test-crm.token', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000003', 1, 'env://eaf.credentials.test-crm.token', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000004', 1, 'env://eaf.credentials.a2a-peer-review.token', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000005', 1, 'env://eaf.credentials.a2a-peer-review.token', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000006', 1, 'env://eaf.credentials.a2a-peer-review.token', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000007', 1, 'env://eaf.credentials.a2a-evaluation-review.token', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000008', 1, 'env://eaf.credentials.a2a-evaluation-review.token', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000009', 1, 'env://eaf.credentials.a2a-evaluation-review.token', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000010', 1, 'env://DASHSCOPE_API_KEY', 'ACTIVE', now()),
 ('a7000000-0000-4000-8000-000000000011', 1, 'env://DEEPSEEK_API_KEY', 'ACTIVE', now());

-- 仅本地合成 Workspace 与模型 profile 有默认绑定；企业部署必须配置受管秘密后端并审查元数据。
