-- 单独登记隔离评测 peer；默认禁用，真实 endpoint 与凭据必须经后续授权配置。
insert into connector.instance(id, tenant_id, workspace_id, provider, base_url, status,
                               credential_ref, audience, allowed_uses, permissions) values
 ('23000000-0000-4000-8000-000000000104', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
  'A2A_EVALUATION_REVIEW_PEER', 'https://evaluation-peer.invalid/a2a', 'DISABLED', 'a2a-evaluation-review',
  'eaf:evaluation-peer', array['a2a.send', 'a2a.get', 'a2a.cancel'], array['agent:risk-review-evaluation']),
 ('23000000-0000-4000-8000-000000000105', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002',
  'A2A_EVALUATION_REVIEW_PEER', 'https://evaluation-peer.invalid/a2a', 'DISABLED', 'a2a-evaluation-review',
  'eaf:evaluation-peer', array['a2a.send', 'a2a.get', 'a2a.cancel'], array['agent:risk-review-evaluation']),
 ('23000000-0000-4000-8000-000000000106', '70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003',
  'A2A_EVALUATION_REVIEW_PEER', 'https://evaluation-peer.invalid/a2a', 'DISABLED', 'a2a-evaluation-review',
  'eaf:evaluation-peer', array['a2a.send', 'a2a.get', 'a2a.cancel'], array['agent:risk-review-evaluation']);

comment on column connector.instance.provider is '稳定的本域连接类型；评测 peer 使用独立凭据引用、受众和授权动作。';
