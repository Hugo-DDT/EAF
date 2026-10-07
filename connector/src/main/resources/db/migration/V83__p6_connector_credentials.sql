alter table connector.instance
    add column credential_ref varchar(120),
    add column audience varchar(160),
    add column allowed_uses text[] not null default '{}',
    add column permissions text[] not null default '{}';

update connector.instance
set credential_ref = 'test-crm',
    audience = 'eaf:test-crm',
    allowed_uses = array['customer.read', 'followup.create', 'followup.verify'],
    permissions = array['crm.customer.read', 'crm.followup.create', 'crm.followup.read']
where provider = 'TEST_CRM';

insert into connector.instance(id, tenant_id, workspace_id, provider, base_url, status,
                               credential_ref, audience, allowed_uses, permissions) values
 ('23000000-0000-4000-8000-000000000101', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', 'A2A_REVIEW_PEER', 'http://127.0.0.1:19091/a2a', 'ACTIVE', 'a2a-peer-review', 'eaf:a2a:peer', array['a2a.send', 'a2a.get'], array['agent:risk-review']),
 ('23000000-0000-4000-8000-000000000102', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000002', 'A2A_REVIEW_PEER', 'http://127.0.0.1:19091/a2a', 'ACTIVE', 'a2a-peer-review', 'eaf:a2a:peer', array['a2a.send', 'a2a.get'], array['agent:risk-review']),
 ('23000000-0000-4000-8000-000000000103', '70000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000003', 'A2A_REVIEW_PEER', 'http://127.0.0.1:19091/a2a', 'ACTIVE', 'a2a-peer-review', 'eaf:a2a:peer', array['a2a.send', 'a2a.get'], array['agent:risk-review']);

alter table connector.instance
    add constraint connector_credential_ref_format check (credential_ref is null or credential_ref ~ '^[a-z0-9][a-z0-9._-]{0,119}$'),
    add constraint connector_audience_required check (audience is not null and length(trim(audience)) between 1 and 160),
    add constraint connector_allowed_uses_nonempty check (cardinality(allowed_uses) > 0),
    add constraint connector_permissions_nonempty check (cardinality(permissions) > 0);

comment on column connector.instance.credential_ref is '外部进程配置中的秘密引用；本表不存储秘密明文。';
comment on column connector.instance.audience is '固定凭据受众，必须与 Integration 调用要求一致。';
comment on column connector.instance.allowed_uses is '该凭据允许的有限出站操作用途。';
comment on column connector.instance.permissions is '连接凭据在远端声明的最小权限集合。';
-- 本迁移只登记本地测试 CRM 和单个只读 A2A peer；生产秘密托管与轮换未交付。
