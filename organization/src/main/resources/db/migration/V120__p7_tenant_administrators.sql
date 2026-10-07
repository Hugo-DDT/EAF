-- 租户管理员表只登记经部署开通流程确认的管理员，不由 HTTP 请求或 JWT claims 创建。
create table organization.tenant_admin (
    tenant_id uuid not null,
    subject_id uuid not null,
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, subject_id),
    foreign key (tenant_id, subject_id) references organization.member(tenant_id, subject_id)
);

-- IdentityService 当前为每个认证主体解析单一租户；成员 API 同时禁止身份在租户间迁移。
create unique index organization_one_active_tenant_per_subject
    on organization.member(subject_id) where status = 'ACTIVE';
