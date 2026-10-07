create table "identity".external_identity (
    issuer varchar(512) not null check (length(issuer) between 1 and 512),
    subject varchar(512) not null check (length(subject) between 1 and 512),
    actor_id uuid not null references "identity".subject(id),
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    created_at timestamptz not null default now(),
    primary key (issuer, subject)
);

create index external_identity_actor_idx on "identity".external_identity(actor_id);
-- 本迁移只保存已审批配置的外部主体映射，不接纳 JWT 自报的租户、角色或授权。
