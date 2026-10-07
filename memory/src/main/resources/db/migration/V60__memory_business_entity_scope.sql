-- 仅把明确绑定到业务实体的 Memory 注入同一实体任务；空绑定代表 Workspace 通用记忆。
alter table memory.version add column business_entity_type varchar(80);
alter table memory.version add column business_entity_id varchar(160);
alter table memory.version add constraint memory_version_business_entity_pair
    check ((business_entity_type is null) = (business_entity_id is null));

update memory.version
set business_entity_type = 'Customer', business_entity_id = 'customer-001'
where memory_id = '56000000-0000-4000-8000-000000000001' and asset_version = '1.0.0';

-- 实体归属也是发布内容的一部分；发布或撤回后禁止通过更新改变适用对象。
create or replace function memory.reject_published_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.status in ('PUBLISHED', 'REVOKED') and (
        new.memory_id is distinct from old.memory_id
        or new.tenant_id is distinct from old.tenant_id
        or new.workspace_id is distinct from old.workspace_id
        or new.asset_version is distinct from old.asset_version
        or new.memory_type is distinct from old.memory_type
        or new.scope is distinct from old.scope
        or new.content is distinct from old.content
        or new.confidence is distinct from old.confidence
        or new.expires_at is distinct from old.expires_at
        or new.source_type is distinct from old.source_type
        or new.source_ref is distinct from old.source_ref
        or new.evidence_refs is distinct from old.evidence_refs
        or new.created_at is distinct from old.created_at
        or new.business_entity_type is distinct from old.business_entity_type
        or new.business_entity_id is distinct from old.business_entity_id
    ) then
        raise exception 'published memory scope is immutable';
    end if;
    return new;
end;
$$;
