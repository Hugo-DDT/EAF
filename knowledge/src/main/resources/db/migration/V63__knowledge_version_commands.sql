-- 每个 Knowledge 版本命令可重试；版本内容发布后只允许状态转为撤回。
alter table knowledge.document_version add column idempotency_key varchar(200);
alter table knowledge.document_version add column request_hash char(64);
alter table knowledge.document_version add constraint knowledge_version_idempotency_pair
    check ((idempotency_key is null) = (request_hash is null));
create unique index knowledge_version_idempotency_unique
    on knowledge.document_version(tenant_id, workspace_id, document_id, idempotency_key)
    where idempotency_key is not null;

create or replace function knowledge.reject_published_version_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.status in ('PUBLISHED', 'REVOKED') and (
        new.id is distinct from old.id
        or new.tenant_id is distinct from old.tenant_id
        or new.workspace_id is distinct from old.workspace_id
        or new.document_id is distinct from old.document_id
        or new.asset_version is distinct from old.asset_version
        or new.content is distinct from old.content
        or new.content_hash is distinct from old.content_hash
        or new.created_at is distinct from old.created_at
        or new.idempotency_key is distinct from old.idempotency_key
        or new.request_hash is distinct from old.request_hash
    ) then
        raise exception 'published knowledge version content is immutable';
    end if;
    if old.status = 'REVOKED' and new.status <> 'REVOKED' then
        raise exception 'revoked knowledge version cannot be restored';
    end if;
    return new;
end;
$$;
create trigger knowledge_published_version_immutable
before update on knowledge.document_version
for each row execute function knowledge.reject_published_version_mutation();
