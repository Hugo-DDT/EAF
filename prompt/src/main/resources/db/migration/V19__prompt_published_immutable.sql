create or replace function prompt.reject_published_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.status = 'PUBLISHED' and (
        new.id is distinct from old.id
        or new.tenant_id is distinct from old.tenant_id
        or new.workspace_id is distinct from old.workspace_id
        or new.asset_version is distinct from old.asset_version
        or new.system_template is distinct from old.system_template
        or new.user_template is distinct from old.user_template
        or new.created_at is distinct from old.created_at
    ) then
        raise exception 'published prompt content is immutable';
    end if;
    return new;
end;
$$;

create trigger prompt_published_immutable
before update on prompt.version
for each row execute function prompt.reject_published_mutation();
-- 本文件负责 EAF 的 V19__prompt_published_immutable.sql 相关定义。
