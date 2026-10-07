create or replace function agent.reject_published_mutation()
returns trigger
language plpgsql
as $$
begin
    if old.status = 'PUBLISHED' and (
        new.id is distinct from old.id
        or new.tenant_id is distinct from old.tenant_id
        or new.workspace_id is distinct from old.workspace_id
        or new.name is distinct from old.name
        or new.asset_version is distinct from old.asset_version
        or new.prompt_id is distinct from old.prompt_id
        or new.prompt_version is distinct from old.prompt_version
        or new.model_profile_id is distinct from old.model_profile_id
        or new.created_at is distinct from old.created_at
    ) then
        raise exception 'published agent content is immutable';
    end if;
    return new;
end;
$$;

create trigger agent_published_immutable
before update on agent.version
for each row execute function agent.reject_published_mutation();
-- 本文件负责 EAF 的 V20__agent_published_immutable.sql 相关定义。
