-- 发布与依赖增删共用版本行锁，防止依赖集合在发布校验期间漂移。
create or replace function workflow.protect_published_dependency()
returns trigger
language plpgsql
as $$
declare
    current_status varchar(20);
begin
    if tg_op = 'UPDATE' and (
        new.tenant_id is distinct from old.tenant_id
        or new.workspace_id is distinct from old.workspace_id
        or new.workflow_id is distinct from old.workflow_id
        or new.workflow_version is distinct from old.workflow_version
        or new.capability_id is distinct from old.capability_id
        or new.capability_version is distinct from old.capability_version
    ) then
        raise exception 'workflow dependency identity is immutable';
    end if;
    if tg_op = 'DELETE' then
        select status into current_status from workflow.version
        where tenant_id = old.tenant_id and workspace_id = old.workspace_id
          and workflow_id = old.workflow_id and asset_version = old.workflow_version
        for update;
    else
        select status into current_status from workflow.version
        where tenant_id = new.tenant_id and workspace_id = new.workspace_id
          and workflow_id = new.workflow_id and asset_version = new.workflow_version
        for update;
    end if;
    if current_status is distinct from 'DRAFT' then
        raise exception 'published workflow dependencies are immutable';
    end if;
    if tg_op = 'DELETE' then return old; end if;
    return new;
end;
$$;

drop trigger workflow_dependency_immutable on workflow.capability_dependency;
create trigger workflow_dependency_immutable
before insert or update or delete on workflow.capability_dependency
for each row execute function workflow.protect_published_dependency();
