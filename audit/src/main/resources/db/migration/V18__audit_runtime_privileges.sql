do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'eaf_audit_owner') then
        create role eaf_audit_owner nologin;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'eaf_audit_runtime') then
        create role eaf_audit_runtime nologin;
    end if;
end $$;

alter table audit.audit_event owner to eaf_audit_owner;

revoke all on audit.audit_event from public;
revoke update, delete, truncate, references, trigger on audit.audit_event from current_user;
grant select, insert on audit.audit_event to current_user;

grant usage on schema audit to eaf_audit_runtime;
grant select, insert on audit.audit_event to eaf_audit_runtime;
revoke update, delete, truncate, references, trigger on audit.audit_event from eaf_audit_runtime;
-- 本文件负责 EAF 的 V18__audit_runtime_privileges.sql 相关定义。
