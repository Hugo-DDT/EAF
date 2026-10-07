-- Workflow 可以先持有根预算范围；范围不再要求伪造一个根 Task。
alter table task.budget_reservation drop constraint budget_reservation_root_task_id_fkey;
alter table task.budget_scope drop constraint budget_scope_root_fk;
alter table task.budget_scope add column scope_id uuid;
update task.budget_scope set scope_id = root_task_id;
alter table task.budget_scope alter column scope_id set not null;
alter table task.budget_scope drop constraint budget_scope_pkey;
alter table task.budget_scope alter column root_task_id drop not null;
alter table task.budget_scope
    add constraint budget_scope_pkey primary key (scope_id),
    add constraint budget_scope_root_unique unique (root_task_id),
    add constraint budget_scope_identity_unique unique (tenant_id, workspace_id, scope_id),
    add constraint budget_scope_root_fk foreign key (tenant_id, workspace_id, root_task_id)
        references task.task(tenant_id, workspace_id, id),
    add column created_by uuid,
    add column creation_key varchar(64),
    add column creation_hash varchar(64),
    add constraint budget_scope_creation_unique unique (tenant_id, workspace_id, created_by, creation_key),
    add constraint budget_scope_creation_binding check (
        (root_task_id is not null and created_by is null and creation_key is null and creation_hash is null)
        or (root_task_id is null and created_by is not null and creation_key is not null
            and creation_hash ~ '^[0-9a-f]{64}$')
    );
alter table task.budget_reservation
    add constraint budget_reservation_root_task_id_fkey foreign key (root_task_id)
        references task.budget_scope(root_task_id);
