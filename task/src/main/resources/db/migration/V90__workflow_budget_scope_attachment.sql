-- Workflow 首个业务 Task 成为真实预算根；保留预算范围来源键用于实例幂等恢复。
alter table task.budget_scope drop constraint budget_scope_creation_binding;
alter table task.budget_scope add constraint budget_scope_creation_binding check (
    (created_by is null and creation_key is null and creation_hash is null)
    or (created_by is not null and creation_key is not null and creation_hash ~ '^[0-9a-f]{64}$')
);
