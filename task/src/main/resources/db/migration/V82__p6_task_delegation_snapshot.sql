-- Task 固定签发时的委托引用、受托身份与授权摘要；跨域只存 ID，不加外键。
alter table task.task
    add column principal_id uuid,
    add column delegate_id uuid,
    add column delegation_id uuid,
    add column authorization_hash varchar(64),
    add constraint task_delegation_snapshot_all_or_none check (
        (principal_id is null and delegate_id is null and delegation_id is null and authorization_hash is null)
        or (principal_id is not null and delegate_id is not null and delegation_id is not null and length(authorization_hash) = 64)
    );
