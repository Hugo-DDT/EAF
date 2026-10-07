-- Task 自存 Workflow 步骤来源，供 Execution 在评测 reviewer 出站前验证精确实例与步骤。
alter table task.task
    add column workflow_instance_id uuid,
    add column workflow_id uuid,
    add column workflow_version varchar(40),
    add column workflow_step_id varchar(120),
    add constraint task_workflow_provenance_complete check (
        (workflow_instance_id is null and workflow_id is null and workflow_version is null and workflow_step_id is null)
        or (workflow_instance_id is not null and workflow_id is not null
            and workflow_version is not null and workflow_step_id is not null)
    );

comment on column task.task.workflow_instance_id is '由 Workflow 传入的不透明实例 ID，用于执行来源审计，不建立跨域外键。';
comment on column task.task.workflow_id is '创建该 Task 的固定 Workflow 定义 ID。';
comment on column task.task.workflow_version is '创建该 Task 的不可变 Workflow 版本。';
comment on column task.task.workflow_step_id is '创建该 Task 的固定 Workflow 步骤 ID。';
