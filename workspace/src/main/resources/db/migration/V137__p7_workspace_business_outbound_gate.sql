-- 本迁移扩展 Workspace 运维闸门，明确区分新 Task 接收与新业务出站动作。
alter table workspace.operational_gate drop constraint operational_gate_gate_name_check;
alter table workspace.operational_gate add constraint operational_gate_gate_name_check
    check (gate_name in ('TASK_ADMISSION', 'BUSINESS_OUTBOUND'));

alter table workspace.operations_command drop constraint operations_command_gate_name_check;
alter table workspace.operations_command add constraint operations_command_gate_name_check
    check (gate_name in ('TASK_ADMISSION', 'BUSINESS_OUTBOUND'));

-- 既有 Workspace 默认允许业务出站；新 gate 在关闭后由 Execution Owner 执行检查。
insert into workspace.operational_gate(tenant_id, workspace_id, gate_name, enabled, changed_by, changed_at, row_version, command_id)
select tenant_id, id, 'BUSINESS_OUTBOUND', true, null, now(), 1, null
from workspace.workspace;
