-- P4 为写入意图、审批绑定和 UNKNOWN 恢复补齐持久字段；历史 READ 记录用自身 id 作为稳定 operationId。
alter table execution.execution drop constraint if exists execution_execution_status_check;
alter table execution.execution add constraint execution_execution_status_check check (status in ('RECEIVED','VALIDATING','READY','AWAITING_APPROVAL','EXECUTING','VERIFYING','SUCCEEDED','DENIED','FAILED','UNKNOWN','VERIFICATION_FAILED','CANCELLED'));
alter table execution.execution add column if not exists operation_id uuid;
update execution.execution set operation_id = id where operation_id is null;
alter table execution.execution alter column operation_id set not null;
alter table execution.execution add column if not exists connector_id uuid;
alter table execution.execution add column if not exists connector_version varchar(80);
alter table execution.execution add column if not exists preview_json jsonb;
alter table execution.execution add column if not exists preview_hash varchar(64);
alter table execution.execution add column if not exists approval_id uuid;
alter table execution.execution add column if not exists verification_json jsonb;
alter table execution.execution add column if not exists row_version bigint not null default 1;
alter table execution.execution add column if not exists lease_until timestamptz;
create unique index if not exists execution_operation_scope on execution.execution(tenant_id, workspace_id, operation_id);
create index if not exists execution_approval_state on execution.execution(approval_id, status);

-- P4 API 的最小测试授权；正式部署由身份与角色目录配置，不能从模型建议推导。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', action, 'ACTIVE'
from unnest(array['task:resume','execution:read','execution:verify','approval:read']) action
on conflict (workspace_id, actor_id, action) do nothing;
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000002', 'execution:read', 'ACTIVE'
on conflict (workspace_id, actor_id, action) do nothing;
