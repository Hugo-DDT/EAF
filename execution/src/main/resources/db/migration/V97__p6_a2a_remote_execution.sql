-- P6 将异步 A2A 请求意图、委托/预算快照与轮询进度保存在 Execution 域，发送前先提交此行。
alter table execution.execution drop constraint if exists execution_execution_status_check;
alter table execution.execution add constraint execution_execution_status_check
    check (status in ('RECEIVED','VALIDATING','READY','AWAITING_APPROVAL','EXECUTING','VERIFYING',
                      'AWAITING_REMOTE','SUCCEEDED','DENIED','FAILED','UNKNOWN','VERIFICATION_FAILED','CANCELLED'));

create table execution.remote_a2a_operation (
    execution_id uuid primary key references execution.execution(id),
    tenant_id uuid not null,
    workspace_id uuid not null,
    task_id uuid not null,
    attempt integer not null check (attempt > 0),
    actor_id uuid not null,
    principal_id uuid not null,
    delegation_id uuid,
    authorization_hash varchar(64),
    root_task_id uuid not null,
    operation_key varchar(200) not null,
    message_id varchar(128) not null,
    request_hash varchar(64) not null,
    registration_id uuid not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    connector_id uuid not null,
    peer_skill_id varchar(120) not null,
    state varchar(24) not null check (state in ('SEND_PENDING','SEND_UNKNOWN','ACCEPTED','WORKING','COMPLETED','FAILED','UNKNOWN')),
    remote_task_id varchar(200),
    remote_context_id varchar(200),
    polls_used integer not null default 0 check (polls_used >= 0),
    poll_limit integer not null check (poll_limit between 1 and 100),
    next_poll_at timestamptz,
    poll_deadline_at timestamptz not null,
    remote_cost_status varchar(16) not null check (remote_cost_status in ('UNKNOWN','KNOWN')),
    result_hash varchar(64),
    trace_id varchar(80) not null,
    row_version bigint not null default 1,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (tenant_id, workspace_id, operation_key),
    unique (tenant_id, workspace_id, message_id),
    check ((delegation_id is null and authorization_hash is null)
        or (delegation_id is not null and authorization_hash ~ '^[0-9a-f]{64}$')),
    check ((remote_task_id is null and remote_context_id is null)
        or (remote_task_id is not null and remote_context_id is not null))
);

create index remote_a2a_poll_due on execution.remote_a2a_operation(next_poll_at, execution_id)
    where state in ('ACCEPTED','WORKING');
create index remote_a2a_task_scope on execution.remote_a2a_operation(tenant_id, workspace_id, task_id, attempt);

comment on table execution.remote_a2a_operation is '固定 reviewer 的远端请求/Task 关联、授权快照、根预算引用与有界轮询进度；不保存凭据明文。';
comment on column execution.remote_a2a_operation.request_hash is '绑定输入、调用主体、委托、Workspace、固定 reviewer、Connector 与根 Task 的摘要。';
comment on column execution.remote_a2a_operation.remote_cost_status is 'peer 不返回可核验成本时保持 UNKNOWN，不伪造零成本。';
