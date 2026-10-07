-- P22 报告读取/复核与已发布只读资产解析分别授权；运行可见性仍由 Evaluation 限定为本人运行。
insert into workspace."grant"(tenant_id, workspace_id, actor_id, action, status)
select g.tenant_id, g.workspace_id, g.actor_id, permitted.action, 'ACTIVE'
from workspace."grant" g
cross join unnest(array['evaluation:read', 'evaluation:review', 'capability:read', 'prompt:read', 'skill:read']) as permitted(action)
where g.action = 'evaluation:run' and g.status = 'ACTIVE'
on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE', tenant_id = excluded.tenant_id;
