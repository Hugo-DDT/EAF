-- 复核 Tool 作为单独 Capability 版本暴露；已发布的 1.0.0 依赖保持不变。
insert into capability.version(capability_id, tenant_id, workspace_id, asset_version, agent_id, agent_version,
                               skill_id, skill_version, prompt_id, prompt_version, evaluation_ref, status)
select '54000000-0000-4000-8000-000000000001', '70000000-0000-4000-8000-000000000001',
       '10000000-0000-4000-8000-000000000001', '1.1.0', '20000000-0000-4000-8000-000000000001', '2.0.0',
       '53000000-0000-4000-8000-000000000001', '1.0.0',
       '21000000-0000-4000-8000-000000000001', '2.0.0', 'p2-v1', 'DRAFT'
where not exists (select 1 from capability.version where capability_id = '54000000-0000-4000-8000-000000000001'
                  and asset_version = '1.1.0');

insert into capability.tool_dependency(tenant_id, workspace_id, capability_id, capability_version,
                                       tool_name, tool_version, input_schema, output_schema)
select v.tenant_id, v.workspace_id, '54000000-0000-4000-8000-000000000001', '1.1.0',
       v.name, v.asset_version, v.input_schema, v.output_schema
from tool.version v
where v.tenant_id = '70000000-0000-4000-8000-000000000001'
  and v.workspace_id = '10000000-0000-4000-8000-000000000001'
  and ((v.name = 'crm.customer.query' and v.asset_version = '1.0.0')
    or (v.name = 'agent.risk.review' and v.asset_version = '1.0.0'))
  and not exists (select 1 from capability.tool_dependency d where d.capability_id = '54000000-0000-4000-8000-000000000001'
                  and d.capability_version = '1.1.0' and d.tool_name = v.name and d.tool_version = v.asset_version);
