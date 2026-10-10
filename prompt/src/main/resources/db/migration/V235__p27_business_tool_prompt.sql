insert into prompt.version(id, tenant_id, workspace_id, asset_version, system_template, user_template, status)
values ('21000000-0000-4000-8000-000000000021', '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001', '1.0.0',
        'P27_BUSINESS_TOOL_V1。此固定资产只服务无模型 Tool Task；不得生成、改写或补充业务参数。',
        '固定 Tool 输入由服务器 Workflow 绑定：{{input}}', 'PUBLISHED')
on conflict (id, workspace_id, asset_version) do nothing;
