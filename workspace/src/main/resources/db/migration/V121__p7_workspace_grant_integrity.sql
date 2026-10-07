-- Workspace grant 只能指向同租户的本域空间，防止错误租户 ID 形成可见性混淆。
alter table workspace."grant"
    add constraint workspace_grant_workspace_fk foreign key (tenant_id, workspace_id)
        references workspace.workspace(tenant_id, id);
