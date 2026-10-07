insert into tool.version(tenant_id, workspace_id, name, asset_version, description, input_schema,
                        output_schema, permission_action, effect, binding_ref, status)
values ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
        'service.request.register', '1.0.0', '将已确认的内部服务请求经独立审批登记到固定合成服务台。',
        '{"type":"object","required":["submissionId"],"properties":{"submissionId":{"type":"string","maxLength":36,"minLength":36}},"additionalProperties":false}',
        '{"type":"object","required":["requestId","operationId","status","requesterId","category","title","summary","handlingSuggestion","sourceTaskId","sourceResultHash"],"properties":{"requestId":{"type":"string"},"operationId":{"type":"string"},"status":{"type":"string"},"requesterId":{"type":"string"},"category":{"type":"string"},"title":{"type":"string"},"summary":{"type":"string"},"handlingSuggestion":{"type":"string"},"sourceTaskId":{"type":"string"},"sourceResultHash":{"type":"string"}},"additionalProperties":false}',
        'service-request:submit', 'WRITE', 'p15-service-desk.register', 'PUBLISHED')
on conflict (tenant_id, workspace_id, name, asset_version) do nothing;
