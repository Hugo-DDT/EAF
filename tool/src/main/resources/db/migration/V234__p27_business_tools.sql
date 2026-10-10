insert into tool.version(tenant_id, workspace_id, name, asset_version, description, input_schema,
                        output_schema, permission_action, effect, binding_ref, status)
values
 ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
  'oa.todo.list', '1.0.0', '读取当前员工获准的 OA 待办页。',
  '{"type":"object","required":["status","cursor","limit"],"additionalProperties":false,"properties":{"status":{"type":"string","maxLength":20},"cursor":{"type":"string","maxLength":512},"limit":{"type":"integer"}}}',
  '{"type":"object","required":["bindingVersion","page"],"additionalProperties":false,"properties":{"bindingVersion":{"type":"string"},"page":{"type":"object"}}}',
  'oa:todo:read', 'READ', 'p27-oa.todo', 'PUBLISHED'),
 ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
  'oa.todo.get', '1.0.0', '读取当前员工获准的 OA 单项待办。',
  '{"type":"object","required":["todoId"],"additionalProperties":false,"properties":{"todoId":{"type":"string","minLength":1,"maxLength":160}}}',
  '{"type":"object","required":["bindingVersion","found"],"additionalProperties":true,"properties":{"bindingVersion":{"type":"string"},"found":{"type":"boolean"}}}',
  'oa:todo:read', 'READ', 'p27-oa.todo', 'PUBLISHED'),
 ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
  'service.request.status.get', '1.0.0', '读取已核验 P16 服务请求的当前服务台状态。',
  '{"type":"object","required":["workItemId"],"additionalProperties":false,"properties":{"workItemId":{"type":"string","minLength":36,"maxLength":36}}}',
  '{"type":"object","required":["bindingVersion","state"],"additionalProperties":false,"properties":{"bindingVersion":{"type":"string"},"state":{"type":"object"}}}',
  'service-request:status:read', 'READ', 'p27-service-desk.result', 'PUBLISHED'),
 ('70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
  'service.request.result.record', '1.0.0', '经独立审批追加保存的 P16 人工处理结果。',
  '{"type":"object","required":["syncId"],"additionalProperties":false,"properties":{"syncId":{"type":"string","minLength":36,"maxLength":36}}}',
  '{"type":"object","required":["operationId","requestId","registrationOperationId","resultId","workItemId","workItemVersion","sourceResultHash","externalSubjectId","completedBy","completedAt","outcome","summary","recordState","previousExternalVersion","resultingExternalVersion","resultingStatus","acceptedAt"],"additionalProperties":true,"properties":{"operationId":{"type":"string"},"requestId":{"type":"string"},"registrationOperationId":{"type":"string"},"resultId":{"type":"string"},"workItemId":{"type":"string"},"workItemVersion":{"type":"integer"},"sourceResultHash":{"type":"string"},"externalSubjectId":{"type":"string"},"completedBy":{"type":"string"},"completedAt":{"type":"string"},"outcome":{"type":"string"},"summary":{"type":"string"},"recordState":{"type":"string"},"previousExternalVersion":{"type":"string"},"resultingExternalVersion":{"type":"string"},"resultingStatus":{"type":"string"},"acceptedAt":{"type":"string"}}}',
  'service-request:result:sync', 'WRITE', 'p27-service-desk.result', 'PUBLISHED')
on conflict (tenant_id, workspace_id, name, asset_version) do nothing;
