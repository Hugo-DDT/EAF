insert into workflow.definition(id, tenant_id, workspace_id, owner_id, name, description)
values
 ('58000000-0000-4000-8000-000000000019', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', 'p27-oa-todo-list', '通过固定只读 Tool 查询本人 OA 待办页。'),
 ('58000000-0000-4000-8000-00000000001a', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', 'p27-oa-todo-item', '通过固定只读 Tool 查询本人 OA 待办单项。'),
 ('58000000-0000-4000-8000-00000000001b', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', 'p27-service-request-state', '读取已核验 P16 服务请求的当前服务台状态。'),
 ('58000000-0000-4000-8000-00000000001c', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '80000000-0000-4000-8000-000000000001', 'p27-service-request-result-sync', '独立审批后追加 P16 人工处理结果并按原操作身份核验。')
on conflict (id, workspace_id) do nothing;

insert into workflow.version(workflow_id, tenant_id, workspace_id, asset_version, input_schema, output_schema,
                             entry_step_id, steps_json, content_hash, status, row_version)
values
 ('58000000-0000-4000-8000-000000000019', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '1.0.0',
  '{"type":"object","required":["status","cursor","limit"],"additionalProperties":false,"properties":{"status":{"type":"string","maxLength":20},"cursor":{"type":"string","maxLength":512},"limit":{"type":"integer"}}}',
  '{"type":"object","required":["bindingVersion","page"],"additionalProperties":false,"properties":{"bindingVersion":{"type":"string"},"page":{"type":"object"}}}',
  'read', '[{"id":"read","type":"RUN_TOOL","nextStepId":"complete","capabilityId":"54000000-0000-4000-8000-000000000022","capabilityVersion":"1.0.0","toolName":"oa.todo.list","toolVersion":"1.0.0","inputMapping":{"status":"$.input.status","cursor":"$.input.cursor","limit":"$.input.limit"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"complete","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"bindingVersion":"$.steps.read.output.bindingVersion","page":"$.steps.read.output.page"}}]'::jsonb, null, 'DRAFT', 1),
 ('58000000-0000-4000-8000-00000000001a', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '1.0.0',
  '{"type":"object","required":["todoId"],"additionalProperties":false,"properties":{"todoId":{"type":"string","minLength":1,"maxLength":160}}}',
  '{"type":"object","required":["bindingVersion","found"],"additionalProperties":false,"properties":{"bindingVersion":{"type":"string"},"found":{"type":"boolean"}}}',
  'read', '[{"id":"read","type":"RUN_TOOL","nextStepId":"complete","capabilityId":"54000000-0000-4000-8000-000000000022","capabilityVersion":"1.0.0","toolName":"oa.todo.get","toolVersion":"1.0.0","inputMapping":{"todoId":"$.input.todoId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"complete","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"bindingVersion":"$.steps.read.output.bindingVersion","found":"$.steps.read.output.found"}}]'::jsonb, null, 'DRAFT', 1),
 ('58000000-0000-4000-8000-00000000001b', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '1.0.0',
  '{"type":"object","required":["workItemId"],"additionalProperties":false,"properties":{"workItemId":{"type":"string","minLength":36,"maxLength":36}}}',
  '{"type":"object","required":["bindingVersion","state"],"additionalProperties":false,"properties":{"bindingVersion":{"type":"string"},"state":{"type":"object"}}}',
  'read', '[{"id":"read","type":"RUN_TOOL","nextStepId":"complete","capabilityId":"54000000-0000-4000-8000-000000000022","capabilityVersion":"1.0.0","toolName":"service.request.status.get","toolVersion":"1.0.0","inputMapping":{"workItemId":"$.input.workItemId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"complete","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"bindingVersion":"$.steps.read.output.bindingVersion","state":"$.steps.read.output.state"}}]'::jsonb, null, 'DRAFT', 1),
 ('58000000-0000-4000-8000-00000000001c', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '1.0.0',
  '{"type":"object","required":["syncId"],"additionalProperties":false,"properties":{"syncId":{"type":"string","minLength":36,"maxLength":36}}}',
  '{"type":"object","required":["operationId","requestId","registrationOperationId","resultId","workItemId","workItemVersion","sourceResultHash","externalSubjectId","completedBy","completedAt","outcome","summary","recordState","previousExternalVersion","resultingExternalVersion","resultingStatus","acceptedAt"],"additionalProperties":false,"properties":{"operationId":{"type":"string"},"requestId":{"type":"string"},"registrationOperationId":{"type":"string"},"resultId":{"type":"string"},"workItemId":{"type":"string"},"workItemVersion":{"type":"integer"},"sourceResultHash":{"type":"string"},"externalSubjectId":{"type":"string"},"completedBy":{"type":"string"},"completedAt":{"type":"string"},"outcome":{"type":"string"},"summary":{"type":"string"},"recordState":{"type":"string"},"previousExternalVersion":{"type":"string"},"resultingExternalVersion":{"type":"string"},"resultingStatus":{"type":"string"},"acceptedAt":{"type":"string"}}}',
  'record', '[{"id":"record","type":"RUN_TOOL","nextStepId":"complete","capabilityId":"54000000-0000-4000-8000-000000000022","capabilityVersion":"1.0.0","toolName":"service.request.result.record","toolVersion":"1.0.0","inputMapping":{"syncId":"$.input.syncId"},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{}},{"id":"complete","type":"COMPLETE","nextStepId":null,"capabilityId":null,"capabilityVersion":null,"toolName":null,"toolVersion":null,"inputMapping":{},"conditionPath":null,"conditionValue":null,"whenTrueStepId":null,"whenFalseStepId":null,"outputMapping":{"operationId":"$.steps.record.output.operationId","requestId":"$.steps.record.output.requestId","registrationOperationId":"$.steps.record.output.registrationOperationId","resultId":"$.steps.record.output.resultId","workItemId":"$.steps.record.output.workItemId","workItemVersion":"$.steps.record.output.workItemVersion","sourceResultHash":"$.steps.record.output.sourceResultHash","externalSubjectId":"$.steps.record.output.externalSubjectId","completedBy":"$.steps.record.output.completedBy","completedAt":"$.steps.record.output.completedAt","outcome":"$.steps.record.output.outcome","summary":"$.steps.record.output.summary","recordState":"$.steps.record.output.recordState","previousExternalVersion":"$.steps.record.output.previousExternalVersion","resultingExternalVersion":"$.steps.record.output.resultingExternalVersion","resultingStatus":"$.steps.record.output.resultingStatus","acceptedAt":"$.steps.record.output.acceptedAt"}}]'::jsonb, null, 'DRAFT', 1)
on conflict (workflow_id, workspace_id, asset_version) do nothing;

insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version,
                                            capability_id, capability_version, content_hash)
select '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001',
       workflow_id, '1.0.0', '54000000-0000-4000-8000-000000000022', '1.0.0', null
from (values ('58000000-0000-4000-8000-000000000019'::uuid), ('58000000-0000-4000-8000-00000000001a'::uuid),
             ('58000000-0000-4000-8000-00000000001b'::uuid), ('58000000-0000-4000-8000-00000000001c'::uuid)) w(workflow_id)
on conflict do nothing;

update workflow.capability_dependency set content_hash = 'aa7ba842a57cb162eabd0d720ec31f2a3cfaf03d6d028ac5ebc56efa088c8708'
where workflow_id in ('58000000-0000-4000-8000-000000000019', '58000000-0000-4000-8000-00000000001a',
                      '58000000-0000-4000-8000-00000000001b', '58000000-0000-4000-8000-00000000001c')
  and workflow_version = '1.0.0';

update workflow.version v set status = 'PUBLISHED', content_hash = h.content_hash, row_version = 2
from (values
 ('58000000-0000-4000-8000-000000000019'::uuid, 'd0de3577d3e637dc8e9cc8c6d4aa2e56dc48e46ed32d96b332a7d6b82b5b7997'),
 ('58000000-0000-4000-8000-00000000001a'::uuid, '998666cbd099054627b001414e38772160345feca99fdb53650db92727faf336'),
 ('58000000-0000-4000-8000-00000000001b'::uuid, '1404ed1622636164f51f3f3a8a294c2585fb7de2928eb5197ab1fc3a5ce75901'),
 ('58000000-0000-4000-8000-00000000001c'::uuid, 'c760e8765af106694594a1c7cda9d5ce954c4f0ab4c2a13f37dc90e2925e3b14')
) h(workflow_id, content_hash)
where v.workflow_id = h.workflow_id and v.workspace_id = '10000000-0000-4000-8000-000000000001'
  and v.asset_version = '1.0.0' and v.status = 'DRAFT';

insert into workflow.release(release_id, tenant_id, workspace_id, workflow_id, workflow_version, action, actor_id)
values
 ('59000000-0000-4000-8000-000000000291', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-000000000019', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
 ('59000000-0000-4000-8000-000000000292', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-00000000001a', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
 ('59000000-0000-4000-8000-000000000293', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-00000000001b', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001'),
 ('59000000-0000-4000-8000-000000000294', '70000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '58000000-0000-4000-8000-00000000001c', '1.0.0', 'PUBLISHED', '80000000-0000-4000-8000-000000000001')
on conflict (tenant_id, workspace_id, workflow_id, workflow_version, action) do nothing;
