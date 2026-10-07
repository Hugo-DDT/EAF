package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.EafException;
import io.eaf.execution.api.ExecutionService;
import io.eaf.task.api.TaskService;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workflow.api.CreateWorkflowCommand;
import io.eaf.workflow.api.CreateWorkflowInstanceCommand;
import io.eaf.workflow.api.CreateWorkflowVersionCommand;
import io.eaf.workflow.api.WorkflowDefinition;
import io.eaf.workflow.api.WorkflowDependency;
import io.eaf.workflow.api.WorkflowInstance;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.api.WorkflowStepSpec;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** REST 只负责认证上下文与 DTO 转换；Workflow 域继续执行 Owner、Workspace、版本和幂等校验。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/workflows")
public class WorkflowController {
    private final WorkflowService workflows;
    private final ObjectMapper json;
    private final TaskService tasks;
    private final ExecutionService executions;
    private final WorkspaceAuthorization workspaces;

    public WorkflowController(WorkflowService workflows, ObjectMapper json, TaskService tasks,
                              ExecutionService executions, WorkspaceAuthorization workspaces) {
        this.workflows = workflows;
        this.json = json;
        this.tasks = tasks;
        this.executions = executions;
        this.workspaces = workspaces;
    }

    @GetMapping
    WorkflowList list(@PathVariable UUID workspaceId, Authentication authentication) {
        return new WorkflowList(workflows.list(ApiSupport.actor(authentication), workspaceId).stream()
                .map(WorkflowView::of).toList(), null);
    }

    // 请求不能指定 Owner；领域服务从认证身份确定 Owner 并校验创建权限。
    @PostMapping
    ResponseEntity<WorkflowView> create(@PathVariable UUID workspaceId, @RequestBody Map<String, Object> raw,
                                        Authentication authentication) {
        var body = convert(raw, CreateBody.class,
                Set.of("name", "description", "version", "inputSchema", "outputSchema", "entryStepId", "steps"));
        var version = version(body.version(), body.inputSchema(), body.outputSchema(), body.entryStepId(), body.steps());
        var created = workflows.create(new CreateWorkflowCommand(ApiSupport.actor(authentication), workspaceId,
                body.name(), body.description(), version));
        return ResponseEntity.status(HttpStatus.CREATED).body(WorkflowView.of(created));
    }

    @PostMapping("/{workflowId}/versions")
    WorkflowView addVersion(@PathVariable UUID workspaceId, @PathVariable UUID workflowId,
                            @RequestBody Map<String, Object> raw, Authentication authentication) {
        var body = convert(raw, VersionBody.class,
                Set.of("version", "inputSchema", "outputSchema", "entryStepId", "steps"));
        return WorkflowView.of(workflows.addVersion(ApiSupport.actor(authentication), workspaceId, workflowId,
                version(body.version(), body.inputSchema(), body.outputSchema(), body.entryStepId(), body.steps())));
    }

    @GetMapping("/{workflowId}/versions/{version}")
    WorkflowView get(@PathVariable UUID workspaceId, @PathVariable UUID workflowId, @PathVariable String version,
                     Authentication authentication) {
        return WorkflowView.of(workflows.get(ApiSupport.actor(authentication), workspaceId, workflowId, version));
    }

    @PostMapping("/{workflowId}/versions/{version}/publish")
    WorkflowView publish(@PathVariable UUID workspaceId, @PathVariable UUID workflowId, @PathVariable String version,
                         @RequestParam long expectedVersion, Authentication authentication) {
        return WorkflowView.of(workflows.publish(ApiSupport.actor(authentication), workspaceId, workflowId, version, expectedVersion));
    }

    @PostMapping("/{workflowId}/versions/{version}/withdraw")
    WorkflowView withdraw(@PathVariable UUID workspaceId, @PathVariable UUID workflowId, @PathVariable String version,
                          @RequestParam long expectedVersion, Authentication authentication) {
        return WorkflowView.of(workflows.withdraw(ApiSupport.actor(authentication), workspaceId, workflowId, version, expectedVersion));
    }

    // Actor、source 和输入摘要都由可信身份与 Workflow 域派生；请求体只允许业务输入和固定版本。
    @PostMapping("/{workflowId}/instances")
    ResponseEntity<WorkflowInstanceView> createInstance(@PathVariable UUID workspaceId, @PathVariable UUID workflowId,
                                                        @RequestHeader("Idempotency-Key") String idempotencyKey,
                                                        @RequestBody Map<String, Object> raw,
                                                        Authentication authentication) {
        var body = convert(raw, InstanceBody.class, Set.of("version", "input", "deadlineAt"));
        if (body.input() == null) throw EafException.invalid("Workflow input 不能为空。");
        var actor = ApiSupport.actor(authentication);
        var instance = workflows.createInstance(new CreateWorkflowInstanceCommand(actor,
                workspaceId, workflowId, body.version(), write(body.input()), idempotencyKey, "USER", body.deadlineAt()));
        return ResponseEntity.accepted().body(instanceView(actor, workspaceId, instance));
    }

    @GetMapping("/{workflowId}/instances/{instanceId}")
    WorkflowInstanceView getInstance(@PathVariable UUID workspaceId, @PathVariable UUID workflowId,
                                     @PathVariable UUID instanceId, Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        return instanceView(actor, workspaceId, instance(actor, workspaceId, workflowId, instanceId));
    }

    @PostMapping("/{workflowId}/instances/{instanceId}/cancel")
    WorkflowInstanceView cancelInstance(@PathVariable UUID workspaceId, @PathVariable UUID workflowId,
                                        @PathVariable UUID instanceId, @RequestBody CancelBody body,
                                        Authentication authentication) {
        var actor = ApiSupport.actor(authentication);
        var current = instance(actor, workspaceId, workflowId, instanceId);
        return instanceView(actor, workspaceId,
                workflows.cancelInstance(actor, workspaceId, current.id(), body.expectedVersion()));
    }

    // 只在调用者也有 Task 可见权时返回续办关联键；审批 ID 仅向有审批读取权的 HUMAN 暴露。
    private WorkflowInstanceView instanceView(io.eaf.shared.ActorContext actor, UUID workspaceId,
                                              WorkflowInstance instance) {
        UUID childTaskId = null;
        Long childTaskVersion = null;
        UUID approvalId = null;
        if (instance.childTaskId() != null) {
            try {
                var child = tasks.get(actor, workspaceId, instance.childTaskId());
                childTaskId = child.id();
                childTaskVersion = child.version();
                if (actor.type() == io.eaf.shared.ActorType.HUMAN
                        && workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "approval:read"))
                    approvalId = executions.pending(actor.tenantId(), workspaceId, child.id(), child.attempt())
                            .filter(pending -> "AWAITING_APPROVAL".equals(pending.status()))
                            .map(io.eaf.execution.api.ExecutionSnapshot::approvalId).orElse(null);
            } catch (EafException e) {
                // Workflow 状态仍可见时不因无独立 Task 读取权泄露其 Task 关联。
                if (e.status() != 403 && e.status() != 404) throw e;
            }
        }
        return WorkflowInstanceView.of(instance, childTaskId, childTaskVersion, approvalId);
    }

    private WorkflowInstance instance(io.eaf.shared.ActorContext actor, UUID workspaceId, UUID workflowId, UUID instanceId) {
        var instance = workflows.getInstance(actor, workspaceId, instanceId);
        if (!workflowId.equals(instance.workflowId())) throw EafException.notFound();
        return instance;
    }

    private CreateWorkflowVersionCommand version(String version, String inputSchema, String outputSchema,
                                                 String entryStepId, List<WorkflowStepSpec> steps) {
        return new CreateWorkflowVersionCommand(version, inputSchema, outputSchema, entryStepId, steps);
    }

    private <T> T convert(Map<String, Object> raw, Class<T> type, Set<String> allowed) {
        if (raw == null || raw.keySet().stream().anyMatch(key -> !allowed.contains(key)))
            throw EafException.invalid("Workflow 请求包含无效或未允许字段。");
        try { return json.convertValue(raw, type); }
        catch (IllegalArgumentException e) { throw EafException.invalid("Workflow 请求字段类型无效。"); }
    }

    private String write(JsonNode value) {
        try { return json.writeValueAsString(value); }
        catch (Exception e) { throw EafException.invalid("Workflow input 无法解析。"); }
    }

    record WorkflowList(List<WorkflowView> items, String nextCursor) { }
    record CreateBody(String name, String description, String version, String inputSchema, String outputSchema,
                      String entryStepId, List<WorkflowStepSpec> steps) { }
    record VersionBody(String version, String inputSchema, String outputSchema, String entryStepId,
                       List<WorkflowStepSpec> steps) { }
    record InstanceBody(String version, JsonNode input, Instant deadlineAt) { }
    record CancelBody(long expectedVersion) { }
    record WorkflowView(UUID id, UUID ownerId, String name, String description, String version, String status,
                        long rowVersion, String inputSchema, String outputSchema, String entryStepId,
                        List<WorkflowStepSpec> steps, List<WorkflowDependency> dependencies, String contentHash) {
        static WorkflowView of(WorkflowDefinition definition) {
            return new WorkflowView(definition.id(), definition.ownerId(), definition.name(), definition.description(),
                    definition.version(), definition.status(), definition.rowVersion(), definition.inputSchema(),
                    definition.outputSchema(), definition.entryStepId(), definition.steps(), definition.dependencies(),
                    definition.contentHash());
        }
    }
    record WorkflowInstanceView(UUID id, UUID workflowId, String workflowVersion, String status, String currentStepId,
                                String errorCode, long rowVersion, Instant createdAt, Instant deadlineAt,
                                String waitingReason, String businessEffectStatus, UUID childTaskId,
                                Long childTaskVersion, UUID approvalId) {
        static WorkflowInstanceView of(WorkflowInstance instance, UUID childTaskId, Long childTaskVersion,
                                       UUID approvalId) {
            return new WorkflowInstanceView(instance.id(), instance.workflowId(), instance.workflowVersion(),
                    instance.status(), instance.currentStepId(), instance.errorCode(), instance.rowVersion(),
                    instance.createdAt(), instance.deadlineAt(), instance.waitingReason(), instance.businessEffectStatus(),
                    childTaskId, childTaskVersion, approvalId);
        }
    }
}
