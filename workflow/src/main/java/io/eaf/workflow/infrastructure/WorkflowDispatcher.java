package io.eaf.workflow.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.identity.api.IdentityService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.task.api.CreateWorkflowTaskCommand;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.workflow.api.WorkflowDefinition;
import io.eaf.workflow.api.WorkflowInstance;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.api.WorkflowStepSpec;
import io.eaf.workflow.api.WorkflowStepType;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class WorkflowDispatcher {
    private static final Logger LOG = Logger.getLogger(WorkflowDispatcher.class.getName());
    private static final Pattern INPUT_PATH = Pattern.compile("\\$\\.input\\.([A-Za-z][A-Za-z0-9_]*)");
    private static final Pattern STEP_PATH = Pattern.compile("\\$\\.steps\\.([a-z][a-z0-9-]{0,63})\\.output\\.([A-Za-z][A-Za-z0-9_]*)");
    private final JdbcWorkflowService workflows;
    private final TaskService tasks;
    private final CapabilityService capabilities;
    private final IdentityService identities;
    private final WorkspaceAuthorization workspaces;
    private final ObjectMapper json;
    private final boolean enabled;

    public WorkflowDispatcher(JdbcWorkflowService workflows, TaskService tasks, CapabilityService capabilities,
                              IdentityService identities, WorkspaceAuthorization workspaces, ObjectMapper json,
                              @Value("${eaf.workflow.dispatcher-enabled:true}") boolean enabled) {
        this.workflows = workflows;
        this.tasks = tasks;
        this.capabilities = capabilities;
        this.identities = identities;
        this.workspaces = workspaces;
        this.json = json;
        this.enabled = enabled;
    }

    @Scheduled(fixedDelayString = "${eaf.workflow.poll-delay:500}")
    public void dispatch() {
        if (enabled) dispatchOne();
    }

    public boolean dispatchOne() {
        var lease = workflows.claimOne();
        if (lease.isEmpty()) return false;
        advance(lease.get());
        return true;
    }

    public void advance(WorkflowLease lease) {
        WorkflowRuntimeState state = null;
        try {
            state = workflows.runtimeState(lease);
            if (state == null) return;
            var instance = state.instance();
            if ("CANCELLING".equals(instance.status())) {
                advanceCancellation(instance, lease);
                return;
            }
            var actor = currentActor(state);
            workflows.requireRunnable(actor, instance.workspaceId(), instance.id());
            var definition = readDefinition(instance.definitionSnapshot());
            var stepId = instance.currentStepId() == null ? definition.entryStepId() : instance.currentStepId();
            var step = definition.steps().stream().filter(candidate -> candidate.id().equals(stepId)).findFirst()
                    .orElseThrow(() -> EafException.conflict("WORKFLOW_STEP_MISSING", "当前 Workflow 步骤不在固定定义中。"));
            var outputs = workflows.completedOutputs(instance.id());
            var mapped = step.type() == WorkflowStepType.RUN_CAPABILITY || step.type() == WorkflowStepType.RUN_TOOL
                    || step.type() == WorkflowStepType.HUMAN_TASK
                    ? mapInput(step.inputMapping(), parse(instance.inputJson(), "input"), outputs) : json.createObjectNode();
            var mappedText = workflows.canonicalWorkflowJson(mapped);
            var dispatchKey = instance.id() + ":" + step.id();
            var inputHash = Hashing.sha256(mappedText);
            var old = workflows.stepRuntime(instance.id(), step.id());
            var runtime = workflows.prepareStep(lease, step, mappedText, inputHash, dispatchKey);
            if (old == null) {
                // 首次持久化只建立恢复点；外部 Task 一定在下一轮单独创建。
                workflows.release(lease, "RUNNING", step.id());
                return;
            }
            switch (step.type()) {
                case RUN_CAPABILITY, RUN_TOOL -> advanceExecution(instance, actor, step, runtime, lease);
                case PARALLEL_READ -> advanceParallelRead(instance, actor, step, lease,
                        parse(instance.inputJson(), "input"), outputs);
                case HUMAN_TASK -> workflows.openHumanWorkItem(lease, step.id());
                case BRANCH -> advanceBranch(step, lease, parse(instance.inputJson(), "input"), outputs);
                case COMPLETE -> advanceComplete(state, definition, step, lease, parse(instance.inputJson(), "input"), outputs);
            }
        } catch (EafException e) {
            if ("TASK_CAPACITY_EXCEEDED".equals(e.code()) && state != null) {
                // Task 队列满时保留当前步骤和 dispatchKey；下一轮以同一幂等键继续创建。
                var stepId = state.instance().currentStepId() == null
                        ? readDefinition(state.instance().definitionSnapshot()).entryStepId()
                        : state.instance().currentStepId();
                workflows.release(lease, "RUNNING", stepId);
            } else if (!"WORKFLOW_LEASE_LOST".equals(e.code())) {
                workflows.beginCancellation(lease, "DEADLINE_EXCEEDED".equals(e.code()) ? "TIMED_OUT" : "FAILED", e.code());
            }
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Workflow Worker 遇到可重试错误；租约释放后将从持久步骤恢复。", e);
            if (state != null) {
                var current = workflows.stepRuntime(lease.instanceId(), state.instance().currentStepId() == null
                        ? readDefinition(state.instance().definitionSnapshot()).entryStepId() : state.instance().currentStepId());
                workflows.release(lease, current != null && current.childTaskId() != null ? "WAITING_CHILD" : "RUNNING",
                        current == null ? null : current.stepId());
            }
        }
    }

    private void advanceExecution(WorkflowInstance instance, ActorContext actor,
                                  WorkflowStepSpec step, WorkflowStepRuntime runtime, WorkflowLease lease) {
        if (runtime.childTaskId() == null) {
            var capability = capabilities.requirePublished(actor, instance.workspaceId(), step.capabilityId(), step.capabilityVersion());
            var command = taskCommand(instance, actor, step, runtime, capability);
            if (UUID.fromString("58000000-0000-4000-8000-00000000001d").equals(instance.workflowId())) {
                workflows.createProjectBriefTaskAndLink(lease, step.id(), command);
                return;
            }
            // 查询是丢失创建响应后的恢复路径；再次走创建 API 会校验同键绑定并安全地重放。
            var existing = tasks.findByIdempotencyKey(actor, instance.workspaceId(), runtime.dispatchKey());
            var child = tasks.createWorkflowTask(command);
            if (existing.isPresent() && !existing.get().id().equals(child.id()))
                throw EafException.conflict("WORKFLOW_CHILD_CONFLICT", "Task 来源键查询与幂等创建返回了不同 Task。");
            workflows.linkChildTask(lease, step.id(), child.id(), child.rootTaskId());
            return;
        }
        var child = tasks.get(actor, instance.workspaceId(), runtime.childTaskId());
        if (child.status() == TaskStatus.SUCCEEDED) {
            var output = parse(child.resultJson(), "childTask.output");
            workflows.validateStepOutput(actor, instance.workspaceId(), step, output);
            var next = step.nextStepId();
            workflows.completeStep(lease, step.id(), workflows.canonicalWorkflowJson(output), next, "RUNNING", null,
                    child.externalEffectStatus());
        } else if (child.status() == TaskStatus.FAILED || child.status() == TaskStatus.CANCELLED
                || child.status() == TaskStatus.TIMED_OUT) {
            workflows.fail(lease, child.status() == TaskStatus.TIMED_OUT ? "TIMED_OUT" : "FAILED",
                    child.errorCode() == null ? "CHILD_TASK_" + child.status() : child.errorCode(), child.externalEffectStatus());
        } else {
            var reason = switch (child.status()) {
                case WAITING_APPROVAL -> safeReason(child.errorCode(), "APPROVAL_REQUIRED");
                case WAITING_VERIFICATION -> safeReason(child.errorCode(), "VERIFICATION_REQUIRED");
                case QUEUED -> "CHILD_TASK_QUEUED";
                default -> "CHILD_TASK_RUNNING";
            };
            workflows.releaseWaitingChild(lease, step.id(), reason, child.externalEffectStatus());
        }
    }

    private void advanceParallelRead(WorkflowInstance instance, ActorContext actor, WorkflowStepSpec step,
            WorkflowLease lease, JsonNode input, Map<String, JsonNode> outputs) {
        var intents = step.parallelBranches().stream().map(branch -> {
            var mapped = mapInput(branch.inputMapping(), input, outputs);
            var content = workflows.canonicalWorkflowJson(mapped);
            var roleStep = step.id() + "-" + branch.role();
            var dispatch = instance.id() + ":" + step.id() + ":" + branch.role();
            return new ParallelBranchIntent(branch.role(), roleStep, dispatch, content, Hashing.sha256(content),
                    branch.capabilityId(), branch.capabilityVersion());
        }).toList();
        var branches = workflows.prepareParallelBranches(lease, step, intents);
        var byRole = branches.stream().collect(java.util.stream.Collectors.toMap(
                WorkflowService.ParallelBranch::role, branch -> branch));
        // knowledge 始终先绑定根 Task；Task 本身独立排队，experience 可与其真实交叠执行。
        for (var role : java.util.List.of("knowledge", "experience")) {
            var branch = byRole.get(role);
            if (branch.childTaskId() == null) {
                var capability = capabilities.requirePublished(actor, instance.workspaceId(), branch.capabilityId(), branch.capabilityVersion());
                var binding = new io.eaf.task.api.TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                        capability.skillId(), capability.skillVersion(), capability.skillContentHash());
                var command = new CreateWorkflowTaskCommand(actor, instance.workspaceId(), instance.rootBudgetScopeId(),
                        instance.rootTaskId(), branch.dispatchKey(), capability.agentId(), capability.agentVersion(),
                        branch.inputJson(), "USER", binding, null, null, null,
                        "workflow:" + instance.id() + "@" + instance.workflowVersion() + ":" + branch.stepId(), null,
                        new io.eaf.task.api.WorkflowTaskProvenance(instance.id(), instance.workflowId(),
                                instance.workflowVersion(), branch.stepId()));
                workflows.createParallelBranchTask(lease, branch, command);
                return;
            }
        }
        var pending = false;
        for (var role : java.util.List.of("knowledge", "experience")) {
            var branch = byRole.get(role);
            var child = tasks.get(actor, instance.workspaceId(), branch.childTaskId());
            if (child.status() == TaskStatus.SUCCEEDED) {
                workflows.updateParallelBranch(lease, branch, "SUCCEEDED", null);
            } else if (child.status() == TaskStatus.FAILED || child.status() == TaskStatus.CANCELLED
                    || child.status() == TaskStatus.TIMED_OUT) {
                var status = child.status() == TaskStatus.TIMED_OUT ? "TIMED_OUT"
                        : child.status() == TaskStatus.CANCELLED ? "CANCELLED" : "FAILED";
                workflows.updateParallelBranch(lease, branch, status,
                        child.errorCode() == null ? "CHILD_TASK_" + child.status() : child.errorCode());
                workflows.beginCancellation(lease, status, child.errorCode() == null ? "PARALLEL_BRANCH_FAILED" : child.errorCode());
                return;
            } else {
                pending = true;
            }
        }
        if (pending) {
            workflows.releaseWaitingChild(lease, step.id(), "PARALLEL_BRANCH_RUNNING", "NONE");
            return;
        }
        var output = json.createObjectNode().put("knowledgeTaskId", byRole.get("knowledge").childTaskId().toString())
                .put("experienceTaskId", byRole.get("experience").childTaskId().toString());
        workflows.completeParallelRead(lease, step, workflows.canonicalWorkflowJson(output));
    }

    private void advanceCancellation(WorkflowInstance instance, WorkflowLease lease) {
        if (instance.currentStepId() != null && "PARALLEL_READ".equals(workflows.stepRuntime(
                instance.id(), instance.currentStepId()).stepType())) {
            if (!workflows.cancelParallelBranches(instance)) {
                workflows.releaseCancellation(lease, "PARALLEL_BRANCHES_STOPPING", "NONE");
                return;
            }
            var target = instance.cancelTargetStatus() == null ? "CANCELLED" : instance.cancelTargetStatus();
            workflows.finishCancellation(lease, target, instance.errorCode(), "NONE");
            return;
        }
        var cancellation = workflows.cancelCurrentChild(instance);
        WorkflowStepRuntime step = instance.currentStepId() == null ? null
                : workflows.stepRuntime(instance.id(), instance.currentStepId());
        var effect = cancellation.externalEffectStatus();
        if ("NONE".equals(effect) && step != null && "RUN_TOOL".equals(step.stepType())
                && cancellation.status() == TaskStatus.SUCCEEDED) effect = "SUCCEEDED";
        if (cancellation.externalEffectPending()) {
            workflows.releaseCancellation(lease, "EXTERNAL_EFFECT_VERIFYING", effect);
            return;
        }
        var target = instance.cancelTargetStatus() == null ? "CANCELLED" : instance.cancelTargetStatus();
        workflows.finishCancellation(lease, target, instance.errorCode(), effect);
    }

    private String safeReason(String value, String fallback) {
        return value != null && value.matches("[A-Z0-9_]{1,80}") ? value : fallback;
    }

    private CreateWorkflowTaskCommand taskCommand(WorkflowInstance instance, ActorContext actor,
                                                  WorkflowStepSpec step,
                                                  WorkflowStepRuntime runtime, CapabilityDefinition capability) {
        var binding = new io.eaf.task.api.TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        var input = step.type() == WorkflowStepType.RUN_TOOL
                ? "Workflow 固定步骤 " + step.id() : runtime.inputJson();
        var arguments = step.type() == WorkflowStepType.RUN_TOOL ? runtime.inputJson() : null;
        return new CreateWorkflowTaskCommand(actor, instance.workspaceId(), instance.rootBudgetScopeId(),
                instance.rootTaskId(), runtime.dispatchKey(), capability.agentId(), capability.agentVersion(), input,
                instance.source(), binding, step.toolName(), step.toolVersion(), arguments,
                  "workflow:" + instance.id() + "@" + instance.workflowVersion() + ":" + step.id(), instance.qualityRunId(),
                new io.eaf.task.api.WorkflowTaskProvenance(instance.id(), instance.workflowId(),
                        instance.workflowVersion(), step.id()));
    }

    private void advanceBranch(WorkflowStepSpec step, WorkflowLease lease, JsonNode input,
                               Map<String, JsonNode> outputs) {
        var value = resolve(step.conditionPath(), input, outputs);
        if (!value.isTextual()) throw EafException.invalid("Workflow BRANCH 条件运行值必须是字符串。");
        var target = value.asText().equals(step.conditionValue()) ? step.whenTrueStepId() : step.whenFalseStepId();
        workflows.completeStep(lease, step.id(), "{}", target, "RUNNING", null, "NONE");
    }

    private void advanceComplete(WorkflowRuntimeState state, WorkflowDefinition definition, WorkflowStepSpec step,
                                 WorkflowLease lease, JsonNode input, Map<String, JsonNode> outputs) {
        var output = mapInput(step.outputMapping(), input, outputs);
        workflows.validateWorkflowOutput(definition, output);
        workflows.completeStep(lease, step.id(), workflows.canonicalWorkflowJson(output), null, "SUCCEEDED", null, "NONE");
    }

    private ActorContext currentActor(WorkflowRuntimeState state) {
        var instance = state.instance();
        if (instance.delegationId() == null)
            return new ActorContext(instance.actorId(), instance.tenantId(), ActorType.HUMAN,
                    workspaces.actions(instance.tenantId(), instance.actorId(), instance.workspaceId()));
        return identities.resolveDelegation(instance.tenantId(), instance.initiatorId(), instance.actorId(),
                        instance.delegationId(), instance.workspaceId(), IdentityService.REST_AUDIENCE)
                .filter(current -> current.authorizationHash().equals(state.authorizationHash()))
                .orElseThrow(() -> EafException.forbidden("Workflow 委托已撤销、过期或授权摘要已变化。"));
    }

    private WorkflowDefinition readDefinition(String source) {
        try { return json.readValue(source, WorkflowDefinition.class); }
        catch (JsonProcessingException e) { throw EafException.conflict("WORKFLOW_SNAPSHOT_INVALID", "Workflow 固定定义快照无法解析。"); }
    }

    private ObjectNode mapInput(Map<String, String> mappings, JsonNode input, Map<String, JsonNode> outputs) {
        var result = json.createObjectNode();
        mappings.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            var value = resolve(entry.getValue(), input, outputs);
            if (value == null || value.isNull() || value.isMissingNode())
                throw EafException.invalid("Workflow 映射来源运行时缺失：" + entry.getKey());
            result.set(entry.getKey(), value.deepCopy());
        });
        return result;
    }

    private JsonNode resolve(String expression, JsonNode input, Map<String, JsonNode> outputs) {
        if ("literal:true".equals(expression)) return json.getNodeFactory().booleanNode(true);
        if ("literal:false".equals(expression)) return json.getNodeFactory().booleanNode(false);
        Matcher inputPath = INPUT_PATH.matcher(expression == null ? "" : expression);
        if (inputPath.matches()) return input.path(inputPath.group(1));
        Matcher stepPath = STEP_PATH.matcher(expression == null ? "" : expression);
        if (stepPath.matches()) return outputs.getOrDefault(stepPath.group(1), json.nullNode()).path(stepPath.group(2));
        throw EafException.invalid("Workflow 映射路径格式无效。");
    }

    private JsonNode parse(String source, String field) {
        try {
            var result = source == null ? null : json.readTree(source);
            if (result == null) throw EafException.invalid(field + " 必须是有效 JSON。");
            return result;
        } catch (JsonProcessingException e) {
            throw EafException.conflict("WORKFLOW_STEP_DATA_INVALID", field + " 持久内容无法解析。");
        }
    }
}
