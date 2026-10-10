package io.eaf.policy.infrastructure;

import io.eaf.agent.api.AgentCatalog;
import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.identity.api.DelegationResourceAuthorizer;
import io.eaf.identity.api.IdentityService;
import io.eaf.organization.api.OrganizationDirectory;
import io.eaf.policy.api.PolicyCustomerGrantAdministration;
import io.eaf.policy.api.PolicyDecision;
import io.eaf.policy.api.PolicyRequest;
import io.eaf.policy.api.PolicyService;
import io.eaf.policy.api.ServiceRequestPolicyRequest;
import io.eaf.shared.ActorType;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.UUID;
import java.util.Set;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcPolicyService implements PolicyService, DelegationResourceAuthorizer, PolicyCustomerGrantAdministration {
    private static final String READ_VERSION = "p2-v1";
    private static final String WRITE_VERSION = "p4-v2";
    private static final String MANAGE_ACTION = "policy:customer-grants:manage";
    private static final UUID SERVICE_REQUEST_REGISTRATION_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-00000000000f");
    private static final UUID P27_BUSINESS_TOOL_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000021");
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final AgentCatalog agents;
    private final ToolCatalog tools;
    private final IdentityService identities;
    private final OrganizationDirectory organizations;
    private final AuditPort audit;

    public JdbcPolicyService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces, AgentCatalog agents,
                             ToolCatalog tools, IdentityService identities, OrganizationDirectory organizations,
                             AuditPort audit) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.agents = agents;
        this.tools = tools;
        this.identities = identities;
        this.organizations = organizations;
        this.audit = audit;
    }

    @Override
    @Transactional
    public int grantCustomers(ActorContext administrator, UUID workspaceId, UUID subjectId, Set<String> customerIds) {
        requireCustomerGrantManager(administrator, workspaceId, subjectId);
        organizations.lockActiveMembership(administrator.tenantId(), administrator.actorId());
        organizations.lockActiveMembership(administrator.tenantId(), subjectId);
        requireCustomerGrantManager(administrator, workspaceId, subjectId);
        var requested = validateCustomerIds(customerIds);
        for (var customerId : requested) {
            if (!workspaces.isAuthorized(administrator.tenantId(), administrator.actorId(), workspaceId, "crm:customer:read")
                    || !hasCustomerGrant(administrator.tenantId(), workspaceId, administrator.actorId(), customerId))
                throw EafException.forbidden("Policy 管理员只能转授自己当前可读取的客户资源。");
        }
        var changed = 0;
        for (var customerId : requested) {
            changed += jdbc.update("insert into policy.customer_grant as current_grant(tenant_id, workspace_id, actor_id, customer_id, status) "
                            + "values (?, ?, ?, ?, 'ACTIVE') on conflict (tenant_id, workspace_id, actor_id, customer_id) "
                            + "do update set status = 'ACTIVE' where current_grant.status <> 'ACTIVE'",
                    administrator.tenantId(), workspaceId, subjectId, customerId);
        }
        if (changed > 0) appendGrantAudit(administrator, workspaceId, subjectId, "policy.customer_grant.add", changed);
        return changed;
    }

    @Override
    @Transactional
    public int revokeCustomers(ActorContext administrator, UUID workspaceId, UUID subjectId, Set<String> customerIds) {
        requireCustomerGrantManager(administrator, workspaceId, subjectId);
        organizations.lockActiveMembership(administrator.tenantId(), administrator.actorId());
        organizations.lockActiveMembership(administrator.tenantId(), subjectId);
        requireCustomerGrantManager(administrator, workspaceId, subjectId);
        var requested = validateCustomerIds(customerIds);
        var changed = 0;
        for (var customerId : requested) {
            changed += jdbc.update("update policy.customer_grant set status = 'REVOKED' "
                            + "where tenant_id = ? and workspace_id = ? and actor_id = ? and customer_id = ? and status = 'ACTIVE'",
                    administrator.tenantId(), workspaceId, subjectId, customerId);
        }
        if (changed > 0) appendGrantAudit(administrator, workspaceId, subjectId, "policy.customer_grant.revoke", changed);
        return changed;
    }

    @Override
    @Transactional
    public int revokeAllForTenantMember(ActorContext tenantAdministrator, UUID tenantId, UUID subjectId) {
        if (tenantAdministrator == null || tenantAdministrator.type() != ActorType.HUMAN || tenantAdministrator.delegated()
                || tenantId == null || subjectId == null || !tenantId.equals(tenantAdministrator.tenantId())
                || !organizations.isTenantAdministrator(tenantId, tenantAdministrator.actorId())
                || organizations.membership(tenantId, subjectId).isEmpty())
            throw EafException.forbidden("只有当前租户的已登记 HUMAN 管理员可以清理客户资源授权。");
        var changed = jdbc.update("update policy.customer_grant set status = 'REVOKED' "
                        + "where tenant_id = ? and actor_id = ? and status = 'ACTIVE'",
                tenantId, subjectId);
        if (changed > 0) appendGrantAudit(tenantAdministrator, null, subjectId, "policy.customer_grant.revoke_all", changed);
        return changed;
    }

    @Override
    public PolicyDecision evaluate(PolicyRequest request) {
        if (request == null || request.actor() == null) return deny("身份上下文缺失。");
        if (isP27BusinessTool(request.toolName())) return evaluateP27BusinessTool(request);
        if (request.customerId() == null || request.customerId().isBlank() || request.customerId().length() > 160)
            return deny("customerId 无效。");
        var actor = request.actor();
        if (actor.delegated()) {
            if (actor.type() != ActorType.AGENT || !request.workspaceId().equals(actor.delegationWorkspaceId())
                    || !identities.resolveDelegation(actor.tenantId(), actor.principalId(), actor.actorId(),
                    actor.delegationId(), actor.delegationWorkspaceId(), IdentityService.REST_AUDIENCE)
                    .filter(current -> current.authorizationHash().equals(actor.authorizationHash()))
                    .filter(current -> current.can("task:create")).isPresent())
                return deny("委托身份已过期、撤销或授权已变化。");
            if ("WRITE".equals(request.effect())) return deny("受托 Agent 仅允许读取操作。");
        } else if (actor.type() != ActorType.HUMAN) {
            return deny("当前身份不能执行测试 CRM 操作。");
        }
        if (!workspaces.isAuthorized(actor.tenantId(), actor.principalIdOrActorId(), request.workspaceId(), "task:create")
                || (actor.delegated() && !workspaces.isAuthorized(actor.tenantId(), actor.actorId(), request.workspaceId(), "task:create")))
            return deny("Workspace 执行授权已撤销。");
        var tool = tools.requirePublished(actor.tenantId(), request.workspaceId(), request.toolName(), request.toolVersion());
        if (!tool.effect().equals(request.effect())) return deny("工具声明的 effect 与请求不一致。");
        // USER reviewer 与合成评测 reviewer 使用不同 Tool 和权限；评测 peer 只接受 EVALUATION Task。
        boolean remoteReview = "agent.risk.review".equals(tool.name()) && "agent:risk-review".equals(tool.permissionAction());
        boolean evaluationReviewer = "agent.risk.review.evaluation".equals(tool.name())
                && "agent:risk-review-evaluation".equals(tool.permissionAction());
        if ("READ".equals(request.effect()) && !remoteReview && !evaluationReviewer
                && (!"crm.customer.query".equals(tool.name()) || !"crm:customer:read".equals(tool.permissionAction())))
            return deny("读取工具动作不在治理范围。");
        boolean createFollowupWrite = "crm.followup.create".equals(tool.name())
                && "crm:followup:create".equals(tool.permissionAction());
        boolean resultFollowupWrite = "crm.followup.result.record".equals(tool.name())
                && "crm:followup:result".equals(tool.permissionAction());
        if ("WRITE".equals(request.effect()) && !createFollowupWrite && !resultFollowupWrite)
            return deny("写入工具动作不在治理范围。");
        if (!"READ".equals(request.effect()) && !"WRITE".equals(request.effect())) return deny("未知 effect 默认拒绝。");
        var bound = agents.tools(actor.tenantId(), request.workspaceId(), request.agentId(), request.agentVersion()).stream()
                .anyMatch(t -> t.name().equals(tool.name()) && t.version().equals(tool.version()));
        if (!bound) return deny("Agent 版本未允许该工具。");
        if (actor.delegated() && (!actor.can(tool.permissionAction())
                || ("crm:customer:read".equals(tool.permissionAction())
                && !identities.allowsDelegatedCustomerRead(actor, request.customerId()))))
            return deny("委托未包含当前工具或客户资源。");
        if ("EVALUATION".equals(request.taskSource())) {
            // 隔离 peer 只能在 EVALUATION 来源和独立动作授权下提供建议；真实外部连接仍由 Connector 禁用状态阻断。
            if (evaluationReviewer && "READ".equals(request.effect())) {
                if (!workspaces.isAuthorized(actor.tenantId(), actor.principalIdOrActorId(), request.workspaceId(), "agent:risk-review-evaluation")
                        || actor.delegated() && (!actor.can("agent:risk-review-evaluation")
                        || !workspaces.isAuthorized(actor.tenantId(), actor.actorId(), request.workspaceId(), "agent:risk-review-evaluation")))
                    return deny("当前身份没有隔离评测 reviewer 的权限。");
                return new PolicyDecision(true, READ_VERSION, "EVALUATION_SANDBOX_REVIEW");
            }
            // 评测只切换为合成数据源，常规客户查询仍执行资源授权；写入和 USER reviewer 均拒绝。
            if ("READ".equals(request.effect()) && "crm.customer.query".equals(tool.name())
                    && "crm:customer:read".equals(tool.permissionAction())) {
                if (!hasCustomerGrant(request)) return deny("当前身份没有该客户资源的读取权限。");
                return new PolicyDecision(true, READ_VERSION, "EVALUATION_SANDBOX");
            }
            return deny("EVALUATION_WRITE_DENIED");
        }
        if (!"USER".equals(request.taskSource())) return deny("未知 Task 来源默认拒绝工具访问。");
        if (remoteReview && (!workspaces.isAuthorized(actor.tenantId(), actor.principalIdOrActorId(), request.workspaceId(), "agent:risk-review")
                || actor.delegated() && (!actor.can("agent:risk-review")
                || !workspaces.isAuthorized(actor.tenantId(), actor.actorId(), request.workspaceId(), "agent:risk-review"))))
            return deny("当前身份没有固定远端 reviewer 的权限。");
        if ("READ".equals(request.effect()) && !hasCustomerGrant(request)) return deny("当前身份没有该客户资源的读取权限。");
        if ("WRITE".equals(request.effect())) {
            var writeAction = createFollowupWrite ? "crm:followup:create" : "crm:followup:result";
            if (!workspaces.isAuthorized(actor.tenantId(), actor.principalIdOrActorId(), request.workspaceId(), writeAction))
                return deny("当前身份没有对应 CRM 跟进写入权限。");
            if (!hasCustomerGrant(request)) return deny("当前身份没有该客户资源的跟进权限。");
        }
        return new PolicyDecision(true, "WRITE".equals(request.effect()) ? WRITE_VERSION : READ_VERSION, "ALLOW");
    }

    private PolicyDecision evaluateP27BusinessTool(PolicyRequest request) {
        var actor = request.actor();
        if (actor.type() != ActorType.HUMAN || actor.delegated() || request.workspaceId() == null
                || !"USER".equals(request.taskSource()) || !P27_BUSINESS_TOOL_AGENT_ID.equals(request.agentId())
                || !"1.0.0".equals(request.agentVersion()) || !"1.0.0".equals(request.toolVersion()))
            return deny("P27 Tool 只接受本人 HUMAN 的固定 USER Workflow 版本。");
        var tool = tools.requirePublished(actor.tenantId(), request.workspaceId(), request.toolName(), request.toolVersion());
        var expected = switch (request.toolName()) {
            case "oa.todo.list", "oa.todo.get" -> new P27PolicyBinding("READ", "oa:todo:read", "p27-oa.todo");
            case "service.request.status.get" -> new P27PolicyBinding("READ", "service-request:status:read", "p27-service-desk.result");
            case "service.request.result.record" -> new P27PolicyBinding("WRITE", "service-request:result:sync", "p27-service-desk.result");
            default -> null;
        };
        if (expected == null || !expected.effect().equals(request.effect()) || !expected.effect().equals(tool.effect())
                || !expected.action().equals(tool.permissionAction()) || !expected.bindingRef().equals(tool.bindingRef()))
            return deny("P27 Tool 与固定动作或连接绑定不匹配。");
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), request.workspaceId(), "task:create")
                || !workspaces.isAuthorized(actor.tenantId(), actor.actorId(), request.workspaceId(), expected.action()))
            return deny("Workspace 未授权当前 P27 Tool 动作。");
        var bound = agents.tools(actor.tenantId(), request.workspaceId(), request.agentId(), request.agentVersion())
                .stream().anyMatch(binding -> binding.name().equals(tool.name()) && binding.version().equals(tool.version()));
        return bound ? new PolicyDecision(true, "P27-BUSINESS-TOOL-V1", "ALLOW")
                : deny("固定 P27 Agent 未绑定当前 Tool。");
    }

    private boolean isP27BusinessTool(String name) {
        return Set.of("oa.todo.list", "oa.todo.get", "service.request.status.get", "service.request.result.record")
                .contains(name);
    }

    private record P27PolicyBinding(String effect, String action, String bindingRef) { }

    @Override
    public PolicyDecision evaluateServiceRequest(ServiceRequestPolicyRequest request) {
        if (request == null || request.actor() == null || request.workspaceId() == null
                || request.actor().type() != ActorType.HUMAN || request.actor().delegated()) return deny("本人 HUMAN 身份缺失或不受支持。");
        var actor = request.actor();
        if (!"USER".equals(request.taskSource()) || !"WRITE".equals(request.effect())
                || !SERVICE_REQUEST_REGISTRATION_AGENT_ID.equals(request.agentId()) || !"1.0.0".equals(request.agentVersion())
                || !"service.request.register".equals(request.toolName()) || !"1.0.0".equals(request.toolVersion()))
            return deny("服务请求只接受 USER 来源与固定登记 Tool/Agent 版本。");
        if (!workspaces.isAuthorized(actor.tenantId(), actor.principalIdOrActorId(), request.workspaceId(), "task:create")
                || !workspaces.isAuthorized(actor.tenantId(), actor.principalIdOrActorId(), request.workspaceId(), "service-request:submit"))
            return deny("Workspace 未授权服务请求登记。");
        final var tool = tools.requirePublished(actor.tenantId(), request.workspaceId(), request.toolName(), request.toolVersion());
        if (!"WRITE".equals(tool.effect()) || !"service-request:submit".equals(tool.permissionAction())
                || !"p15-service-desk.register".equals(tool.bindingRef()))
            return deny("服务请求 Tool 声明与固定服务台动作不匹配。");
        boolean bound = agents.tools(actor.tenantId(), request.workspaceId(), request.agentId(), request.agentVersion())
                .stream().anyMatch(binding -> binding.name().equals(tool.name()) && binding.version().equals(tool.version()));
        return bound ? new PolicyDecision(true, "P15-WRITE-V1", "ALLOW") : deny("登记 Agent 版本未绑定固定 Tool。");
    }

    private boolean hasCustomerGrant(PolicyRequest request) {
        var actor = request.actor();
        if (actor.delegated()) {
            var allowed = jdbc.queryForObject("select count(distinct actor_id) from policy.customer_grant where tenant_id = ? and workspace_id = ? and actor_id in (?, ?) and customer_id = ? and status = 'ACTIVE'",
                    Integer.class, actor.tenantId(), request.workspaceId(), actor.principalId(), actor.actorId(), request.customerId());
            return allowed != null && allowed == 2 && identities.allowsDelegatedCustomerRead(actor, request.customerId());
        }
        var allowed = jdbc.queryForObject("select count(*) from policy.customer_grant where tenant_id = ? and workspace_id = ? and actor_id = ? and customer_id = ? and status = 'ACTIVE'",
                Integer.class, actor.tenantId(), request.workspaceId(), actor.actorId(), request.customerId());
        return allowed != null && allowed > 0;
    }

    @Override
    public boolean mayDelegateCustomerRead(UUID tenantId, UUID ownerId, UUID delegateId, UUID workspaceId, String customerId) {
        var allowed = jdbc.queryForObject("select count(distinct actor_id) from policy.customer_grant where tenant_id = ? and workspace_id = ? and actor_id in (?, ?) and customer_id = ? and status = 'ACTIVE'",
                Integer.class, tenantId, workspaceId, ownerId, delegateId, customerId);
        return allowed != null && allowed == 2
                && workspaces.isAuthorized(tenantId, ownerId, workspaceId, "task:create")
                && workspaces.isAuthorized(tenantId, delegateId, workspaceId, "crm:customer:read");
    }

    private void requireCustomerGrantManager(ActorContext administrator, UUID workspaceId, UUID subjectId) {
        if (administrator == null || administrator.type() != ActorType.HUMAN || administrator.delegated()
                || workspaceId == null || subjectId == null)
            throw EafException.forbidden("只有本人操作的 HUMAN Policy 管理员可以维护客户授权。");
        workspaces.require(administrator, workspaceId, MANAGE_ACTION);
        if (!organizations.isActiveMember(administrator.tenantId(), subjectId)) throw EafException.notFound();
    }

    private Set<String> validateCustomerIds(Set<String> customerIds) {
        if (customerIds == null || customerIds.isEmpty() || customerIds.size() > 100
                || customerIds.stream().anyMatch(customerId -> customerId == null || customerId.isBlank()
                || customerId.length() > 160 || customerId.chars().anyMatch(Character::isISOControl)))
            throw EafException.invalid("客户授权必须包含 1 至 100 个有效客户标识。");
        return Set.copyOf(customerIds);
    }

    private boolean hasCustomerGrant(UUID tenantId, UUID workspaceId, UUID actorId, String customerId) {
        var count = jdbc.queryForObject("select count(*) from policy.customer_grant where tenant_id = ? and workspace_id = ? "
                        + "and actor_id = ? and customer_id = ? and status = 'ACTIVE'",
                Integer.class, tenantId, workspaceId, actorId, customerId);
        return count != null && count > 0;
    }

    // 审计只记录变化规模和目标主体，不把客户清单复制到审计事件。
    private void appendGrantAudit(ActorContext administrator, UUID workspaceId, UUID subjectId, String action, int count) {
        audit.append(new AuditFact(UUID.randomUUID().toString(), administrator.tenantId(), workspaceId,
                administrator.actorId(), null, action, "CHANGED", "{\"subjectId\":\"" + subjectId
                + "\",\"grantCount\":" + count + "}", null));
    }

    private PolicyDecision deny(String reason) { return new PolicyDecision(false, READ_VERSION, reason); }
}
// 本文件负责实现 EAF 的 JdbcPolicyService.java 相关代码。
