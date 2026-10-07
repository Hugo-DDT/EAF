package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.approval.api.ApprovalDecisionCommand;
import io.eaf.approval.api.ApprovalService;
import io.eaf.audit.api.AuditPort;
import io.eaf.capability.api.CapabilityService;
import io.eaf.context.api.ContextService;
import io.eaf.context.api.EvaluationContextSnapshotReader;
import io.eaf.execution.api.ExecutionService;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelResult;
import io.eaf.model.api.ModelToolCall;
import io.eaf.observability.api.TraceRecorder;
import io.eaf.identity.api.IdentityAdministration;
import io.eaf.provisioning.api.MembershipProvisioning;
import io.eaf.prompt.api.PromptCatalog;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.usage.api.UsageRecorder;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceMembershipAdministration;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.model.scenario=WRITE"})
// 本地集成回归验证批准后的成员停用会阻断已排队的恢复执行。
class P7MembershipRevocationTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final UUID ALICE = UUID.fromString("80000000-0000-4000-8000-000000000001");
    private static final UUID BOB = UUID.fromString("80000000-0000-4000-8000-000000000002");
    private static final UUID AGENT = UUID.fromString("20000000-0000-4000-8000-000000000001");
    private static final String ISSUER = "https://p7-idp.example.test";
    private static final AtomicInteger CRM_POSTS = new AtomicInteger();

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
    private static HttpServer crm;

    @BeforeAll
    static void startLoopbackCrm() throws Exception {
        crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/customers/customer-001", exchange -> {
            if (!"Bearer p7-membership-test-credential".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }
            // 预览阶段需要合成客户事实；只有后续写入 endpoint 才用于观察 CRM 副作用。
            var body = "{\"customerId\":\"customer-001\",\"renewalStatus\":\"ACTIVE\",\"lastContactDate\":\"2026-09-20\",\"complaintSummary\":\"none\",\"sourceId\":\"p7-membership-fixture\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.createContext("/followups", exchange -> {
            CRM_POSTS.incrementAndGet();
            var body = "{\"operationId\":\"loopback\",\"externalId\":\"fu-loopback\",\"customerId\":\"customer-001\",\"summary\":\"local\",\"ownerId\":\"owner\",\"status\":\"CREATED\",\"acceptedAt\":\"2026-10-01T00:00:00Z\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(201, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.start();
    }

    @AfterAll
    static void stopLoopbackCrm() {
        if (crm != null) crm.stop(0);
    }

    @BeforeEach
    void resetFixture() {
        CRM_POSTS.set(0);
        jdbc.update("update organization.member set status = 'ACTIVE' where tenant_id = ? and subject_id = ?", TENANT, ALICE);
        for (var action : List.of("agent:read", "task:create", "task:read", "task:cancel", "task:resume",
                "approval:read", "execution:read", "tool:read", "crm:customer:read", "crm:followup:create")) {
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                            + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                    TENANT, WORKSPACE, ALICE, action);
        }
        jdbc.update("insert into policy.customer_grant(tenant_id, workspace_id, actor_id, customer_id, status) "
                + "values (?, ?, ?, 'customer-001', 'ACTIVE') on conflict (tenant_id, workspace_id, actor_id, customer_id) do update set status = 'ACTIVE'",
                TENANT, WORKSPACE, ALICE);
        // 测试通过受保护部署路径登记初始租户管理员，并授予其有限的管理与资源范围。
        jdbc.update("insert into organization.tenant_admin(tenant_id, subject_id, status) values (?, ?, 'ACTIVE') "
                + "on conflict (tenant_id, subject_id) do update set status = 'ACTIVE', updated_at = now()", TENANT, BOB);
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                TENANT, WORKSPACE, BOB, "workspace:members:manage");
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                TENANT, WORKSPACE, BOB, "policy:customer-grants:manage");
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                TENANT, WORKSPACE, BOB, "crm:customer:read");
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                TENANT, WORKSPACE, BOB, "task:read");
        jdbc.update("insert into policy.customer_grant(tenant_id, workspace_id, actor_id, customer_id, status) "
                + "values (?, ?, ?, 'customer-001', 'ACTIVE') on conflict (tenant_id, workspace_id, actor_id, customer_id) do update set status = 'ACTIVE'",
                TENANT, WORKSPACE, BOB);
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? "
                + "and actor_id = ? and action = 'workspace:members:manage'", TENANT, WORKSPACE, ALICE);
    }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.credentials.test-crm.token", () -> "p7-membership-test-credential");
        registry.add("eaf.security.enterprise.issuer", () -> ISSUER);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired TaskService tasks;
    @Autowired ApprovalService approvals;
    @Autowired ExecutionService executions;
    @Autowired AgentCatalog agents;
    @Autowired PromptCatalog prompts;
    @Autowired AuditPort audit;
    @Autowired UsageRecorder usage;
    @Autowired TraceRecorder traces;
    @Autowired ToolCatalog tools;
    @Autowired ContextService contexts;
    @Autowired ObjectMapper json;
    @Autowired Clock clock;
    @Autowired EvaluationContextSnapshotReader evaluationContexts;
    @Autowired CapabilityService capabilities;
    @Autowired WorkspaceAuthorization workspaces;
    @Autowired MembershipProvisioning provisioning;
    @Autowired WorkspaceMembershipAdministration workspaceMemberships;
    @Autowired IdentityAdministration identities;

    @Test
    void membershipLifecycleIsIdempotentAndCannotExpandTheAdministratorGrant() {
        var bob = actor(BOB, "workspace:members:manage", "policy:customer-grants:manage", "crm:customer:read");
        var grants = java.util.Map.of(WORKSPACE, Set.of("task:read"));
        var customers = java.util.Map.of(WORKSPACE, Set.of("customer-001"));

        var created = provisioning.onboardHuman(bob, TENANT, "新成员", ISSUER, "external-new-member", grants, customers);
        var changedFacts = jdbc.queryForObject("select count(*) from audit.audit_event where tenant_id = ? and actor_id = ? "
                        + "and action in ('organization.member.add', 'identity.external_identity.bind', 'workspace.member.grant', 'policy.customer_grant.add')",
                Integer.class, TENANT, BOB);
        var repeated = provisioning.onboardHuman(bob, TENANT, "显示名称不会覆盖", ISSUER, "external-new-member", grants, customers);

        assertThat(created.created()).isTrue();
        assertThat(repeated.created()).isFalse();
        assertThat(repeated.actorId()).isEqualTo(created.actorId());
        assertThat(workspaces.isAuthorized(TENANT, created.actorId(), WORKSPACE, "task:read")).isTrue();
        assertThat(jdbc.queryForObject("select count(*) from \"identity\".external_identity where issuer = ? and subject = ? and actor_id = ? and status = 'ACTIVE'",
                Integer.class, ISSUER, "external-new-member", created.actorId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where tenant_id = ? and actor_id = ? "
                        + "and action in ('organization.member.add', 'identity.external_identity.bind', 'workspace.member.grant', 'policy.customer_grant.add')",
                Integer.class, TENANT, BOB)).isEqualTo(changedFacts);
        identities.appointTenantAdministrator(bob, TENANT, created.actorId());
        assertThat(jdbc.queryForObject("select count(*) from organization.tenant_admin where tenant_id = ? and subject_id = ? and status = 'ACTIVE'",
                Integer.class, TENANT, created.actorId())).isEqualTo(1);
        assertThatThrownBy(() -> identities.appointTenantAdministrator(bob, TENANT, AGENT))
                .isInstanceOf(EafException.class);

        provisioning.offboardMember(bob, TENANT, created.actorId());
        assertThat(jdbc.queryForObject("select status from \"identity\".external_identity where issuer = ? and subject = ?",
                String.class, ISSUER, "external-new-member")).isEqualTo("DISABLED");
        assertThat(jdbc.queryForObject("select status from policy.customer_grant where tenant_id = ? and workspace_id = ? "
                        + "and actor_id = ? and customer_id = 'customer-001'", String.class, TENANT, WORKSPACE, created.actorId()))
                .isEqualTo("REVOKED");
        provisioning.offboardMember(bob, TENANT, created.actorId());
        var rejoined = provisioning.onboardHuman(bob, TENANT, "新成员重入", ISSUER, "external-new-member",
                java.util.Map.of(), java.util.Map.of());
        assertThat(rejoined.actorId()).isEqualTo(created.actorId());
        assertThat(rejoined.created()).isFalse();
        assertThat(workspaces.isAuthorized(TENANT, rejoined.actorId(), WORKSPACE, "task:read")).isFalse();
        assertThat(jdbc.queryForObject("select count(*) from organization.tenant_admin where tenant_id = ? and subject_id = ? and status = 'ACTIVE'",
                Integer.class, TENANT, rejoined.actorId())).isZero();
        assertThat(jdbc.queryForObject("select status from policy.customer_grant where tenant_id = ? and workspace_id = ? "
                        + "and actor_id = ? and customer_id = 'customer-001'", String.class, TENANT, WORKSPACE, rejoined.actorId()))
                .isEqualTo("REVOKED");

        var factsBeforeDeniedGrant = jdbc.queryForObject("select count(*) from audit.audit_event where tenant_id = ? and actor_id = ?",
                Integer.class, TENANT, BOB);
        assertThatThrownBy(() -> provisioning.onboardHuman(bob, TENANT, "越权成员", ISSUER, "external-overgrant",
                java.util.Map.of(WORKSPACE, Set.of("task:create")), java.util.Map.of()))
                .isInstanceOf(EafException.class);
        assertThatThrownBy(() -> provisioning.onboardHuman(bob, TENANT, "错误 issuer", "https://attacker.example.test",
                "external-attacker", java.util.Map.of(), java.util.Map.of()))
                .isInstanceOf(EafException.class);
        assertThat(workspaces.isAuthorized(TENANT, created.actorId(), WORKSPACE, "task:create")).isFalse();
        assertThat(jdbc.queryForObject("select count(*) from \"identity\".external_identity where subject = 'external-overgrant'",
                Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from \"identity\".subject where display_name = '越权成员'",
                Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where tenant_id = ? and actor_id = ?",
                Integer.class, TENANT, BOB)).isEqualTo(factsBeforeDeniedGrant);
    }

    @Test
    void concurrentManagerRevocationsCannotRemoveTheLastWorkspaceAdministrator() throws Exception {
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                + "values (?, ?, ?, 'workspace:members:manage', 'ACTIVE') "
                + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                TENANT, WORKSPACE, ALICE);
        var alice = actor(ALICE, "workspace:members:manage");
        var bob = actor(BOB, "workspace:members:manage");
        var start = new CountDownLatch(1);

        // 两个有效管理员同时尝试撤销对方；租户/Workspace 行锁应使第二个请求重验后拒绝。
        try (var pool = Executors.newFixedThreadPool(2)) {
            var revokeAlice = pool.submit(() -> {
                start.await();
                try {
                    workspaceMemberships.revoke(bob, WORKSPACE, ALICE, Set.of("workspace:members:manage"));
                    return true;
                } catch (EafException rejected) {
                    return false;
                }
            });
            var revokeBob = pool.submit(() -> {
                start.await();
                try {
                    workspaceMemberships.revoke(alice, WORKSPACE, BOB, Set.of("workspace:members:manage"));
                    return true;
                } catch (EafException rejected) {
                    return false;
                }
            });
            start.countDown();
            var changed = (revokeAlice.get(15, TimeUnit.SECONDS) ? 1 : 0)
                    + (revokeBob.get(15, TimeUnit.SECONDS) ? 1 : 0);
            assertThat(changed).isEqualTo(1);
        }
        assertThat(jdbc.queryForObject("select count(*) from workspace.\"grant\" g "
                        + "join organization.member m on m.tenant_id = g.tenant_id and m.subject_id = g.actor_id "
                        + "where g.tenant_id = ? and g.workspace_id = ? and g.action = 'workspace:members:manage' "
                        + "and g.status = 'ACTIVE' and m.status = 'ACTIVE'",
                Integer.class, TENANT, WORKSPACE)).isEqualTo(1);
    }

    @Test
    void revokingInitiatorAfterApprovalStopsQueuedExecutionBeforeTheModelOrCrm() {
        // CRM 地址固定到本地 stub；只有真正越过 Execution 写入边界才会增加请求计数。
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://127.0.0.1:" + crm.getAddress().getPort(), TENANT, WORKSPACE);
        var alice = actor(ALICE, "task:create", "task:read", "task:resume", "approval:read", "execution:read", "tool:read");
        var bob = actor(BOB, "approval:read", "approval:decide", "execution:read");
        var modelCalls = new AtomicInteger();
        ModelGateway gateway = new ModelGateway() {
            @Override
            public ModelResult call(io.eaf.model.api.ModelRequest request) {
                modelCalls.incrementAndGet();
                return new ModelResult("local-fixture", "fixture-model", null, 20, 5, "KNOWN",
                        List.of(new ModelToolCall("call-p7-revocation", "crm.followup.create",
                                "{\"customerId\":\"customer-001\",\"summary\":\"安排一次客户跟进。\"}")), "TOOL_CALLS");
            }

            @Override public int callCount() { return modelCalls.get(); }
        };
        var runtime = new io.eaf.agentruntime.infrastructure.JdbcAgentRuntime(jdbc, agents, prompts, gateway, audit,
                usage, traces, tasks, tools, executions, contexts, json, clock, evaluationContexts, capabilities);
        var created = tasks.create(new CreateTaskCommand(alice, WORKSPACE, AGENT, "2.0.0",
                "请为 customerId=customer-001 创建跟进。", null, null,
                "p7-member-revoke-after-approval", "trace-p7-member-revoke-after-approval", "USER"));
        var initialWork = tasks.claimOne().orElseThrow();
        var pending = runtime.run(initialWork);
        tasks.complete(initialWork, pending);
        assertThat(pending.status()).as("code=%s detail=%s", pending.errorCode(), pending.errorDetail())
                .isEqualTo(io.eaf.task.api.TaskStatus.WAITING_APPROVAL);
        assertThat(CRM_POSTS).hasValue(0);

        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, created.id());
        var execution = executions.get(alice, WORKSPACE, executionId);
        var approval = approvals.get(bob, WORKSPACE, execution.approvalId());
        approvals.decide(new ApprovalDecisionCommand(bob, WORKSPACE, approval.id(), "APPROVED", approval.version()));
        var waiting = tasks.get(alice, WORKSPACE, created.id());
        tasks.resume(alice, WORKSPACE, created.id(), waiting.version(), "p7-member-revoke-resume");

        var staleWork = tasks.claimOne().orElseThrow();
        // 在 Task 已排队并领取后调用跨域停用 API，模拟撤权提交与 Worker 开始之间的竞态窗口。
        provisioning.offboardMember(bob, TENANT, ALICE);
        provisioning.offboardMember(bob, TENANT, ALICE);
        var rejected = runtime.run(staleWork);
        tasks.complete(staleWork, rejected);

        assertThat(rejected.status()).isEqualTo(io.eaf.task.api.TaskStatus.FAILED);
        assertThat(rejected.errorCode()).isEqualTo("AUTHORIZATION_REVOKED");
        assertThat(modelCalls).hasValue(1);
        assertThat(CRM_POSTS).hasValue(0);
        assertThat(workspaces.isAuthorized(TENANT, ALICE, WORKSPACE, "task:create")).isFalse();
        assertThat(workspaces.actions(TENANT, ALICE, WORKSPACE)).isEmpty();
        assertThat(jdbc.queryForObject("select status from policy.customer_grant where tenant_id = ? and workspace_id = ? and actor_id = ? and customer_id = 'customer-001'",
                String.class, TENANT, WORKSPACE, ALICE)).isEqualTo("REVOKED");
        assertThat(jdbc.queryForObject("select status from task.task where id = ?", String.class, created.id())).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("select status from execution.execution where id = ?", String.class, executionId))
                .isEqualTo("AWAITING_APPROVAL");
        assertThatThrownBy(() -> workspaces.require(alice, WORKSPACE, "task:read")).isInstanceOf(EafException.class);
    }

    private static ActorContext actor(UUID id, String... actions) {
        return new ActorContext(id, TENANT, ActorType.HUMAN, Set.of(actions));
    }
}
