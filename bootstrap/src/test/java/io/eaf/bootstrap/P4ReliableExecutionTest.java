package io.eaf.bootstrap;

import com.sun.net.httpserver.HttpServer;
import io.eaf.approval.api.ApprovalDecisionCommand;
import io.eaf.approval.api.ApprovalService;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.capability.api.CapabilityService;
import io.eaf.capability.api.CapabilityToolReference;
import io.eaf.capability.api.CreateCapabilityCommand;
import io.eaf.capability.api.CreateCapabilityVersionCommand;
import io.eaf.context.api.ContextService;
import io.eaf.context.api.EvaluationContextSnapshotReader;
import io.eaf.audit.api.AuditPort;
import io.eaf.execution.api.ExecutionService;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelMessage;
import io.eaf.model.api.ModelResult;
import io.eaf.model.api.ModelToolCall;
import io.eaf.agentruntime.infrastructure.JdbcAgentRuntime;
import io.eaf.observability.api.TraceRecorder;
import io.eaf.prompt.api.PromptCatalog;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.usage.api.UsageRecorder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
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

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false", "eaf.model.scenario=WRITE"})
// 本组集成测试核对审批绑定、外部操作恢复与稳定 operationId。
class P4ReliableExecutionTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final UUID ALICE = UUID.fromString("80000000-0000-4000-8000-000000000001");
    private static final UUID BOB = UUID.fromString("80000000-0000-4000-8000-000000000002");
    private static final UUID AGENT = UUID.fromString("20000000-0000-4000-8000-000000000001");
    private static final AtomicInteger POST_COUNT = new AtomicInteger();
    private static final AtomicInteger QUERY_COUNT = new AtomicInteger();
    private static final java.util.concurrent.atomic.AtomicBoolean FAIL_AFTER_WRITE = new java.util.concurrent.atomic.AtomicBoolean();
    private static final Map<String, String> FOLLOWUPS = new ConcurrentHashMap<>();
    private static final java.util.concurrent.atomic.AtomicReference<Runnable> QUERY_AFTER_RESPONSE = new java.util.concurrent.atomic.AtomicReference<>();
    private record CompletedRuntime(UUID taskId, TaskRunner.RunOutcome outcome) { }

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
    static HttpServer crm;

    @BeforeAll
    static void startCrm() throws Exception {
        crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/customers/customer-001", exchange -> {
            QUERY_COUNT.incrementAndGet();
            if (!authorized(exchange)) { exchange.sendResponseHeaders(401, -1); exchange.close(); return; }
            var body = "{\"customerId\":\"customer-001\",\"renewalStatus\":\"ACTIVE\",\"lastContactDate\":\"2026-09-20\",\"complaintSummary\":\"none\",\"sourceId\":\"crm-test-001\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
            var afterResponse = QUERY_AFTER_RESPONSE.getAndSet(null);
            if (afterResponse != null) afterResponse.run();
        });
        crm.createContext("/followups", exchange -> {
            if (!authorized(exchange)) { exchange.sendResponseHeaders(401, -1); exchange.close(); return; }
            var request = new String(read(exchange.getRequestBody()), StandardCharsets.UTF_8);
            var operationId = jsonValue(request, "operationId");
            var customerId = jsonValue(request, "customerId");
            var summary = jsonValue(request, "summary");
            var ownerId = jsonValue(request, "ownerId");
            FOLLOWUPS.putIfAbsent(operationId, "{\"operationId\":\"" + operationId + "\",\"externalId\":\"fu-" + operationId + "\",\"customerId\":\""
                    + customerId + "\",\"summary\":\"" + summary + "\",\"ownerId\":\"" + ownerId + "\",\"status\":\"CREATED\",\"acceptedAt\":\"2026-09-23T00:00:00Z\"}");
            POST_COUNT.incrementAndGet();
            if (FAIL_AFTER_WRITE.get()) { exchange.sendResponseHeaders(500, -1); exchange.close(); return; }
            var body = FOLLOWUPS.get(operationId).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(201, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.createContext("/followups/by-operation/", exchange -> {
            if (!authorized(exchange)) { exchange.sendResponseHeaders(401, -1); exchange.close(); return; }
            var operationId = exchange.getRequestURI().getPath().substring("/followups/by-operation/".length());
            var value = FOLLOWUPS.get(operationId);
            if (value == null) { exchange.sendResponseHeaders(404, -1); exchange.close(); return; }
            var body = value.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.start();
    }

    @AfterAll
    static void stopCrm() { if (crm != null) crm.stop(0); }

    @BeforeEach
    void resetCrmState() { POST_COUNT.set(0); QUERY_COUNT.set(0); FAIL_AFTER_WRITE.set(false); FOLLOWUPS.clear(); QUERY_AFTER_RESPONSE.set(null); }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.credentials.test-crm.token", () -> "p4-test-credential");
    }

    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired ApprovalService approvals;
    @Autowired ExecutionService executions;
    @Autowired CapabilityService capabilities;
    @Autowired DataSource dataSource;
    @Autowired JdbcTemplate jdbc;
    @Autowired AgentCatalog agents;
    @Autowired PromptCatalog prompts;
    @Autowired AuditPort audit;
    @Autowired UsageRecorder usage;
    @Autowired TraceRecorder traces;
    @Autowired ToolCatalog toolCatalog;
    @Autowired ContextService contextService;
    @Autowired ObjectMapper objectMapper;
    @Autowired Clock clock;
    @Autowired EvaluationContextSnapshotReader evaluationContexts;

    @Test
    void approvalRecoveryReturnsStructuredToolTranscriptToModel() {
        pointCrm();
        var alice = actor(ALICE, "task:create", "task:read", "task:resume", "approval:read", "execution:read", "execution:verify", "tool:read");
        var bob = actor(BOB, "approval:read", "approval:decide", "execution:read");
        var secondRequest = new java.util.concurrent.atomic.AtomicReference<io.eaf.model.api.ModelRequest>();
        var calls = new AtomicInteger();
        ModelGateway gateway = new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                if (calls.incrementAndGet() == 1) {
                    assertThat(request.tools()).anySatisfy(tool -> assertThat(tool.name()).isEqualTo("crm.followup.create"));
                    return new ModelResult("local-fixture", "fixture-model", null, 20, 5, "KNOWN",
                            List.of(new ModelToolCall("call-p7-approval-1", "crm.followup.create",
                                    "{\"customerId\":\"customer-001\",\"summary\":\"安排一次客户跟进。\"}")), "TOOL_CALLS");
                }
                secondRequest.set(request);
                return new ModelResult("local-fixture", "fixture-model",
                        "{\"riskLevel\":\"LOW\",\"summary\":\"客户跟进已创建。\",\"reasons\":[\"Execution 返回已核验结果\"],\"uncertainties\":[]}",
                        30, 10, "KNOWN");
            }
            @Override public int callCount() { return calls.get(); }
        };
        var testRuntime = runtimeUsing(gateway);
        var created = tasks.create(new CreateTaskCommand(alice, WORKSPACE, AGENT, "2.0.0",
                "请为 customerId=customer-001 创建跟进。", null, null, "p7-tool-approval-roundtrip", "trace-p7-tool-approval-roundtrip", "USER"));
        var work = tasks.claimOne().orElseThrow();
        var waitingOutcome = testRuntime.run(work);
        tasks.complete(work, waitingOutcome);
        assertThat(waitingOutcome.status()).isEqualTo(io.eaf.task.api.TaskStatus.WAITING_APPROVAL);
        assertThat(POST_COUNT).hasValue(0);
        var waiting = tasks.get(alice, WORKSPACE, created.id());
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, created.id());
        var execution = executions.get(alice, WORKSPACE, executionId);
        var approval = approvals.get(bob, WORKSPACE, execution.approvalId());
        approvals.decide(new ApprovalDecisionCommand(bob, WORKSPACE, approval.id(), "APPROVED", approval.version()));

        tasks.resume(alice, WORKSPACE, created.id(), waiting.version(), "p7-tool-approval-resume");
        var resumed = tasks.claimOne().orElseThrow();
        var finalOutcome = testRuntime.run(resumed);
        tasks.complete(resumed, finalOutcome);

        assertThat(finalOutcome.status()).as("code=%s detail=%s", finalOutcome.errorCode(), finalOutcome.errorDetail())
                .isEqualTo(io.eaf.task.api.TaskStatus.SUCCEEDED);
        assertThat(POST_COUNT).hasValue(1);
        assertThat(calls).hasValue(2);
        var transcript = secondRequest.get().messages();
        // 恢复后把原调用和 Execution 结果按 ID 送回模型，并确认 CRM 写入仍只发生一次。
        assertThat(transcript).anyMatch(message -> "assistant".equals(message.role())
                && message.toolCalls().equals(List.of(new ModelToolCall("call-p7-approval-1", "crm.followup.create",
                "{\"customerId\":\"customer-001\",\"summary\":\"安排一次客户跟进。\"}"))));
        assertThat(transcript).anyMatch(message -> "tool".equals(message.role())
                && "call-p7-approval-1".equals(message.toolCallId())
                && "crm.followup.create".equals(message.toolName()) && message.content().contains("CREATED"));
        assertThat(jdbc.queryForObject("select count(*) from agent_runtime.step s join agent_runtime.run r on r.id = s.run_id where r.task_id = ? and s.type = 'MODEL_TOOL_CALLS' and s.validation = 'COMPLETE'", Integer.class, created.id())).isEqualTo(1);
    }

    @Test
    void multipleCallsKeepTheirIdsAndDeduplicateTheSameExecution() {
        // 两个不同 callId 可共享同一幂等 Execution，但模型历史仍须收到两条各自关联的结果。
        pointCrm();
        var alice = actor(ALICE, "task:create", "task:read", "tool:read");
        var secondRequest = new java.util.concurrent.atomic.AtomicReference<io.eaf.model.api.ModelRequest>();
        var calls = new AtomicInteger();
        ModelGateway gateway = new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                if (calls.incrementAndGet() == 1) return new ModelResult("local-fixture", "fixture-model", null, 20, 5, "KNOWN",
                        List.of(new ModelToolCall("call-p7-multi-1", "crm.customer.query", "{\"customerId\":\"customer-001\"}"),
                                new ModelToolCall("call-p7-multi-2", "crm.customer.query", "{\"customerId\":\"customer-001\"}")), "TOOL_CALLS");
                secondRequest.set(request);
                return new ModelResult("local-fixture", "fixture-model",
                        "{\"riskLevel\":\"LOW\",\"summary\":\"已读取客户记录。\",\"reasons\":[\"只读查询完成\"],\"uncertainties\":[]}",
                        30, 10, "KNOWN");
            }
            @Override public int callCount() { return calls.get(); }
        };
        var created = tasks.create(new CreateTaskCommand(alice, WORKSPACE, AGENT, "2.0.0",
                "请查询 customerId=customer-001。", null, null, "p7-multi-tool-call-roundtrip", "trace-p7-multi-tool-call-roundtrip", "USER"));
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtimeUsing(gateway).run(work);
        tasks.complete(work, outcome);

        assertThat(outcome.status()).as("code=%s detail=%s", outcome.errorCode(), outcome.errorDetail())
                .isEqualTo(io.eaf.task.api.TaskStatus.SUCCEEDED);
        assertThat(QUERY_COUNT).hasValue(1);
        assertThat(POST_COUNT).hasValue(0);
        assertThat(outcome.toolCalls()).isEqualTo(2);
        assertThat(outcome.toolExecutions()).isEqualTo(1);
        var transcript = secondRequest.get().messages();
        assertThat(transcript.stream().filter(message -> "assistant".equals(message.role()) && !message.toolCalls().isEmpty())
                .findFirst().orElseThrow().toolCalls()).containsExactly(
                new ModelToolCall("call-p7-multi-1", "crm.customer.query", "{\"customerId\":\"customer-001\"}"),
                new ModelToolCall("call-p7-multi-2", "crm.customer.query", "{\"customerId\":\"customer-001\"}"));
        assertThat(transcript.stream().filter(message -> "tool".equals(message.role())).map(ModelMessage::toolCallId).toList())
                .containsExactly("call-p7-multi-1", "call-p7-multi-2");
    }

    @Test
    void unknownToolAndRepeatedCallIdFailBeforeConnectorAccess() {
        // 未声明工具和重复调用 ID 都在任何 Connector 请求之前拒绝。
        pointCrm();
        var alice = actor(ALICE, "task:create", "task:read", "tool:read");
        var queries = QUERY_COUNT.get();
        var unknown = new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                return new ModelResult("local-fixture", "fixture-model", null, 10, 2, "KNOWN",
                        List.of(new ModelToolCall("call-p7-unknown-1", "crm.customer.delete", "{}")), "TOOL_CALLS");
            }
            @Override public int callCount() { return 1; }
        };
        var unknownTask = tasks.create(new CreateTaskCommand(alice, WORKSPACE, AGENT, "2.0.0", "查看客户记录。", null, null,
                "p7-unknown-tool-denied", "trace-p7-unknown-tool-denied", "USER"));
        var unknownWork = tasks.claimOne().orElseThrow();
        var unknownOutcome = runtimeUsing(unknown).run(unknownWork);
        tasks.complete(unknownWork, unknownOutcome);
        assertThat(unknownOutcome.errorCode()).isEqualTo("UNKNOWN_TOOL");
        assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from execution.execution where task_id = ?", Integer.class, unknownTask.id())).isZero();

        var repeatedId = new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                return new ModelResult("local-fixture", "fixture-model", null, 10, 2, "KNOWN",
                        List.of(new ModelToolCall("call-p7-repeated-1", "crm.customer.query", "{\"customerId\":\"customer-001\"}"),
                                new ModelToolCall("call-p7-repeated-1", "crm.customer.query", "{\"customerId\":\"customer-001\"}")), "TOOL_CALLS");
            }
            @Override public int callCount() { return 1; }
        };
        var repeatedTask = tasks.create(new CreateTaskCommand(alice, WORKSPACE, AGENT, "2.0.0", "查看客户记录。", null, null,
                "p7-repeated-tool-call-id", "trace-p7-repeated-tool-call-id", "USER"));
        var repeatedWork = tasks.claimOne().orElseThrow();
        var repeatedOutcome = runtimeUsing(repeatedId).run(repeatedWork);
        tasks.complete(repeatedWork, repeatedOutcome);
        assertThat(repeatedOutcome.errorCode()).isEqualTo("MODEL_PROTOCOL_ERROR");
        assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from execution.execution where task_id = ?", Integer.class, repeatedTask.id())).isZero();
        assertThat(QUERY_COUNT).hasValue(queries);
        assertThat(POST_COUNT).hasValue(0);
    }

    @Test
    void malformedArgumentsOrOrphanResultsOrToolLimitFailBeforeConnectorAccess() {
        // 信任边界拒绝非法 JSON、没有 Assistant 批次的结果，以及超出任务预算的整批提议。
        pointCrm();
        var alice = actor(ALICE, "task:create", "task:read", "tool:read");
        var queries = QUERY_COUNT.get();
        var malformed = runAgent(alice, toolGateway(List.of(new ModelToolCall("call-p7-invalid-json", "crm.customer.query", "{")), "TOOL_CALLS"), "p7-invalid-json");
        assertThat(malformed.outcome().errorCode()).isEqualTo("MODEL_PROTOCOL_ERROR");
        assertThat(executionCount(malformed.taskId())).isZero();

        var legacy = new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                var runId = jdbc.queryForObject("select id from agent_runtime.run where task_id = ? and attempt = 1", UUID.class, request.taskId());
                var legacyStep = jdbc.queryForObject("select coalesce(max(step_no), 0) + 100 from agent_runtime.step where run_id = ?", Integer.class, runId);
                jdbc.update("insert into agent_runtime.step(run_id, task_id, attempt, step_no, type, role, content, validation, occurred_at) "
                                + "values (?, ?, 1, ?, 'TOOL_CALL', 'assistant', '{}', 'PROPOSED', now())",
                        runId, request.taskId(), legacyStep);
                return new ModelResult("local-fixture", "fixture-model", null, 10, 2, "KNOWN",
                        List.of(new ModelToolCall("call-p7-after-legacy", "crm.customer.query", "{\"customerId\":\"customer-001\"}")), "TOOL_CALLS");
            }
            @Override public int callCount() { return 1; }
        };
        var legacyRun = runAgent(alice, legacy, "p7-legacy-tool-call");
        assertThat(legacyRun.outcome().errorCode()).isEqualTo("TOOL_CALL_HISTORY_UNAVAILABLE");
        assertThat(executionCount(legacyRun.taskId())).isZero();

        var orphan = new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                var runId = jdbc.queryForObject("select id from agent_runtime.run where task_id = ? and attempt = 1", UUID.class, request.taskId());
                var orphanStep = jdbc.queryForObject("select coalesce(max(step_no), 0) + 100 from agent_runtime.step where run_id = ?", Integer.class, runId);
                jdbc.update("insert into agent_runtime.step(run_id, task_id, attempt, step_no, type, role, content, validation, occurred_at, call_no, execution_id) "
                                + "values (?, ?, 1, ?, 'TOOL_RESULT', 'tool', '{}', 'SUCCEEDED', now(), ?, ?)",
                        runId, request.taskId(), orphanStep, request.callNo() + 1, UUID.randomUUID());
                return new ModelResult("local-fixture", "fixture-model", null, 10, 2, "KNOWN",
                        List.of(new ModelToolCall("call-p7-orphan-result", "crm.customer.query", "{\"customerId\":\"customer-001\"}")), "TOOL_CALLS");
            }
            @Override public int callCount() { return 1; }
        };
        var orphanRun = runAgent(alice, orphan, "p7-orphan-result");
        assertThat(orphanRun.outcome().errorCode()).isEqualTo("TOOL_CALL_HISTORY_UNAVAILABLE");
        assertThat(executionCount(orphanRun.taskId())).isZero();

        var overLimitCalls = java.util.stream.IntStream.range(0, 17)
                .mapToObj(index -> new ModelToolCall("call-p7-limit-" + index, "crm.customer.query", "{\"customerId\":\"customer-001\"}"))
                .toList();
        var overLimit = runAgent(alice, toolGateway(overLimitCalls, "TOOL_CALLS"), "p7-tool-limit");
        assertThat(overLimit.outcome().errorCode()).isEqualTo("TOOL_CALL_LIMIT");
        assertThat(executionCount(overLimit.taskId())).isZero();
        assertThat(QUERY_COUNT).hasValue(queries);
        assertThat(POST_COUNT).hasValue(0);
    }

    @Test
    void capabilityRevocationBetweenToolCallsStopsTheNextConnectorRequest() {
        // 第一次 CRM 查询后撤回绑定 Capability，Runtime 必须在第二次提交前重新鉴权。
        pointCrm();
        var alice = actor(ALICE, "task:create", "task:read", "tool:read", "capability:read", "capability:publish");
        var template = capabilities.requirePublished(alice, WORKSPACE,
                UUID.fromString("54000000-0000-4000-8000-000000000001"), "1.0.0");
        var draft = capabilities.create(new CreateCapabilityCommand(alice, WORKSPACE, "p7-revocation-fixture", "撤权集成用例",
                new CreateCapabilityVersionCommand("1.0.0", template.agentId(), template.agentVersion(), template.skillId(),
                        template.skillVersion(), template.promptId(), template.promptVersion(), template.toolDependencies().stream()
                        .map(tool -> new CapabilityToolReference(tool.name(), tool.version())).toList(), template.evaluationRef())));
        capabilities.publish(alice, WORKSPACE, draft.id(), draft.version(), draft.rowVersion());
        var capability = capabilities.requirePublished(alice, WORKSPACE, draft.id(), draft.version());
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        QUERY_AFTER_RESPONSE.set(() -> capabilities.revoke(alice, WORKSPACE, capability.id(), capability.version(), capability.rowVersion()));
        var gateway = toolGateway(List.of(
                new ModelToolCall("call-p7-revoke-1", "crm.customer.query", "{\"customerId\":\"customer-001\"}"),
                new ModelToolCall("call-p7-revoke-2", "crm.customer.query", "{\"customerId\":\"customer-001\"}")), "TOOL_CALLS");
        var created = tasks.create(new CreateTaskCommand(alice, WORKSPACE, AGENT, "2.0.0", "查询客户记录。", null, null,
                "p7-capability-revoke-mid-batch", "trace-p7-capability-revoke-mid-batch", "USER", binding));
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtimeUsing(gateway).run(work);
        tasks.complete(work, outcome);

        assertThat(outcome.status()).isEqualTo(io.eaf.task.api.TaskStatus.FAILED);
        assertThat(outcome.errorCode()).isEqualTo("RESOURCE_NOT_FOUND");
        assertThat(QUERY_COUNT).hasValue(1);
        assertThat(executionCount(created.id())).isEqualTo(1);
    }

    @Test
    void persistsPreviewRequiresIndependentApprovalAndWritesExactlyOnce() {
        pointCrm();
        var alice = actor(ALICE, "task:create", "task:read", "task:resume", "approval:read", "execution:read", "execution:verify", "tool:read");
        var bob = actor(BOB, "approval:read", "approval:decide", "execution:read");
        var capability = capabilities.requirePublished(alice, WORKSPACE,
                UUID.fromString("54000000-0000-4000-8000-000000000001"), "1.0.0");
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        var created = tasks.create(new CreateTaskCommand(alice, WORKSPACE, AGENT, "2.0.0",
                "请为 customerId=customer-001 创建跟进。", null, null, "p5-cap-write-1", "trace-p5-cap-write-1", "USER", binding));
        assertThat(created.assetBinding()).isEqualTo(binding);
        var work = tasks.claimOne().orElseThrow();
        var first = runtime.run(work);
        tasks.complete(work, first);
        var waiting = tasks.get(alice, WORKSPACE, created.id());
        var debugExecutions = new JdbcTemplate(dataSource).queryForList("select status, error_code, error_detail from execution.execution where task_id = ?", created.id());

        assertThat(first.status()).as("status=%s code=%s detail=%s executions=%s", first.status(), first.errorCode(), first.errorDetail(), debugExecutions)
                .isEqualTo(io.eaf.task.api.TaskStatus.WAITING_APPROVAL);
        assertThat(waiting.status()).isEqualTo(io.eaf.task.api.TaskStatus.WAITING_APPROVAL);
        assertThat(POST_COUNT).hasValue(0);
        var executionId = new JdbcTemplate(dataSource).queryForObject("select id from execution.execution where task_id = ?", UUID.class, created.id());
        var execution = executions.get(alice, WORKSPACE, executionId);
        assertThat(execution.status()).isEqualTo("AWAITING_APPROVAL");
        var approval = approvals.get(bob, WORKSPACE, execution.approvalId());
        var decided = approvals.decide(new ApprovalDecisionCommand(bob, WORKSPACE, approval.id(), "APPROVED", approval.version()));
        assertThat(decided.state()).isEqualTo("APPROVED");
        assertThat(approvals.get(bob, WORKSPACE, approval.id()).state()).isEqualTo("APPROVED");
        assertThat(new JdbcTemplate(dataSource).queryForObject("select state from approval.request where id = ?", String.class, approval.id()))
                .isEqualTo("APPROVED");

        var resumed = tasks.resume(alice, WORKSPACE, created.id(), waiting.version(), "p4-resume-1");
        var resumedWork = tasks.claimOne().orElseThrow();
        tasks.complete(resumedWork, runtime.run(resumedWork));
        var done = tasks.get(alice, WORKSPACE, created.id());
        var debugAfterResume = new JdbcTemplate(dataSource).queryForList("select status, error_code, error_detail, row_version, approval_id from execution.execution where task_id = ?", created.id());
        var debugApprovals = new JdbcTemplate(dataSource).queryForList("select state, row_version from approval.request where task_id = ?", created.id());
        assertThat(done.status()).as("status=%s code=%s detail=%s executions=%s approvals=%s", done.status(), done.errorCode(), done.errorDetail(), debugAfterResume, debugApprovals)
                .isEqualTo(io.eaf.task.api.TaskStatus.SUCCEEDED);
        assertThat(POST_COUNT).hasValue(1);
        assertThat(executions.get(alice, WORKSPACE, executionId).status()).isEqualTo("SUCCEEDED");

        // 重复恢复只读取已落库结果，operationId 不变且不再触发第二次 CRM POST。
        assertThat(executions.resume(alice, WORKSPACE, executionId).status()).isEqualTo("SUCCEEDED");
        assertThat(executions.verify(alice, WORKSPACE, executionId).status()).isEqualTo("SUCCEEDED");
        assertThat(POST_COUNT).hasValue(1);
        assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from approval.outbox o join approval.request r on r.id = o.approval_id where r.task_id = ?", Integer.class, created.id())).isEqualTo(2);
    }

    @Test
    void unknownWriteIsVerifiedWithoutRetryingPost() {
        pointCrm();
        FAIL_AFTER_WRITE.set(true);
        var alice = actor(ALICE, "task:create", "task:read", "task:resume", "approval:read", "execution:read", "execution:verify", "tool:read");
        var bob = actor(BOB, "approval:read", "approval:decide", "execution:read");
        var created = tasks.create(new CreateTaskCommand(alice, WORKSPACE, AGENT, "2.0.0",
                "请为 customerId=customer-001 创建跟进。", null, null, "p4-write-unknown", "trace-p4-write-unknown", "USER"));
        var work = tasks.claimOne().orElseThrow();
        tasks.complete(work, runtime.run(work));
        var waiting = tasks.get(alice, WORKSPACE, created.id());
        var executionId = new JdbcTemplate(dataSource).queryForObject("select id from execution.execution where task_id = ?", UUID.class, created.id());
        var execution = executions.get(alice, WORKSPACE, executionId);
        var approval = approvals.get(bob, WORKSPACE, execution.approvalId());
        approvals.decide(new ApprovalDecisionCommand(bob, WORKSPACE, approval.id(), "APPROVED", approval.version()));
        tasks.resume(alice, WORKSPACE, created.id(), waiting.version(), "p4-resume-unknown");
        var resumedWork = tasks.claimOne().orElseThrow();
        var waitingVerification = runtime.run(resumedWork);
        tasks.complete(resumedWork, waitingVerification);
        assertThat(waitingVerification.status())
                .as("status=%s code=%s detail=%s execution=%s", waitingVerification.status(), waitingVerification.errorCode(),
                        waitingVerification.errorDetail(), executions.get(alice, WORKSPACE, executionId))
                .isEqualTo(io.eaf.task.api.TaskStatus.WAITING_VERIFICATION);
        assertThat(tasks.get(alice, WORKSPACE, created.id()).status()).isEqualTo(io.eaf.task.api.TaskStatus.WAITING_VERIFICATION);
        assertThat(POST_COUNT).hasValue(1);

        FAIL_AFTER_WRITE.set(false);
        assertThat(executions.verify(alice, WORKSPACE, executionId).status()).isEqualTo("SUCCEEDED");
        var verifiedTask = tasks.get(alice, WORKSPACE, created.id());
        tasks.resume(alice, WORKSPACE, created.id(), verifiedTask.version(), "p4-resume-verified");
        var finalWork = tasks.claimOne().orElseThrow();
        tasks.complete(finalWork, runtime.run(finalWork));
        var finalTask = tasks.get(alice, WORKSPACE, created.id());
        assertThat(finalTask.status())
                .as("status=%s code=%s detail=%s execution=%s", finalTask.status(), finalTask.errorCode(), finalTask.errorDetail(),
                        executions.get(alice, WORKSPACE, executionId))
                .isEqualTo(io.eaf.task.api.TaskStatus.SUCCEEDED);
        assertThat(POST_COUNT).hasValue(1);
    }

    @Test
    void expiredExecutionLeaseBecomesUnknownAndKeepsOriginalOperation() {
        pointCrm();
        var alice = actor(ALICE, "task:create", "task:read", "task:resume", "approval:read", "execution:read", "execution:verify", "tool:read");
        var created = tasks.create(new CreateTaskCommand(alice, WORKSPACE, AGENT, "2.0.0",
                "请为 customerId=customer-001 创建跟进。", null, null, "p6-expired-execution", "trace-p6-expired-execution", "USER"));
        var work = tasks.claimOne().orElseThrow();
        tasks.complete(work, runtime.run(work));
        var executionId = new JdbcTemplate(dataSource).queryForObject("select id from execution.execution where task_id = ?", UUID.class, created.id());
        var before = executions.get(alice, WORKSPACE, executionId);
        var jdbc = new JdbcTemplate(dataSource);
        jdbc.update("update execution.execution set status = 'EXECUTING', lease_until = now() - interval '1 second', row_version = row_version + 1 where id = ? and status = 'AWAITING_APPROVAL'", executionId);

        executions.recoverOnStartup();
        var recovered = executions.get(alice, WORKSPACE, executionId);
        assertThat(recovered.status()).isEqualTo("UNKNOWN");
        assertThat(recovered.errorCode()).isEqualTo("INTERRUPTED_BEFORE_RESULT");
        assertThat(recovered.operationId()).isEqualTo(before.operationId());
        var recoveredVersion = recovered.version();

        executions.recoverOnStartup();
        assertThat(executions.get(alice, WORKSPACE, executionId).version()).isEqualTo(recoveredVersion);
        assertThat(executions.verify(alice, WORKSPACE, executionId).status()).isEqualTo("UNKNOWN");
        assertThat(executions.get(alice, WORKSPACE, executionId).operationId()).isEqualTo(before.operationId());
        assertThat(POST_COUNT).hasValue(0);
    }

    private void pointCrm() {
        new JdbcTemplate(dataSource).update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://127.0.0.1:" + crm.getAddress().getPort(), TENANT, WORKSPACE);
        //  CRM 写入用例显式登记 HUMAN 写权限；生产授权不会由 Connector 配置自动派生。
        new JdbcTemplate(dataSource).update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, 'crm:followup:create', 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                TENANT, WORKSPACE, ALICE);
    }

    // 用替身 ModelGateway 跑实际 Runtime 与 Execution/Approval，避免测试触发付费 Provider。
    private JdbcAgentRuntime runtimeUsing(ModelGateway gateway) {
        return new JdbcAgentRuntime(jdbc, agents, prompts, gateway, audit, usage, traces, tasks,
                toolCatalog, executions, contextService, objectMapper, clock, evaluationContexts, capabilities);
    }

    // 共用确定性 Tool Call 响应，避免负例测试各自搭建 Runtime 流程。
    private ModelGateway toolGateway(List<ModelToolCall> proposedCalls, String finishReason) {
        var calls = new AtomicInteger();
        return new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                if (calls.incrementAndGet() == 1)
                    return new ModelResult("local-fixture", "fixture-model", null, 10, 2, "KNOWN", proposedCalls, finishReason);
                return new ModelResult("local-fixture", "fixture-model",
                        "{\"riskLevel\":\"LOW\",\"summary\":\"已读取客户记录。\",\"reasons\":[\"只读查询完成\"],\"uncertainties\":[]}",
                        10, 2, "KNOWN");
            }
            @Override public int callCount() { return calls.get(); }
        };
    }

    private CompletedRuntime runAgent(ActorContext actor, ModelGateway gateway, String key) {
        var created = tasks.create(new CreateTaskCommand(actor, WORKSPACE, AGENT, "2.0.0", "查询客户记录。", null, null,
                key, "trace-" + key, "USER"));
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtimeUsing(gateway).run(work);
        tasks.complete(work, outcome);
        return new CompletedRuntime(created.id(), outcome);
    }

    private int executionCount(UUID taskId) {
        return jdbc.queryForObject("select count(*) from execution.execution where task_id = ?", Integer.class, taskId);
    }

    private ActorContext actor(UUID id, String... actions) {
        return new ActorContext(id, TENANT, ActorType.HUMAN, Set.of(actions));
    }

    private static byte[] read(InputStream input) {
        try { return input.readAllBytes(); }
        catch (Exception e) { throw new IllegalStateException("无法读取测试 CRM 请求。", e); }
    }

    private static String jsonValue(String json, String field) {
        var marker = "\"" + field + "\":\"";
        var start = json.indexOf(marker) + marker.length();
        return json.substring(start, json.indexOf('"', start));
    }

    // 测试 CRM 只有收到 Connector 注入的本地凭据才处理请求。
    private static boolean authorized(com.sun.net.httpserver.HttpExchange exchange) {
        return "Bearer p4-test-credential".equals(exchange.getRequestHeaders().getFirst("Authorization"));
    }
}
