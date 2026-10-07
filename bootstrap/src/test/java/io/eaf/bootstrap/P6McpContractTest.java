package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.agentprotocol.TaskApplicationService;
import io.eaf.capability.api.CapabilityService;
import io.eaf.capability.api.CreateCapabilityVersionCommand;
import io.eaf.capability.api.CapabilityToolReference;
import io.eaf.memory.api.CreateMemoryCommand;
import io.eaf.memory.api.MemoryService;
import io.eaf.skill.api.CreateSkillVersionCommand;
import io.eaf.skill.api.SkillService;
import io.eaf.skill.api.SkillToolReference;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.ProtocolVersions;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
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
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
// 固定 PostgreSQL 和真实 HTTP 客户端用于核验 MCP/REST 治理等价性与 Task 结果投影。
class P6McpContractTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID WORKSPACE_A = Ids.WORKSPACE_A;
    private static final UUID WORKSPACE_B = Ids.WORKSPACE_B;
    private static final UUID RAG_AGENT_ID = UUID.fromString("20000000-0000-4000-8000-000000000001");
    private static final UUID CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000001");
    private static final UUID MEMORY_ID = UUID.fromString("56000000-0000-4000-8000-000000000001");
    private static final String TOKEN = "eaf-local-alice";
    private static final String BOB_TOKEN = "eaf-local-bob";
    private static final String ACCEPT = "application/json, text/event-stream";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
    }

    @LocalServerPort int port;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper json;
    @Autowired MemoryService memories;
    @Autowired TaskApplicationService taskApplication;
    @Autowired SkillService skills;
    @Autowired CapabilityService capabilityService;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired RuntimeQuery runtimeQuery;

    @Test
    void discoversCreatesQueriesAndCancelsThroughPinnedMcpHttp() throws Exception {
        // 首个请求必须不携带凭据，确保验证的是匿名访问边界而不是工具协议校验。
        assertThat(postUnauthenticated().statusCode()).isEqualTo(401);
        assertThat(rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/capabilities", null, null, false)
                .statusCode()).isEqualTo(401);
        assertThat(postRaw("{}", "text/plain", ACCEPT, null, null).statusCode()).isEqualTo(415);
        assertThat(postRaw("{}", "application/json", "text/plain", null, null).statusCode()).isEqualTo(406);
        assertThat(postRaw(initializeBody(), "application/json", ACCEPT, "https://evil.example", null)
                .statusCode()).isEqualTo(403);
        assertThat(postRaw("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                "application/json", ACCEPT, null, "2025-06-18").statusCode()).isEqualTo(400);
        assertThat(request("GET", null, ACCEPT, null, null).statusCode()).isEqualTo(405);
        var oversized = "{\"jsonrpc\":\"2.0\",\"method\":\"initialize\",\"padding\":\""
                + "x".repeat(33_000) + "\"}";
        assertThat(postRaw(oversized, "application/json", ACCEPT, null, null).statusCode()).isEqualTo(413);

        var transport = HttpClientStreamableHttpTransport.builder(baseUrl())
                .endpoint("/mcp")
                .supportedProtocolVersions(List.of(ProtocolVersions.MCP_2025_11_25))
                .httpRequestCustomizer((builder, method, uri, sessionId, context) -> builder
                        .header("Authorization", "Bearer " + TOKEN).header("Origin", "http://localhost:3000"))
                .build();
        McpSyncClient client = McpClient.sync(transport).build();
        try {
            assertThat(client.initialize().protocolVersion()).isEqualTo(ProtocolVersions.MCP_2025_11_25);
            var tools = client.listTools().tools();
            assertThat(tools).extracting(Tool::name).containsExactlyInAnyOrder(
                    "eaf.capabilities.list", "eaf.tasks.create", "eaf.tasks.get", "eaf.tasks.cancel",
                    "eaf.catalog.search", "eaf.skills.get", "eaf.capabilities.get", "eaf.context.query",
                    "eaf.context.sources.list", "eaf.context.scoped-query");
            assertThat(tools).allSatisfy(tool -> {
                assertThat(tool.inputSchema()).containsEntry("additionalProperties", false);
                assertThat(tool.outputSchema()).containsEntry("type", "object");
            });
            var contextOutputProperties = (Map<String, Object>) tools.stream()
                    .filter(tool -> "eaf.context.query".equals(tool.name())).findFirst().orElseThrow()
                    .outputSchema().get("properties");
            assertThat(contextOutputProperties).containsKey("teamExperienceUsage");
            verifyP13Access(client);

            // 两个入口必须给同一身份相同的已发布能力视图。
            var capabilityResult = client.callTool(new CallToolRequest("eaf.capabilities.list",
                    Map.of("workspaceId", WORKSPACE_A.toString())));
            assertThat(capabilityResult.isError()).isNotEqualTo(Boolean.TRUE);
            var listed = structured(capabilityResult);
            var restCapabilities = rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/capabilities", null, null, true);
            assertThat(restCapabilities.statusCode()).isEqualTo(200);
            var mcpCapabilityIds = new java.util.HashSet<String>();
            for (var item : (List<Map<String, Object>>) listed.get("items"))
                mcpCapabilityIds.add(item.get("id").toString());
            var restCapabilityIds = new java.util.HashSet<String>();
            for (JsonNode item : json.readTree(restCapabilities.body()).path("items"))
                restCapabilityIds.add(item.path("id").asText());
            assertThat(mcpCapabilityIds).isEqualTo(restCapabilityIds);
            var capability = ((List<Map<String, Object>>) listed.get("items")).stream()
                    .filter(item -> CAPABILITY_ID.toString().equals(item.get("id"))).findFirst().orElseThrow();

            var createArgs = new java.util.HashMap<String, Object>(Map.of("workspaceId", WORKSPACE_A.toString(),
                    "capabilityId", CAPABILITY_ID.toString(), "capabilityVersion", capability.get("version"),
                    "input", "通过 MCP 创建的受控任务", "idempotencyKey", "p6-mcp-create-1"));

            // 未公布的批准、候选和 CRM 工具，以及伪造身份/来源字段，均不得进入业务 API。
            for (var forbiddenTool : List.of("eaf.approvals.decide", "eaf.learning.candidates.create",
                    "crm.followup.create")) {
                assertThatThrownBy(() -> client.callTool(new CallToolRequest(forbiddenTool, Map.of())))
                        .as("未公布工具 %s 必须返回 JSON-RPC 错误", forbiddenTool)
                        .isInstanceOf(McpError.class);
            }
            var forgedArgs = new java.util.HashMap<>(createArgs);
            forgedArgs.put("actorId", Ids.BOB.toString());
            forgedArgs.put("tenantId", Ids.TENANT_A.toString());
            forgedArgs.put("source", "EVALUATION");
            var forged = client.callTool(new CallToolRequest("eaf.tasks.create", forgedArgs));
            assertThat(forged.isError()).isTrue();
            assertThat(taskCount()).isEqualTo(1);

            // REST 和 MCP 同时撤销 task:create 时都拒绝，MCP 返回同一个业务错误码。
            setGrant(ALICE.actorId(), "task:create", false);
            var deniedMcp = client.callTool(new CallToolRequest("eaf.tasks.create", createArgs));
            var deniedRest = rest("POST", "/api/v1/workspaces/" + WORKSPACE_A + "/tasks",
                    json.writeValueAsString(restCreateBody(createArgs)), "p6-mcp-create-1", true);
            assertThat(deniedMcp.isError()).isTrue();
            assertThat(deniedMcp.content().toString()).contains("POLICY_DENIED");
            assertThat(deniedRest.statusCode()).isEqualTo(403);
            assertThat(json.readTree(deniedRest.body()).path("code").asText()).isEqualTo("POLICY_DENIED");
            assertThat(taskCount()).isEqualTo(1);
            setGrant(ALICE.actorId(), "task:create", true);

            // 服务端已处理 tools/call 后关闭未读取的响应体，模拟客户端断线后用同键恢复。
            var droppedArgs = new java.util.HashMap<>(createArgs);
            droppedArgs.put("idempotencyKey", "p6-mcp-drop-response-1");
            var beforeDroppedCall = taskCount();
            dropMcpCreateResponse(droppedArgs);
            assertThat(taskCount()).isEqualTo(beforeDroppedCall + 1);
            var recovered = structured(client.callTool(new CallToolRequest("eaf.tasks.create", droppedArgs)));
            var recoveredId = (String) recovered.get("id");
            assertThat(recovered).containsEntry("status", "QUEUED").containsEntry("entryProtocol", "MCP");
            assertThat(taskCount()).isEqualTo(beforeDroppedCall + 1);
            assertThat(structured(client.callTool(new CallToolRequest("eaf.tasks.get",
                    Map.of("workspaceId", WORKSPACE_A.toString(), "taskId", recoveredId)))))
                    .containsEntry("id", recoveredId);
            var recoveredCancelled = client.callTool(new CallToolRequest("eaf.tasks.cancel", Map.of(
                    "workspaceId", WORKSPACE_A.toString(), "taskId", recoveredId,
                    "expectedVersion", ((Number) recovered.get("version")).longValue())));
            assertThat(structured(recoveredCancelled)).containsEntry("status", "CANCELLED");

            var created = structured(client.callTool(new CallToolRequest("eaf.tasks.create", createArgs)));
            var taskId = (String) created.get("id");
            assertThat(created).containsEntry("status", "QUEUED").containsEntry("entryProtocol", "MCP");

            // 收到但丢弃首次应用响应后的同键重试，必须查回原 Task，不因协议入口变化而重放。
            var repeated = structured(client.callTool(new CallToolRequest("eaf.tasks.create", createArgs)));
            assertThat(repeated.get("id")).isEqualTo(taskId);
            var conflictArgs = new java.util.HashMap<>(createArgs);
            conflictArgs.put("input", "同键异参必须冲突");
            var conflict = client.callTool(new CallToolRequest("eaf.tasks.create", conflictArgs));
            assertThat(conflict.isError()).isTrue();
            assertThat(conflict.content().toString()).contains("IDEMPOTENCY_CONFLICT");
            var crossProtocolConflict = rest("POST", "/api/v1/workspaces/" + WORKSPACE_A + "/tasks",
                    json.writeValueAsString(restCreateBody(createArgs)), "p6-mcp-create-1", true);
            assertThat(crossProtocolConflict.statusCode()).isEqualTo(409);
            assertThat(json.readTree(crossProtocolConflict.body()).path("code").asText())
                    .isEqualTo("IDEMPOTENCY_CONFLICT");

            var getArgs = Map.<String, Object>of("workspaceId", WORKSPACE_A.toString(), "taskId", taskId);
            var queried = structured(client.callTool(new CallToolRequest("eaf.tasks.get", getArgs)));
            assertThat(queried).containsEntry("status", "QUEUED");
            var restTask = rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/tasks/" + taskId, null, null, true);
            assertThat(restTask.statusCode()).isEqualTo(200);
            assertThat(json.readTree(restTask.body()).path("status").asText()).isEqualTo(queried.get("status"));
            assertThat(json.readTree(restTask.body()).path("result").isNull()).isTrue();
            assertThat(queried.get("result")).isNull();

            // 两个入口都对无权空间隐藏同一 Task，并对被撤销的读取权返回策略拒绝。
            var hiddenRest = rest("GET", "/api/v1/workspaces/" + WORKSPACE_B + "/tasks/" + taskId,
                    null, null, true);
            var hiddenMcp = client.callTool(new CallToolRequest("eaf.tasks.get",
                    Map.of("workspaceId", WORKSPACE_B.toString(), "taskId", taskId)));
            assertThat(hiddenRest.statusCode()).isEqualTo(404);
            assertThat(hiddenMcp.isError()).isTrue();
            assertThat(hiddenMcp.content().toString()).contains("NOT_FOUND");
            setGrant(ALICE.actorId(), "task:read", false);
            var deniedReadRest = rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/tasks/" + taskId,
                    null, null, true);
            var deniedReadMcp = client.callTool(new CallToolRequest("eaf.tasks.get", getArgs));
            assertThat(deniedReadRest.statusCode()).isEqualTo(403);
            assertThat(json.readTree(deniedReadRest.body()).path("code").asText()).isEqualTo("POLICY_DENIED");
            assertThat(deniedReadMcp.isError()).isTrue();
            assertThat(deniedReadMcp.content().toString()).contains("POLICY_DENIED");
            setGrant(ALICE.actorId(), "task:read", true);

            var cancelArgs = Map.<String, Object>of("workspaceId", WORKSPACE_A.toString(), "taskId", taskId,
                    "expectedVersion", ((Number) queried.get("version")).longValue());
            var cancelled = client.callTool(new CallToolRequest("eaf.tasks.cancel", cancelArgs));
            assertThat(cancelled.isError()).isNotEqualTo(Boolean.TRUE);
            assertThat(structured(cancelled)).containsEntry("status", "CANCELLED");
            assertThat(jdbc.queryForObject("select entry_protocol from task.task where id = ?", String.class,
                    UUID.fromString(taskId))).isEqualTo("MCP");
            assertThat(jdbc.queryForObject("select count(*) from task.task where actor_id = ? and workspace_id = ?",
                    Integer.class, ALICE.actorId(), WORKSPACE_A)).isEqualTo(3);

            verifyRevokedContextIsRedacted(client);
        } finally {
            client.closeGracefully();
        }
    }

    private void verifyRevokedContextIsRedacted(McpSyncClient client) throws Exception {
        var memory = memories.get(ALICE, WORKSPACE_A, MEMORY_ID, "1.0.0");
        var published = "PUBLISHED".equals(memory.status()) ? memory
                : memories.publish(ALICE, WORKSPACE_A, MEMORY_ID, memory.version(), memory.rowVersion());
        // 共用应用入口创建固定 RAG Agent Task，随后比较真实 REST/MCP 查询在 Memory 撤回前后的投影。
        var created = taskApplication.create(ALICE, WORKSPACE_A,
                new TaskApplicationService.CreateRequest(RAG_AGENT_ID, "3.0.0", null, null,
                        "请总结客户续约信息。", new TaskApplicationService.BusinessEntity("Customer", "customer-001")),
                "p6-mcp-redaction-1", null, "MCP");
        var taskId = created.id();
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(taskId);
        tasks.complete(work, runtime.run(work));
        assertThat(tasks.get(ALICE, WORKSPACE_A, taskId).status().name()).isEqualTo("SUCCEEDED");
        assertThat(runtimeQuery.steps(ALICE, WORKSPACE_A, taskId)).anyMatch(step ->
                "CONTEXT_SNAPSHOT".equals(step.type()) && step.content() != null && step.content().contains(MEMORY_ID.toString()));

        var restPath = "/api/v1/workspaces/" + WORKSPACE_A + "/tasks/" + taskId;
        var restBeforeRevoke = json.readTree(rest("GET", restPath, null, null, true).body());
        var mcpBeforeRevoke = structured(client.callTool(new CallToolRequest("eaf.tasks.get",
                Map.of("workspaceId", WORKSPACE_A.toString(), "taskId", taskId.toString()))));
        assertThat(restBeforeRevoke.path("result").isNull()).isFalse();
        assertThat(json.valueToTree(mcpBeforeRevoke.get("result")).toString())
                .isEqualTo(restBeforeRevoke.path("result").toString());

        memories.revoke(ALICE, WORKSPACE_A, MEMORY_ID, "1.0.0", published.rowVersion());
        var restAfterRevoke = json.readTree(rest("GET", restPath, null, null, true).body());
        var mcpAfterRevoke = structured(client.callTool(new CallToolRequest("eaf.tasks.get",
                Map.of("workspaceId", WORKSPACE_A.toString(), "taskId", taskId.toString()))));
        assertThat(restAfterRevoke.path("result").isNull()).isTrue();
        assertThat(mcpAfterRevoke.get("result")).isNull();
        assertThat(restAfterRevoke.path("steps").toString()).doesNotContain(MEMORY_ID.toString(), "合成演示记录");
        assertThat(mcpAfterRevoke.get("steps").toString()).doesNotContain(MEMORY_ID.toString(), "合成演示记录");
        assertThat(jdbc.queryForObject("select count(*) from execution.execution where task_id = ?",
                Integer.class, taskId)).isZero();
    }

    private void verifyP13Access(McpSyncClient client) throws Exception {
        var searchArgs = Map.<String, Object>of("workspaceId", WORKSPACE_A.toString(), "kind", "CAPABILITY",
                "query", "risk", "limit", 1, "offset", 0);
        var mcpSearchResult = client.callTool(new CallToolRequest("eaf.catalog.search", searchArgs));
        var mcpSearch = structured(mcpSearchResult);
        var text = ((io.modelcontextprotocol.spec.McpSchema.TextContent) mcpSearchResult.content().getFirst()).text();
        assertThat(json.readTree(text)).isEqualTo(json.valueToTree(mcpSearch));
        assertThat(((List<Map<String, Object>>) mcpSearch.get("items"))).hasSize(1);
        assertThat(mcpSearch.get("nextOffset")).isEqualTo(1);

        var restSearch = rest("GET", "/api/v1/workspaces/" + WORKSPACE_A
                + "/discovery?kind=CAPABILITY&query=risk&limit=1&offset=0", null, null, true);
        assertThat(restSearch.statusCode()).isEqualTo(200);
        assertThat(json.readTree(restSearch.body())).isEqualTo(json.valueToTree(mcpSearch));
        assertThat(rest("GET", "/api/v1/workspaces/" + WORKSPACE_A
                + "/discovery?kind=CAPABILITY&tenantId=" + Ids.TENANT_A, null, null, true).statusCode()).isEqualTo(400);
        assertThat(rest("GET", "/api/v1/workspaces/" + WORKSPACE_A
                + "/discovery?kind=CAPABILITY&limit=1.5", null, null, true).statusCode()).isEqualTo(400);
        var hiddenCatalog = client.callTool(new CallToolRequest("eaf.catalog.search", Map.of(
                "workspaceId", WORKSPACE_B.toString(), "kind", "CAPABILITY")));
        assertThat(hiddenCatalog.isError()).isTrue();
        assertThat(rest("GET", "/api/v1/workspaces/" + WORKSPACE_B + "/discovery?kind=CAPABILITY",
                null, null, true).statusCode()).isNotEqualTo(200);

        var skill = skills.get(ALICE, WORKSPACE_A, UUID.fromString("53000000-0000-4000-8000-000000000001"), "1.0.0");
        var capabilityArgs = Map.<String, Object>of("workspaceId", WORKSPACE_A.toString(),
                "capabilityId", CAPABILITY_ID.toString(), "version", "1.0.0");
        var mcpCapability = structured(client.callTool(new CallToolRequest("eaf.capabilities.get", capabilityArgs)));
        assertThat(mcpCapability).containsEntry("kind", "CAPABILITY").containsKey("taskInput");
        assertThat(mcpCapability.keySet()).doesNotContain("ownerId", "promptId", "agentId", "modelProfile");
        var restCapability = rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/discovery/capabilities/"
                + CAPABILITY_ID + "/versions/1.0.0", null, null, true);
        assertThat(restCapability.statusCode()).isEqualTo(200);
        assertThat(json.readTree(restCapability.body())).isEqualTo(json.valueToTree(mcpCapability));

        // 目录和精确版本详情用于选择 Task 入口；真正的执行仍走原 TaskApplicationService。
        var delegated = structured(client.callTool(new CallToolRequest("eaf.tasks.create", Map.of(
                "workspaceId", WORKSPACE_A.toString(), "capabilityId", CAPABILITY_ID.toString(),
                "capabilityVersion", "1.0.0", "input", "discovered capability handoff",
                "idempotencyKey", "p13-discovery-task-1"))));
        var delegatedRead = structured(client.callTool(new CallToolRequest("eaf.tasks.get", Map.of(
                "workspaceId", WORKSPACE_A.toString(), "taskId", delegated.get("id")))));
        assertThat(delegatedRead).containsEntry("id", delegated.get("id")).containsEntry("status", "QUEUED");
        var delegatedCancelled = structured(client.callTool(new CallToolRequest("eaf.tasks.cancel", Map.of(
                "workspaceId", WORKSPACE_A.toString(), "taskId", delegated.get("id"),
                "expectedVersion", delegatedRead.get("version")))));
        assertThat(delegatedCancelled).containsEntry("status", "CANCELLED");

        var skillId = skill.id();
        var skillArgs = Map.<String, Object>of("workspaceId", WORKSPACE_A.toString(),
                "skillId", skillId.toString(), "version", skill.version());
        var mcpSkill = structured(client.callTool(new CallToolRequest("eaf.skills.get", skillArgs)));
        assertThat(mcpSkill).containsEntry("kind", "SKILL").containsKeys("inputSchema", "outputSchema", "toolDependencies");
        assertThat(mcpSkill.keySet()).doesNotContain("ownerId", "tenantId", "promptId", "evaluationRef");
        var restSkill = rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/discovery/skills/"
                + skillId + "/versions/" + skill.version(), null, null, true);
        assertThat(restSkill.statusCode()).isEqualTo(200);
        assertThat(json.readTree(restSkill.body())).isEqualTo(json.valueToTree(mcpSkill));

        var publicMemory = memories.get(ALICE, WORKSPACE_A, MEMORY_ID, "1.0.0");
        if (!"PUBLISHED".equals(publicMemory.status()))
            memories.publish(ALICE, WORKSPACE_A, MEMORY_ID, publicMemory.version(), publicMemory.rowVersion());
        var contextArgs = Map.<String, Object>of("workspaceId", WORKSPACE_A.toString(),
                "query", "shared context projection check");
        var mcpContext = structured(client.callTool(new CallToolRequest("eaf.context.query", contextArgs)));
        assertThat(mcpContext).containsEntry("topK", 5).containsEntry("tokenBudget", 2_000);
        assertThat(json.valueToTree(mcpContext).toString()).doesNotContain(MEMORY_ID.toString());
        var restContext = rest("POST", "/api/v1/workspaces/" + WORKSPACE_A + "/context/queries",
                json.writeValueAsString(Map.of("query", "shared context projection check")), null, true);
        assertThat(restContext.statusCode()).isEqualTo(200);
        assertThat(json.readTree(restContext.body())).isEqualTo(json.valueToTree(mcpContext));

        var sourcesArgs = Map.<String, Object>of("workspaceId", WORKSPACE_A.toString());
        var mcpSources = structured(client.callTool(new CallToolRequest("eaf.context.sources.list", sourcesArgs)));
        var restSources = rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/context/sources", null, null, true);
        assertThat(restSources.statusCode()).isEqualTo(200);
        assertThat(json.readTree(restSources.body())).isEqualTo(json.valueToTree(mcpSources));

        var scopedArgs = Map.<String, Object>of("workspaceId", WORKSPACE_A.toString(),
                "query", "MCP and REST scoped context parity");
        var mcpScoped = structured(client.callTool(new CallToolRequest("eaf.context.scoped-query", scopedArgs)));
        var restScoped = rest("POST", "/api/v1/workspaces/" + WORKSPACE_A + "/context/scoped-queries",
                json.writeValueAsString(Map.of("query", "MCP and REST scoped context parity")), null, true);
        assertThat(restScoped.statusCode()).isEqualTo(200);
        assertThat(json.readTree(restScoped.body())).isEqualTo(json.valueToTree(mcpScoped));
        assertThat(mcpScoped).containsKeys("items", "sources", "omitted", "unavailableSourceCount");

        var nullContextArgs = new java.util.HashMap<String, Object>();
        nullContextArgs.put("workspaceId", WORKSPACE_A.toString());
        nullContextArgs.put("query", "null context defaults");
        nullContextArgs.put("topK", null);
        nullContextArgs.put("tokenBudget", null);
        var nullContext = structured(client.callTool(new CallToolRequest("eaf.context.query", nullContextArgs)));
        assertThat(nullContext).containsEntry("topK", 5).containsEntry("tokenBudget", 2_000);

        var privateMemory = memories.create(new CreateMemoryCommand(ALICE, WORKSPACE_A, "p13-private-context",
                "1.0.0", "PREFERENCE", "PERSONAL", "P13_PERSONAL_MEMORY_SENTINEL", 0.9,
                Instant.now().plusSeconds(3_600), "manual:p13-private-context", List.of("test:p13-private-context")));
        memories.publish(ALICE, WORKSPACE_A, privateMemory.id(), privateMemory.version(), privateMemory.rowVersion());
        var privateQuery = "private memory visibility check";
        var alicePrivateContext = structured(client.callTool(new CallToolRequest("eaf.context.query",
                Map.of("workspaceId", WORKSPACE_A.toString(), "query", privateQuery))));
        assertThat((List<Map<String, Object>>) alicePrivateContext.get("items"))
                .anyMatch(item -> privateMemory.id().toString().equals(item.get("memoryId")));
        grant(Ids.BOB, "context:read");
        grant(Ids.BOB, "memory:read");
        var bobRestContext = restAs(BOB_TOKEN, "POST", "/api/v1/workspaces/" + WORKSPACE_A + "/context/queries",
                json.writeValueAsString(Map.of("query", privateQuery)), null);
        assertThat(bobRestContext.statusCode()).isEqualTo(200);
        assertThat(bobRestContext.body()).doesNotContain(privateMemory.id().toString(), "P13_PERSONAL_MEMORY_SENTINEL");
        var bobTransport = HttpClientStreamableHttpTransport.builder(baseUrl())
                .endpoint("/mcp")
                .supportedProtocolVersions(List.of(ProtocolVersions.MCP_2025_11_25))
                .httpRequestCustomizer((builder, method, uri, sessionId, context) -> builder
                        .header("Authorization", "Bearer " + BOB_TOKEN).header("Origin", "http://localhost:3000"))
                .build();
        var bobClient = McpClient.sync(bobTransport).build();
        try {
            bobClient.initialize();
            var bobPrivateContext = structured(bobClient.callTool(new CallToolRequest("eaf.context.query",
                    Map.of("workspaceId", WORKSPACE_A.toString(), "query", privateQuery))));
            assertThat(json.valueToTree(bobPrivateContext).toString())
                    .doesNotContain(privateMemory.id().toString(), "P13_PERSONAL_MEMORY_SENTINEL");
        } finally {
            bobClient.closeGracefully();
        }
        var forgedContext = client.callTool(new CallToolRequest("eaf.context.query", Map.of("workspaceId",
                WORKSPACE_A.toString(), "query", "forged context input", "actorId", Ids.BOB.toString())));
        assertThat(forgedContext.isError()).isTrue();
        assertThat(rest("POST", "/api/v1/workspaces/" + WORKSPACE_A + "/context/queries",
                json.writeValueAsString(Map.of("query", "forged context input", "actorId", Ids.BOB.toString())),
                null, true).statusCode()).isEqualTo(400);
        assertThat(rest("POST", "/api/v1/workspaces/" + WORKSPACE_A + "/context/queries",
                "{\"query\":\" fractional input\",\"topK\":2.5}", null, true).statusCode()).isEqualTo(400);

        // 使用方投影对 Owner 也隐藏草稿和撤回版本，管理接口仍可正常创建与发布这些版本。
        var skillDraft = skills.addVersion(ALICE, WORKSPACE_A, skillId, new CreateSkillVersionCommand("13.0.0",
                skill.inputSchema(), skill.outputSchema(), skill.promptId(), skill.promptVersion(),
                skill.toolDependencies().stream().map(tool -> new SkillToolReference(tool.name(), tool.version())).toList(),
                skill.evaluationRef()));
        assertThat(rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/discovery/skills/"
                + skillId + "/versions/13.0.0", null, null, true).statusCode()).isEqualTo(404);
        assertThat(client.callTool(new CallToolRequest("eaf.skills.get", Map.of("workspaceId", WORKSPACE_A.toString(),
                "skillId", skillId.toString(), "version", "13.0.0"))).isError()).isTrue();
        var publishedSkill = skills.publish(ALICE, WORKSPACE_A, skillId, skillDraft.version(), skillDraft.rowVersion());
        skills.revoke(ALICE, WORKSPACE_A, skillId, skillDraft.version(), publishedSkill.rowVersion());
        assertThat(rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/discovery/skills/"
                + skillId + "/versions/13.0.0", null, null, true).statusCode()).isEqualTo(404);

        var capability = capabilityService.get(ALICE, WORKSPACE_A, CAPABILITY_ID, "1.0.0");
        var capabilityDraft = capabilityService.addVersion(ALICE, WORKSPACE_A, CAPABILITY_ID,
                new CreateCapabilityVersionCommand("13.0.0", capability.agentId(), capability.agentVersion(),
                        capability.skillId(), capability.skillVersion(), capability.promptId(), capability.promptVersion(),
                        capability.toolDependencies().stream().map(tool -> new CapabilityToolReference(tool.name(), tool.version())).toList(),
                        capability.evaluationRef()));
        assertThat(client.callTool(new CallToolRequest("eaf.capabilities.get", Map.of("workspaceId", WORKSPACE_A.toString(),
                "capabilityId", CAPABILITY_ID.toString(), "version", "13.0.0"))).isError()).isTrue();
        var publishedCapability = capabilityService.publish(ALICE, WORKSPACE_A, CAPABILITY_ID, capabilityDraft.version(),
                capabilityDraft.rowVersion());
        capabilityService.revoke(ALICE, WORKSPACE_A, CAPABILITY_ID, capabilityDraft.version(), publishedCapability.rowVersion());
        assertThat(rest("GET", "/api/v1/workspaces/" + WORKSPACE_A + "/discovery/capabilities/"
                + CAPABILITY_ID + "/versions/13.0.0", null, null, true).statusCode()).isEqualTo(404);
    }

    private Map<String, Object> restCreateBody(Map<String, Object> args) {
        return Map.of("capabilityId", args.get("capabilityId"), "capabilityVersion", args.get("capabilityVersion"),
                "input", args.get("input"));
    }

    private void setGrant(UUID actorId, String action, boolean active) {
        jdbc.update("update workspace.\"grant\" set status = ? where tenant_id = ? and workspace_id = ? and actor_id = ? and action = ?",
                active ? "ACTIVE" : "REVOKED", Ids.TENANT_A, WORKSPACE_A, actorId, action);
    }

    private void grant(UUID actorId, String action) {
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, WORKSPACE_A, actorId, action);
    }

    private int taskCount() {
        return jdbc.queryForObject("select count(*) from task.task where actor_id = ? and workspace_id = ?",
                Integer.class, ALICE.actorId(), WORKSPACE_A);
    }

    private HttpResponse<String> postRaw(String body, String contentType, String accept, String origin,
                                         String protocolVersion) throws Exception {
        return request("POST", body, accept, origin, protocolVersion, contentType);
    }

    private HttpResponse<String> postUnauthenticated() throws Exception {
        var request = HttpRequest.newBuilder(URI.create(baseUrl() + "/mcp"))
                .POST(HttpRequest.BodyPublishers.ofString(""))
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> request(String method, String body, String accept, String origin,
                                         String protocolVersion) throws Exception {
        return request(method, body, accept, origin, protocolVersion, "application/json");
    }

    private HttpResponse<String> request(String method, String body, String accept, String origin,
                                         String protocolVersion, String contentType) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(baseUrl() + "/mcp"))
                .header("Authorization", "Bearer " + TOKEN);
        if (accept != null) builder.header("Accept", accept);
        if (contentType != null) builder.header("Content-Type", contentType);
        if (origin != null) builder.header("Origin", origin);
        if (protocolVersion != null) builder.header("MCP-Protocol-Version", protocolVersion);
        if ("GET".equals(method)) builder.GET();
        else if ("DELETE".equals(method)) builder.DELETE();
        else builder.POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body));
        return HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> rest(String method, String path, String body, String idempotencyKey,
                                      boolean authenticated) throws Exception {
        return restAs(authenticated ? TOKEN : null, method, path, body, idempotencyKey);
    }

    private HttpResponse<String> restAs(String token, String method, String path, String body,
                                        String idempotencyKey) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(baseUrl() + path)).header("Accept", "application/json");
        if (token != null) builder.header("Authorization", "Bearer " + token);
        if (idempotencyKey != null) builder.header("Idempotency-Key", idempotencyKey);
        if ("GET".equals(method)) builder.GET();
        else builder.header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body));
        return HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private void dropMcpCreateResponse(Map<String, Object> arguments) throws Exception {
        var http = HttpClient.newHttpClient();
        var initialization = http.send(mcpHttpRequest(initializeBody(), null, null), HttpResponse.BodyHandlers.ofString());
        assertThat(initialization.statusCode()).isEqualTo(200);
        var sessionId = initialization.headers().firstValue("Mcp-Session-Id").orElseThrow();
        var initialized = "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}";
        assertThat(http.send(mcpHttpRequest(initialized, ProtocolVersions.MCP_2025_11_25, sessionId),
                HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(202);
        var call = json.writeValueAsString(Map.of("jsonrpc", "2.0", "id", 2, "method", "tools/call",
                "params", Map.of("name", "eaf.tasks.create", "arguments", arguments)));
        var response = http.send(mcpHttpRequest(call, ProtocolVersions.MCP_2025_11_25, sessionId),
                HttpResponse.BodyHandlers.ofInputStream());
        assertThat(response.statusCode()).isEqualTo(200);
        response.body().close();
    }

    private HttpRequest mcpHttpRequest(String body, String protocolVersion, String sessionId) {
        var builder = HttpRequest.newBuilder(URI.create(baseUrl() + "/mcp"))
                .header("Authorization", "Bearer " + TOKEN)
                .header("Origin", "http://localhost:3000")
                .header("Accept", ACCEPT)
                .header("Content-Type", "application/json");
        if (protocolVersion != null) builder.header("MCP-Protocol-Version", protocolVersion);
        if (sessionId != null) builder.header("Mcp-Session-Id", sessionId);
        return builder.POST(HttpRequest.BodyPublishers.ofString(body)).build();
    }

    private String baseUrl() { return "http://127.0.0.1:" + port; }

    private static String initializeBody() {
        return "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                + "\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},"
                + "\"clientInfo\":{\"name\":\"p6-test\",\"version\":\"1.0\"}}}";
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> structured(CallToolResult result) {
        assertThat(result.isError()).as("MCP 工具错误响应：%s", result.content()).isNotEqualTo(Boolean.TRUE);
        return (Map<String, Object>) result.structuredContent();
    }
}
