package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.audit.api.AuditPort;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.context.api.ContextService;
import io.eaf.context.api.EnterpriseContext;
import io.eaf.identity.api.IdentityService;
import io.eaf.knowledge.api.CreateKnowledgeDocumentCommand;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.infrastructure.KnowledgeOutboxPublisher;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.TaskExecutionService;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.modelcontextprotocol.spec.ProtocolVersions;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
                "eaf.model.mode=deterministic", "eaf.security.mode=local",
                "eaf.agent-protocol.declarative-resources-enabled=true"})
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class P26McpDelegationTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID WORKSPACE = Ids.WORKSPACE_A;
    private static final UUID CAPABILITY = UUID.fromString("54000000-0000-4000-8000-000000000012");
    private static final String OWNER_TOKEN = "eaf-local-alice";
    private static final String AGENT_TOKEN = "eaf-local-risk-agent";
    private static final Set<String> READONLY_ACTIONS = Set.of("task:create", "task:read", "agent:read",
            "capability:read", "skill:read", "tool:read", "context:read", "knowledge:read");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
    }

    @LocalServerPort int port;
    @Autowired ObjectMapper json;
    @Autowired JdbcTemplate jdbc;
    @Autowired IdentityService identities;
    @Autowired KnowledgeService knowledge;
    @Autowired AuditPort audit;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired TaskExecutionService execution;
    @Autowired RuntimeQuery runtime;
    @Autowired ContextService contexts;

    @Test
    void issuesBoundedDelegationAndRunsOnlyItsCurrentReadonlyScopeThroughMcp() throws Exception {
        grantActions(Ids.ALICE, READONLY_ACTIONS);
        grantActions(Ids.ALICE, Set.of("identity:delegation:manage"));
        grantActions(Ids.AGENT_RISK, READONLY_ACTIONS);
        var owner = identities.resolveToken(OWNER_TOKEN).orElseThrow();
        var selected = publishKnowledge(owner, "设备指示灯呈蓝色闪烁时，先记录设备地点和故障现象，再联系服务台。",
                "p26-selected-knowledge");
        var unselected = publishKnowledge(owner, "量子导航容错操作仅适用于演示设备。", "p26-unselected-knowledge");
        allowAgentDocumentRead(selected);
        allowAgentDocumentRead(unselected);

        var expiresAt = Instant.now().plusSeconds(300);
        var createBody = Map.of("delegateId", Ids.AGENT_RISK.toString(), "capabilityId", CAPABILITY.toString(),
                "capabilityVersion", "1.0.0", "knowledgeDocumentIds", List.of(selected.toString()),
                "expiresAt", expiresAt.toString());
        var createdDelegation = rest("POST", "/api/v1/workspaces/" + WORKSPACE + "/delegations/mcp-readonly",
                json.writeValueAsBytes(createBody), OWNER_TOKEN, null);
        assertThat(createdDelegation.statusCode()).isEqualTo(201);
        var delegationJson = json.readTree(createdDelegation.body());
        var delegationId = delegationJson.path("id").asText();
        assertThat(delegationJson.path("audience").asText()).isEqualTo(IdentityService.MCP_AUDIENCE);
        assertThat(delegationJson.path("profile").asText()).isEqualTo(IdentityService.MCP_READONLY_PROFILE);
        var delegatedActor = identities.resolveDelegatedToken(AGENT_TOKEN, UUID.fromString(delegationId),
                IdentityService.MCP_AUDIENCE).orElseThrow();
        assertThat(identities.mcpReadonlyScope(delegatedActor)).isPresent();
        assertThat(delegationJson.path("knowledgeDocumentIds").toString()).contains(selected.toString())
                .doesNotContain(unselected.toString());

        // 同一委托不得借 REST audience 重用。
        assertThat(rest("GET", "/api/v1/workspaces/" + WORKSPACE + "/capabilities", null,
                AGENT_TOKEN, delegationId).statusCode()).isEqualTo(401);
        var transport = HttpClientStreamableHttpTransport.builder(baseUrl())
                .endpoint("/mcp").supportedProtocolVersions(List.of(ProtocolVersions.MCP_2025_11_25))
                .httpRequestCustomizer((builder, method, uri, sessionId, context) -> builder
                        .header("Authorization", "Bearer " + AGENT_TOKEN)
                        .header("X-EAF-Delegation", delegationId)
                        .header("Origin", "http://localhost:3000"))
                .build();
        McpSyncClient client = McpClient.sync(transport).build();
        try {
            var initialization = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(baseUrl() + "/mcp"))
                    .header("Authorization", "Bearer " + AGENT_TOKEN)
                    .header("X-EAF-Delegation", delegationId)
                    .header("Origin", "http://localhost:3000")
                    .header("Accept", "application/json, text/event-stream")
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},\"clientInfo\":{\"name\":\"p26-test\",\"version\":\"1.0\"}}}"))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(initialization.statusCode()).as("MCP init response: %s", initialization.body()).isEqualTo(200);
            assertThat(client.initialize().protocolVersion()).isEqualTo(ProtocolVersions.MCP_2025_11_25);
            assertThat(client.listTools().tools()).extracting(Tool::name).containsExactlyInAnyOrder(
                    "eaf.capabilities.list", "eaf.tasks.create", "eaf.tasks.get", "eaf.tasks.cancel",
                    "eaf.catalog.search", "eaf.skills.get", "eaf.capabilities.get", "eaf.context.query",
                    "eaf.context.sources.list", "eaf.context.scoped-query");
            var listed = structured(client.callTool(new CallToolRequest("eaf.capabilities.list",
                    Map.of("workspaceId", WORKSPACE.toString()))));
            var listedItems = json.valueToTree(listed).path("items");
            assertThat(listedItems.size()).isEqualTo(1);
            assertThat(listedItems.get(0).path("id").asText()).isEqualTo(CAPABILITY.toString());
            assertThat(client.listResources().resources()).extracting(resource -> resource.uri())
                    .containsExactly("eaf://declarations/index");
            var indexResult = client.readResource(new ReadResourceRequest("eaf://declarations/index", Map.of(
                    "io.eaf/declarations", Map.of("workspaceId", WORKSPACE.toString(), "offset", 0))));
            var index = json.readTree(((io.modelcontextprotocol.spec.McpSchema.TextResourceContents)
                    indexResult.contents().getFirst()).text());
            assertThat(index.path("items").size()).isEqualTo(1);
            var entryUri = index.path("items").get(0).path("entryUri").asText();
            assertThat(client.readResource(new ReadResourceRequest(entryUri)).contents()).hasSize(1);
            var outsideScopeUri = "eaf://declarations/workspaces/" + WORKSPACE + "/capabilities/"
                    + UUID.fromString("54000000-0000-4000-8000-000000000099")
                    + "/versions/1.0.0/packages/" + "0".repeat(64) + "/SKILL.md";
            assertThatThrownBy(() -> client.readResource(new ReadResourceRequest(outsideScopeUri)))
                    .isInstanceOf(RuntimeException.class);

            var context = structured(client.callTool(new CallToolRequest("eaf.context.query", Map.of(
                    "workspaceId", WORKSPACE.toString(), "query", "蓝色闪烁设备服务台", "topK", 5))));
            var contextJson = json.valueToTree(context).toString();
            assertThat(contextJson).contains(selected.toString()).doesNotContain(unselected.toString());
            var outsideContext = structured(client.callTool(new CallToolRequest("eaf.context.query", Map.of(
                    "workspaceId", WORKSPACE.toString(), "query", "量子导航容错", "topK", 5))));
            assertThat(json.valueToTree(outsideContext).toString()).doesNotContain(unselected.toString());

            var createArgs = Map.<String, Object>of("workspaceId", WORKSPACE.toString(),
                    "capabilityId", CAPABILITY.toString(), "capabilityVersion", "1.0.0",
                    "input", "设备指示灯呈蓝色闪烁，请基于当前正式知识给出下一步建议。",
                    "idempotencyKey", "p26-delegated-task-1");
            var created = structured(client.callTool(new CallToolRequest("eaf.tasks.create", createArgs)));
            assertThat(created).containsEntry("status", "QUEUED").containsEntry("entryProtocol", "MCP");
            var replay = structured(client.callTool(new CallToolRequest("eaf.tasks.create", createArgs)));
            assertThat(replay.get("id")).isEqualTo(created.get("id"));
            var cancel = client.callTool(new CallToolRequest("eaf.tasks.cancel", Map.of("workspaceId", WORKSPACE.toString(),
                    "taskId", created.get("id"), "expectedVersion", created.get("version"))));
            assertThat(cancel.isError()).isTrue();
            var multiSource = client.callTool(new CallToolRequest("eaf.context.scoped-query", Map.of(
                    "workspaceId", WORKSPACE.toString(), "query", "设备", "sourceWorkspaceIds", List.of())));
            assertThat(multiSource.isError()).isTrue();

            var queuedForRevocation = structured(client.callTool(new CallToolRequest("eaf.tasks.create", Map.of(
                    "workspaceId", WORKSPACE.toString(), "capabilityId", CAPABILITY.toString(),
                    "capabilityVersion", "1.0.0", "input", "撤销前排队检查", "idempotencyKey", "p26-delegated-task-2"))));
            assertThat(execution.dispatchNext()).isTrue();
            var read = awaitTask(client, (String) created.get("id"));
            assertThat(read).containsEntry("status", "SUCCEEDED");
            var result = json.valueToTree(read.get("result"));
            assertThat(result.path("outcome").asText()).isEqualTo("READY");
            assertThat(result.path("readyToSubmit").asBoolean()).isFalse();
            assertThat(result.path("stopReason").asText()).isEqualTo("DELEGATED_READ_ONLY");
            assertThat(result.has("plan")).isFalse();
            assertThat(result.path("citations").size()).isPositive();
            assertThat(result.path("contextRefs").toString()).contains(selected.toString())
                    .doesNotContain(unselected.toString());
            assertThat(json.valueToTree(read).path("steps")).isEmpty();

            var ref = result.path("contextRefs").get(0);
            assertThat(knowledge.isUsable(delegatedActor, WORKSPACE, selected, ref.path("documentVersion").asInt(),
                    UUID.fromString(ref.path("chunkId").asText()), UUID.fromString(ref.path("buildId").asText()),
                    ref.path("contentHash").asText())).isTrue();
            var revoked = jdbc.update("update knowledge.document_permission set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and document_id = ? and actor_id = ? and action = 'knowledge:read'",
                    Ids.TENANT_A, WORKSPACE, selected, Ids.AGENT_RISK);
            assertThat(revoked).isEqualTo(1);
            assertThat(knowledge.isUsable(delegatedActor, WORKSPACE, selected, ref.path("documentVersion").asInt(),
                    UUID.fromString(ref.path("chunkId").asText()), UUID.fromString(ref.path("buildId").asText()),
                    ref.path("contentHash").asText())).isFalse();
            var taskId = UUID.fromString((String) created.get("id"));
            var savedContext = jdbc.query("select s.content from agent_runtime.step s join agent_runtime.run r on r.id = s.run_id where r.task_id = ? and r.tenant_id = ? and r.workspace_id = ? and s.type = 'SERVICE_REQUEST_RETRIEVAL_RESULT' order by r.attempt desc, s.step_no desc limit 1",
                    rs -> rs.next() ? rs.getString("content") : null, taskId, Ids.TENANT_A, WORKSPACE);
            assertThat(savedContext).isNotNull();
            var retrievalContext = json.readTree(savedContext).path("context");
            assertThat(contexts.isCurrent(delegatedActor, WORKSPACE,
                    json.treeToValue(retrievalContext, EnterpriseContext.class))).isFalse();
            assertThat(runtime.canExposeResult(delegatedActor, WORKSPACE, taskId)).isFalse();
            var hiddenResult = structured(client.callTool(new CallToolRequest("eaf.tasks.get", Map.of(
                    "workspaceId", WORKSPACE.toString(), "taskId", created.get("id")))));
            assertThat(hiddenResult.get("result")).isNull();

            var revoke = rest("POST", "/api/v1/workspaces/" + WORKSPACE + "/delegations/" + delegationId + "/revoke",
                    new byte[0], OWNER_TOKEN, null);
            assertThat(revoke.statusCode()).isEqualTo(200);
            assertThatThrownBy(() -> client.readResource(new ReadResourceRequest(entryUri)))
                    .isInstanceOf(RuntimeException.class);
            assertThat(execution.dispatchNext()).isFalse();
            var failed = rest("GET", "/api/v1/workspaces/" + WORKSPACE + "/tasks/" + queuedForRevocation.get("id"),
                    null, OWNER_TOKEN, null);
            assertThat(json.readTree(failed.body()).path("status").asText()).isEqualTo("FAILED");
        } finally {
            client.closeGracefully();
        }
    }

    private UUID publishKnowledge(ActorContext owner, String content, String key) {
        var document = knowledge.create(new CreateKnowledgeDocumentCommand(owner, WORKSPACE,
                key, "manual://" + key, content, Map.of("synthetic", "true"), key + "-create", key));
        knowledge.chunk(owner, WORKSPACE, document.id(), "p3-plain-1");
        var build = knowledge.buildIndex(owner, WORKSPACE, document.id(), "p3-plain-1", key + "-index");
        assertThat(build.status()).isEqualTo("READY");
        knowledge.publish(owner, WORKSPACE, document.id(), document.rowVersion(), build.id(), key + "-publish");
        new KnowledgeOutboxPublisher(jdbc, audit, transactionManager).publish();
        return document.id();
    }

    private void grantActions(UUID actorId, Set<String> actions) {
        actions.forEach(action -> jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE', tenant_id = excluded.tenant_id",
                Ids.TENANT_A, WORKSPACE, actorId, action));
    }

    private void allowAgentDocumentRead(UUID documentId) {
        jdbc.update("insert into knowledge.document_permission(tenant_id, workspace_id, document_id, actor_id, action, status) values (?, ?, ?, ?, 'knowledge:read', 'ACTIVE') on conflict (tenant_id, document_id, actor_id, action) do update set status = 'ACTIVE', workspace_id = excluded.workspace_id",
                Ids.TENANT_A, WORKSPACE, documentId, Ids.AGENT_RISK);
    }

    private Map<String, Object> awaitTask(McpSyncClient client, String taskId) throws Exception {
        var deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
        Map<String, Object> read;
        do {
            read = structured(client.callTool(new CallToolRequest("eaf.tasks.get", Map.of(
                    "workspaceId", WORKSPACE.toString(), "taskId", taskId))));
            if (!Set.of("QUEUED", "RUNNING").contains(read.get("status"))) return read;
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        return read;
    }

    private HttpResponse<byte[]> rest(String method, String path, byte[] body, String token,
                                      String delegationId) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(baseUrl() + path)).header("Accept", "application/json");
        if (token != null) builder.header("Authorization", "Bearer " + token);
        if (delegationId != null) builder.header("X-EAF-Delegation", delegationId);
        if (body != null && body.length > 0) builder.header("Content-Type", "application/json");
        if ("GET".equals(method)) builder.GET();
        else builder.method(method, HttpRequest.BodyPublishers.ofByteArray(body == null ? new byte[0] : body));
        return HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> structured(CallToolResult result) {
        assertThat(result.isError()).as("MCP 响应：%s", result.content()).isNotEqualTo(Boolean.TRUE);
        return (Map<String, Object>) result.structuredContent();
    }

    private String baseUrl() { return "http://127.0.0.1:" + port; }
}
