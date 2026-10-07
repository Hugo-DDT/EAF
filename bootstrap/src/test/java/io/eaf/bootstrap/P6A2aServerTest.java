package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.identity.api.CreateDelegationCommand;
import io.eaf.identity.api.IdentityService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
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

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
// 使用独立 JDK HTTP 客户端和 PostgreSQL 检查 A2A JSON-RPC Task 生命周期与资源边界。
class P6A2aServerTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String TOKEN = "eaf-local-alice";
    private static final UUID WORKSPACE = Ids.WORKSPACE_A;
    private static final UUID OTHER_WORKSPACE = Ids.WORKSPACE_B;
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
    @Autowired ObjectMapper json;
    @Autowired JdbcTemplate jdbc;
    @Autowired TaskService tasks;
    @Autowired IdentityService identities;
    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void independentClientCreatesListsCompletesCancelsAndHidesTasks() throws Exception {
        assertThat(post("1", "GetTask", Map.of("id", UUID.randomUUID().toString()), "1.0", false).statusCode())
                .isEqualTo(401);
        var publicCardResponse = get("/.well-known/agent-card.json", false);
        assertThat(publicCardResponse.statusCode()).isEqualTo(200);
        var publicCard = json.readTree(publicCardResponse.body());
        assertThat(publicCard.path("skills")).isEmpty();
        assertThat(publicCard.toString()).doesNotContain(Ids.TENANT_A.toString(), WORKSPACE.toString());

        var cardResponse = get("/.well-known/agent-card.json?workspaceId=" + WORKSPACE, true);
        assertThat(cardResponse.statusCode()).isEqualTo(200);
        var card = json.readTree(cardResponse.body());
        var skillId = card.path("skills").get(0).path("id").asText();
        assertThat(skillId).isEqualTo("customer-risk-followup@1.0.0");
        assertThat(card.path("supportedInterfaces").get(0).path("url").asText())
                .endsWith("/a2a/" + WORKSPACE);

        var missingVersion = response(post("2", "GetTask", Map.of("id", UUID.randomUUID().toString()), null, true));
        assertThat(missingVersion.path("error").path("code").asInt()).isEqualTo(-32009);
        var wrongVersion = response(post("3", "GetTask", Map.of("id", UUID.randomUUID().toString()), "0.3", true));
        assertThat(wrongVersion.path("error").path("code").asInt()).isEqualTo(-32009);
        var unknownMethod = response(post("4", "SubscribeToTask", Map.of(), "1.0", true));
        assertThat(unknownMethod.path("error").path("code").asInt()).isEqualTo(-32601);

        var first = response(post("rpc-1", "SendMessage", sendParams("message-1", "请分析客户风险。", skillId), "1.0", true));
        var firstTask = first.path("result").path("task");
        assertThat(first.path("error").isMissingNode()).isTrue();
        assertThat(firstTask.path("status").path("state").asText()).isEqualTo("TASK_STATE_SUBMITTED");
        var firstId = UUID.fromString(firstTask.path("id").asText());
        assertThat(taskCount()).isEqualTo(1);

        // JSON-RPC id 只关联一次传输；同一个 messageId 与内容必须恢复同一业务 Task。
        var replay = response(post("rpc-2", "SendMessage", sendParams("message-1", "请分析客户风险。", skillId), "1.0", true));
        assertThat(replay.path("result").path("task").path("id").asText()).isEqualTo(firstId.toString());
        assertThat(taskCount()).isEqualTo(1);
        var conflict = response(post("rpc-3", "SendMessage", sendParams("message-1", "改写同一消息内容。", skillId), "1.0", true));
        assertThat(conflict.path("error").path("code").asInt()).isEqualTo(-32602);
        assertThat(taskCount()).isEqualTo(1);

        var invalidFollowUp = sendParams("message-1-follow-up", "继续补充。", skillId);
        ((Map<String, Object>) invalidFollowUp.get("message")).put("contextId", firstId.toString());
        var followUp = response(post("rpc-4", "SendMessage", invalidFollowUp, "1.0", true));
        assertThat(followUp.path("error").path("code").asInt()).isEqualTo(-32602);
        assertThat(taskCount()).isEqualTo(1);

        var cancelledFirst = response(post("rpc-4a", "CancelTask", Map.of("id", firstId.toString()), "1.0", true));
        assertThat(cancelledFirst.path("result").path("status").path("state").asText())
                .isEqualTo("TASK_STATE_CANCELED");
        var terminalReplay = response(post("rpc-4b", "SendMessage", sendParams("message-1", "请分析客户风险。", skillId), "1.0", true));
        assertThat(terminalReplay.path("result").path("task").path("id").asText()).isEqualTo(firstId.toString());
        assertThat(terminalReplay.path("result").path("task").path("status").path("state").asText())
                .isEqualTo("TASK_STATE_CANCELED");

        var second = response(post("rpc-5", "SendMessage", sendParams("message-2", "为另一个客户分析风险。", skillId), "1.0", true));
        var secondId = UUID.fromString(second.path("result").path("task").path("id").asText());
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(secondId);
        // 合成 RunOutcome 只验证 Task 完成传播，不声称执行了模型或客户业务操作。
        tasks.complete(work, TaskRunner.RunOutcome.success("{\"riskLevel\":\"LOW\"}", false, null, null));
        var completed = response(post("rpc-6", "GetTask", Map.of("id", secondId.toString()), "1.0", true))
                .path("result");
        assertThat(completed.path("status").path("state").asText()).isEqualTo("TASK_STATE_COMPLETED");
        var restTask = get("/api/v1/workspaces/" + WORKSPACE + "/tasks/" + secondId, true);
        assertThat(restTask.statusCode()).isEqualTo(200);
        assertThat(json.readTree(restTask.body()).path("result")).isEqualTo(
                completed.path("artifacts").get(0).path("parts").get(0).path("data"));

        var third = response(post("rpc-7", "SendMessage", sendParams("message-3", "待取消任务。", skillId), "1.0", true));
        var thirdId = UUID.fromString(third.path("result").path("task").path("id").asText());
        var cancelled = response(post("rpc-8", "CancelTask", Map.of("id", thirdId.toString()), "1.0", true));
        assertThat(cancelled.path("result").path("status").path("state").asText()).isEqualTo("TASK_STATE_CANCELED");
        var repeatedCancel = response(post("rpc-9", "CancelTask", Map.of("id", thirdId.toString()), "1.0", true));
        assertThat(repeatedCancel.path("result").path("id").asText()).isEqualTo(thirdId.toString());
        assertThat(taskCount()).isEqualTo(3);

        var pageOne = response(post("rpc-11", "ListTasks", Map.of("pageSize", 2), "1.0", true)).path("result");
        assertThat(pageOne.path("tasks")).hasSize(2);
        assertThat(pageOne.path("totalSize").asLong()).isEqualTo(3);
        assertThat(pageOne.path("nextPageToken").asText()).isNotEmpty();
        assertThat(pageOne.path("tasks").get(0).has("artifacts")).isFalse();
        var pageTwo = response(post("rpc-12", "ListTasks", Map.of("pageSize", 2,
                "pageToken", pageOne.path("nextPageToken").asText()), "1.0", true)).path("result");
        assertThat(pageTwo.path("tasks")).hasSize(1);
        assertThat(pageTwo.path("nextPageToken").asText()).isEmpty();
        var listed = new java.util.HashSet<String>();
        pageOne.path("tasks").forEach(task -> listed.add(task.path("id").asText()));
        pageTwo.path("tasks").forEach(task -> listed.add(task.path("id").asText()));
        assertThat(listed).containsExactlyInAnyOrder(firstId.toString(), secondId.toString(), thirdId.toString());

        var contextList = response(post("rpc-13", "ListTasks", Map.of("contextId", secondId.toString(),
                "includeArtifacts", true), "1.0", true)).path("result");
        assertThat(contextList.path("tasks")).hasSize(1);
        assertThat(contextList.path("tasks").get(0).path("artifacts")).hasSize(1);
        var completedList = response(post("rpc-13a", "ListTasks", Map.of("status", "TASK_STATE_COMPLETED",
                "statusTimestampAfter", "2020-01-01T00:00:00Z"), "1.0", true)).path("result");
        assertThat(completedList.path("totalSize").asLong()).isEqualTo(1);
        assertThat(completedList.path("tasks").get(0).path("id").asText()).isEqualTo(secondId.toString());
        var hiddenTask = response(post("rpc-14", "GetTask", Map.of("id", secondId.toString()), "1.0", true,
                OTHER_WORKSPACE));
        assertThat(hiddenTask.path("error").path("code").asInt()).isEqualTo(-32001);
        assertThat(hiddenTask.path("error").toString()).doesNotContain(secondId.toString());
        var otherWorkspaceList = response(post("rpc-15", "ListTasks", Map.of(), "1.0", true, OTHER_WORKSPACE));
        assertThat(otherWorkspaceList.path("result").toString()).doesNotContain(firstId.toString(), secondId.toString(), thirdId.toString());

        // 同一 Agent 的两个不同委托上下文不得通过 A2A 列表互读对方 Task。
        var owner = identities.resolveToken("alice").orElseThrow();
        var firstDelegation = identities.createDelegation(new CreateDelegationCommand(owner, WORKSPACE,
                Ids.AGENT_RISK, Set.of("task:create", "task:read", "crm:customer:read"), Set.of("customer-001"),
                Instant.now().plusSeconds(300)));
        var secondDelegation = identities.createDelegation(new CreateDelegationCommand(owner, WORKSPACE,
                Ids.AGENT_RISK, Set.of("task:create", "task:read"), Set.of(), Instant.now().plusSeconds(300)));
        var firstDelegate = identities.resolveDelegatedToken("risk-agent", firstDelegation.id(),
                IdentityService.REST_AUDIENCE).orElseThrow();
        var secondDelegate = identities.resolveDelegatedToken("risk-agent", secondDelegation.id(),
                IdentityService.REST_AUDIENCE).orElseThrow();
        var firstDelegatedTask = delegatedTask(firstDelegate, "a2a-delegation-one");
        var secondDelegatedTask = delegatedTask(secondDelegate, "a2a-delegation-two");
        var delegatedList = response(postDelegated("rpc-16", "ListTasks", Map.of(), firstDelegation.id()));
        assertThat(delegatedList.path("result").path("totalSize").asLong()).isEqualTo(1);
        assertThat(delegatedList.path("result").path("tasks").get(0).path("id").asText())
                .isEqualTo(firstDelegatedTask.toString());
        assertThat(delegatedList.toString()).doesNotContain(secondDelegatedTask.toString());
    }

    private Map<String, Object> sendParams(String messageId, String text, String skillId) {
        return new HashMap<>(Map.of("message", new HashMap<>(Map.of("messageId", messageId,
                "role", "ROLE_USER", "parts", List.of(Map.of("text", text)))),
                "metadata", Map.of("skillId", skillId)));
    }

    private HttpResponse<String> post(String id, String method, Object params, String version, boolean authenticated)
            throws Exception {
        return post(id, method, params, version, authenticated, WORKSPACE);
    }

    private HttpResponse<String> post(String id, String method, Object params, String version, boolean authenticated,
                                      UUID workspaceId) throws Exception {
        var requestBody = json.writeValueAsString(Map.of("jsonrpc", "2.0", "id", id,
                "method", method, "params", params));
        var builder = HttpRequest.newBuilder(URI.create(baseUrl() + "/a2a/" + workspaceId))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody));
        if (version != null) builder.header("A2A-Version", version);
        if (authenticated) builder.header("Authorization", "Bearer " + TOKEN);
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path, boolean authenticated) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(baseUrl() + path)).GET();
        if (authenticated) builder.header("Authorization", "Bearer " + TOKEN);
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postDelegated(String id, String method, Object params, UUID delegationId)
            throws Exception {
        var requestBody = json.writeValueAsString(Map.of("jsonrpc", "2.0", "id", id,
                "method", method, "params", params));
        var request = HttpRequest.newBuilder(URI.create(baseUrl() + "/a2a/" + WORKSPACE))
                .header("Content-Type", "application/json")
                .header("A2A-Version", "1.0")
                .header("Authorization", "Bearer risk-agent")
                .header("X-EAF-Delegation", delegationId.toString())
                .POST(HttpRequest.BodyPublishers.ofString(requestBody)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private UUID delegatedTask(ActorContext actor, String key) {
        return tasks.create(new CreateTaskCommand(actor, WORKSPACE, Ids.AGENT_RISK, "2.0.0",
                "Analyze customer risk", null, null, key, UUID.randomUUID().toString(), "USER")).id();
    }

    private JsonNode response(HttpResponse<String> response) throws Exception {
        assertThat(response.statusCode()).isEqualTo(200);
        return json.readTree(response.body());
    }

    private int taskCount() {
        return jdbc.queryForObject("select count(*) from task.task where actor_id = ? and workspace_id = ?",
                Integer.class, ALICE.actorId(), WORKSPACE);
    }

    private String baseUrl() { return "http://127.0.0.1:" + port; }
}
