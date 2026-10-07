package io.eaf.bootstrap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.Ids;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.audit.api.AuditPort;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.api.CreateKnowledgeDocumentCommand;
import io.eaf.knowledge.infrastructure.KnowledgeOutboxPublisher;
import io.eaf.task.api.TaskExecutionService;
import org.springframework.transaction.PlatformTransactionManager;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.modelcontextprotocol.spec.ProtocolVersions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
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
        properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
                "eaf.model.mode=deterministic"})
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class P25SkillPackageTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID WORKSPACE = Ids.WORKSPACE_A;
    private static final UUID CAPABILITY = UUID.fromString("54000000-0000-4000-8000-000000000012");
    private static final UUID SKILL = UUID.fromString("53000000-0000-4000-8000-00000000000f");
    private static final String ALICE = "eaf-local-alice";
    private static final String BOB = "eaf-local-bob";

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
    @Autowired KnowledgeService knowledge;
    @Autowired AuditPort audit;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired TaskExecutionService execution;

    @Test
    void exportsValidatesStoresPrivatelyAndUsesExactVersionThroughMcpHttp() throws Exception {
        var exported = rest("GET", exportPath("1.0.0"), null, null, null, ALICE);
        assertThat(exported.statusCode()).isEqualTo(200);
        assertThat(exported.headers().firstValue("Content-Type").orElse("")).startsWith("application/zip");
        var files = files(exported.body());
        assertThat(files.keySet()).containsExactlyInAnyOrder("SKILL.md", "eaf-package.json", "references/capability.json",
                "references/skill.json", "references/usage.md");
        assertThat(files.get("SKILL.md")).contains("name: eaf-54000000000040008000000000000012")
                .doesNotContain("promptId", "apiKey", "Authorization:");
        var manifest = json.readTree(files.get("eaf-package.json"));
        assertThat(manifest.path("profile").asText()).isEqualTo("EAF_DECLARATIVE_CAPABILITY_V1");
        assertThat(manifest.path("capability").path("version").asText()).isEqualTo("1.0.0");
        assertThat(manifest.path("skill").path("id").asText()).isEqualTo(SKILL.toString());
        var repeatedExport = rest("GET", exportPath("1.0.0"), null, null, null, ALICE);
        assertThat(json.readTree(files(repeatedExport.body()).get("eaf-package.json")).path("packageHash"))
                .isEqualTo(manifest.path("packageHash"));

        var preflight = rest("POST", basePath() + "/skill-packages/validate", exported.body(), "application/zip", null, ALICE);
        assertThat(preflight.statusCode()).isEqualTo(200);
        var preflightResult = json.readTree(preflight.body());
        assertThat(preflightResult.path("status").asText()).isEqualTo("VALID");
        assertThat(preflightResult.path("executable").asBoolean()).isFalse();
        assertThat(preflightResult.path("trust").asText()).isEqualTo("UNVERIFIED");

        var altered = new java.util.HashMap<>(files);
        altered.put("references/usage.md", altered.get("references/usage.md") + "\nextra\n");
        var tampered = rest("POST", basePath() + "/skill-packages/validate", zip(files.get("eaf-package.json"), altered),
                "application/zip", null, ALICE);
        assertThat(tampered.statusCode()).isEqualTo(400);
        assertThat(json.readTree(tampered.body()).path("code").asText()).isIn("PACKAGE_CONTENT_MISMATCH", "PACKAGE_FORMAT_UNSUPPORTED");
        var traversal = rest("POST", basePath() + "/skill-packages/validate", zipEntry("../escape"), "application/zip", null, ALICE);
        assertThat(traversal.statusCode()).isEqualTo(400);
        assertThat(json.readTree(traversal.body()).path("code").asText()).isEqualTo("PACKAGE_PATH_INVALID");
        var oversized = rest("POST", basePath() + "/skill-packages/validate", new byte[1_048_577], "application/zip", null, ALICE);
        assertThat(oversized.statusCode()).isEqualTo(413);
        assertThat(json.readTree(oversized.body()).path("code").asText()).isEqualTo("PACKAGE_TOO_LARGE");

        var imported = rest("POST", basePath() + "/skill-packages", exported.body(), "application/zip", "p25-package-1", ALICE);
        assertThat(imported.statusCode()).isEqualTo(201);
        var stored = json.readTree(imported.body());
        var packageId = UUID.fromString(stored.path("id").asText());
        assertThat(stored.path("status").asText()).isEqualTo("STORED");
        var repeated = rest("POST", basePath() + "/skill-packages", exported.body(), "application/zip", "p25-package-1", ALICE);
        assertThat(repeated.statusCode()).isEqualTo(200);
        assertThat(json.readTree(repeated.body()).path("id").asText()).isEqualTo(packageId.toString());

        var newerVersion = rest("GET", exportPath("1.1.0"), null, null, null, ALICE);
        assertThat(newerVersion.statusCode()).isEqualTo(200);
        var keyConflict = rest("POST", basePath() + "/skill-packages", newerVersion.body(), "application/zip", "p25-package-1", ALICE);
        assertThat(keyConflict.statusCode()).isEqualTo(409);
        assertThat(json.readTree(keyConflict.body()).path("code").asText()).isEqualTo("IDEMPOTENCY_CONFLICT");

        // Bob has Workspace visibility but remains unable to read Alice's private package.
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'skill:read', 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, WORKSPACE, Ids.BOB);
        var hidden = rest("GET", basePath() + "/skill-packages/" + packageId, null, null, null, BOB);
        assertThat(hidden.statusCode()).isEqualTo(404);
        var bobList = rest("GET", basePath() + "/skill-packages", null, null, null, BOB);
        assertThat(bobList.statusCode()).isEqualTo(200);
        assertThat(json.readTree(bobList.body()).path("items").size()).isZero();

        var download = rest("GET", basePath() + "/skill-packages/" + packageId + "/download", null, null, null, ALICE);
        assertThat(download.statusCode()).isEqualTo(200);
        assertThat(files(download.body())).isEqualTo(files);
        assertThat(rest("POST", basePath() + "/skill-packages/" + packageId + "/archive",
                "{\"expectedVersion\":1.5}".getBytes(StandardCharsets.UTF_8), "application/json", null, ALICE).statusCode())
                .isEqualTo(400);
        var archived = rest("POST", basePath() + "/skill-packages/" + packageId + "/archive",
                "{\"expectedVersion\":1}".getBytes(StandardCharsets.UTF_8), "application/json", null, ALICE);
        assertThat(archived.statusCode()).isEqualTo(200);
        assertThat(json.readTree(archived.body()).path("status").asText()).isEqualTo("ARCHIVED");
        assertThat(rest("GET", basePath() + "/skill-packages/" + packageId + "/download", null, null, null, ALICE).statusCode())
                .isEqualTo(404);
        var archivedReplay = rest("POST", basePath() + "/skill-packages", exported.body(), "application/zip", "p25-package-1", ALICE);
        assertThat(archivedReplay.statusCode()).isEqualTo(200);
        assertThat(json.readTree(archivedReplay.body()).path("status").asText()).isEqualTo("ARCHIVED");

        consumePackageThroughMcp(manifest);
    }

    private void consumePackageThroughMcp(JsonNode manifest) throws Exception {
        var owner = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
        var document = knowledge.create(new CreateKnowledgeDocumentCommand(owner, WORKSPACE,
                "合成设备知识", "manual://p25/device", "办公电脑无法启动时，先记录设备位置和故障现象，再按 IT 设备检修流程处理。",
                Map.of("synthetic", "true"), "p25-knowledge-create", "p25-knowledge"));
        knowledge.chunk(owner, WORKSPACE, document.id(), "p3-plain-1");
        var build = knowledge.buildIndex(owner, WORKSPACE, document.id(), "p3-plain-1", "p25-knowledge-index");
        assertThat(build.status()).isEqualTo("READY");
        knowledge.publish(owner, WORKSPACE, document.id(), document.rowVersion(), build.id(), "p25-knowledge-publish");
        new KnowledgeOutboxPublisher(jdbc, audit, transactionManager).publish();
        var transport = HttpClientStreamableHttpTransport.builder("http://127.0.0.1:" + port)
                .endpoint("/mcp").supportedProtocolVersions(List.of(ProtocolVersions.MCP_2025_11_25))
                .httpRequestCustomizer((builder, method, uri, sessionId, context) -> builder
                        .header("Authorization", "Bearer " + ALICE).header("Origin", "http://localhost:3000"))
                .build();
        McpSyncClient client = McpClient.sync(transport).build();
        try {
            assertThat(client.initialize().protocolVersion()).isEqualTo(ProtocolVersions.MCP_2025_11_25);
            assertThat(client.listTools().tools()).extracting(Tool::name).containsExactlyInAnyOrder(
                    "eaf.capabilities.list", "eaf.tasks.create", "eaf.tasks.get", "eaf.tasks.cancel",
                    "eaf.catalog.search", "eaf.skills.get", "eaf.capabilities.get", "eaf.context.query",
                    "eaf.context.sources.list", "eaf.context.scoped-query");
            var capabilityId = manifest.path("capability").path("id").asText();
            var capabilityVersion = manifest.path("capability").path("version").asText();
            var skillId = manifest.path("skill").path("id").asText();
            var skillVersion = manifest.path("skill").path("version").asText();
            var workspace = manifest.path("sourceWorkspaceId").asText();
            var capability = structured(client.callTool(new CallToolRequest("eaf.capabilities.get", Map.of(
                    "workspaceId", workspace, "capabilityId", capabilityId, "version", capabilityVersion))));
            var skill = structured(client.callTool(new CallToolRequest("eaf.skills.get", Map.of(
                    "workspaceId", workspace, "skillId", skillId, "version", skillVersion))));
            assertThat(capability.get("contentHash")).isEqualTo(manifest.path("capability").path("contentHash").asText());
            assertThat(skill.get("contentHash")).isEqualTo(manifest.path("skill").path("contentHash").asText());
            var binding = (Map<String, Object>) capability.get("skill");
            // 现有 MCP Capability 投影只返回 Skill ID/version；精确 Skill.get 提供单独的当前 contentHash。
            assertThat(binding).containsEntry("id", skillId).containsEntry("version", skillVersion);
            var task = structured(client.callTool(new CallToolRequest("eaf.tasks.create", Map.of(
                    "workspaceId", workspace, "capabilityId", capabilityId, "capabilityVersion", capabilityVersion,
                    "input", "办公电脑无法启动，请基于当前正式知识给出有出处的下一步建议。",
                    "idempotencyKey", "p25-consumer-task-1"))));
            var read = structured(client.callTool(new CallToolRequest("eaf.tasks.get", Map.of(
                    "workspaceId", workspace, "taskId", task.get("id")))));
            assertThat(read).containsEntry("id", task.get("id")).containsEntry("status", "QUEUED");
            var replay = structured(client.callTool(new CallToolRequest("eaf.tasks.create", Map.of(
                    "workspaceId", workspace, "capabilityId", capabilityId, "capabilityVersion", capabilityVersion,
                    "input", "办公电脑无法启动，请基于当前正式知识给出有出处的下一步建议。",
                    "idempotencyKey", "p25-consumer-task-1"))));
            assertThat(replay.get("id")).isEqualTo(task.get("id"));
            // 显式调度原 Task，经过既有执行槽、Runtime 和完成写回，再从 MCP 读取实际终态。
            assertThat(execution.dispatchNext()).isTrue();
            var deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
            do {
                read = structured(client.callTool(new CallToolRequest("eaf.tasks.get", Map.of(
                        "workspaceId", workspace, "taskId", task.get("id")))));
                if (!Set.of("QUEUED", "RUNNING").contains(read.get("status"))) break;
                Thread.sleep(50);
            } while (System.nanoTime() < deadline);
            assertThat(read).containsEntry("id", task.get("id")).containsEntry("status", "SUCCEEDED");
            var result = json.valueToTree(read.get("result"));
            assertThat(result.path("outcome").asText()).isEqualTo("READY");
            assertThat(result.path("readyToSubmit").asBoolean()).isTrue();
            assertThat(result.path("citations").size()).isPositive();
            assertThat(result.path("contextRefs").size()).isPositive();
            // 取消另一个未运行 Task，保留原有按版本取消覆盖，成功任务不用于演示取消。
            var pending = structured(client.callTool(new CallToolRequest("eaf.tasks.create", Map.of(
                    "workspaceId", workspace, "capabilityId", capabilityId, "capabilityVersion", capabilityVersion,
                    "input", "合成取消检查", "idempotencyKey", "p25-consumer-cancel-1"))));
            var cancelled = structured(client.callTool(new CallToolRequest("eaf.tasks.cancel", Map.of(
                    "workspaceId", workspace, "taskId", pending.get("id"), "expectedVersion", pending.get("version")))));
            assertThat(cancelled).containsEntry("status", "CANCELLED");
        } finally {
            client.close();
        }
    }

    private HttpResponse<byte[]> rest(String method, String path, byte[] body, String contentType,
                                      String idempotencyKey, String token) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(baseUrl() + path)).header("Accept", "application/json");
        if (token != null) builder.header("Authorization", "Bearer " + token);
        if (idempotencyKey != null) builder.header("Idempotency-Key", idempotencyKey);
        if (contentType != null) builder.header("Content-Type", contentType);
        if ("GET".equals(method)) builder.GET();
        else builder.method(method, HttpRequest.BodyPublishers.ofByteArray(body == null ? new byte[0] : body));
        return HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private String basePath() { return "/api/v1/workspaces/" + WORKSPACE; }
    private String exportPath(String version) { return basePath() + "/capabilities/" + CAPABILITY + "/versions/" + version + "/package"; }
    private String baseUrl() { return "http://127.0.0.1:" + port; }

    private Map<String, String> files(byte[] zip) throws Exception {
        var result = new java.util.TreeMap<String, String>();
        try (var input = new ZipInputStream(new ByteArrayInputStream(zip), StandardCharsets.UTF_8)) {
            ZipEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                var path = entry.getName().substring(entry.getName().indexOf('/') + 1);
                result.put(path, new String(input.readAllBytes(), StandardCharsets.UTF_8));
            }
        }
        return result;
    }

    private byte[] zip(String manifest, Map<String, String> entries) throws Exception {
        var parsed = json.readTree(manifest);
        var capId = parsed.path("capability").path("id").asText().replace("-", "");
        var root = "eaf-" + capId;
        var output = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(output, StandardCharsets.UTF_8)) {
            for (var item : entries.entrySet()) {
                zip.putNextEntry(new ZipEntry(root + "/" + item.getKey()));
                zip.write(item.getValue().getBytes(StandardCharsets.UTF_8));
                zip.closeEntry();
            }
        }
        return output.toByteArray();
    }

    private byte[] zipEntry(String name) throws Exception {
        var output = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(output, StandardCharsets.UTF_8)) {
            zip.putNextEntry(new ZipEntry(name));
            zip.write("x".getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return output.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> structured(CallToolResult result) {
        assertThat(result.isError()).as("MCP 响应：%s", result.content()).isNotEqualTo(Boolean.TRUE);
        return (Map<String, Object>) result.structuredContent();
    }
}
