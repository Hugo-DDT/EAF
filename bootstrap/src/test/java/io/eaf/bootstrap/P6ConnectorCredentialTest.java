package io.eaf.bootstrap;

import com.sun.net.httpserver.HttpServer;
import io.eaf.connector.api.ConnectorService;
import io.eaf.shared.EafException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
@SpringBootTest(properties = "eaf.task.dispatcher-enabled=false")
//  验证 Connector 范围与当前版本化 Credential 同时约束出站请求。
class P6ConnectorCredentialTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String TEST_TOKEN = "p6-synthetic-connector-token";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final AtomicInteger CRM_CALLS = new AtomicInteger();
    private static final AtomicInteger AUTHENTICATED_CALLS = new AtomicInteger();
    private static final AtomicInteger REDIRECT_TARGET_CALLS = new AtomicInteger();

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
    static HttpServer crm;
    static HttpServer redirectTarget;

    @BeforeAll
    static void startTestPeers() throws Exception {
        redirectTarget = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        redirectTarget.createContext("/capture", exchange -> {
            REDIRECT_TARGET_CALLS.incrementAndGet();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        redirectTarget.start();

        crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/customers/customer-001", exchange -> {
            if (!authorized(exchange)) { exchange.sendResponseHeaders(401, -1); exchange.close(); return; }
            CRM_CALLS.incrementAndGet();
            AUTHENTICATED_CALLS.incrementAndGet();
            var body = "{\"customerId\":\"customer-001\",\"renewalStatus\":\"ACTIVE\",\"lastContactDate\":\"2026-09-20\",\"complaintSummary\":\"none\",\"sourceId\":\"crm-test-001\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.createContext("/customers/customer-redirect", exchange -> {
            CRM_CALLS.incrementAndGet();
            exchange.getResponseHeaders().add("Location", "http://127.0.0.1:" + redirectTarget.getAddress().getPort() + "/capture");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        crm.createContext("/customers/customer-500", exchange -> {
            CRM_CALLS.incrementAndGet();
            var body = ("upstream error " + TEST_TOKEN).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.start();
    }

    @AfterAll
    static void stopTestPeers() {
        if (crm != null) crm.stop(0);
        if (redirectTarget != null) redirectTarget.stop(0);
    }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.credentials.test-crm.token", () -> TEST_TOKEN);
        registry.add("eaf.credentials.a2a-peer-review.token", () -> "p6-synthetic-a2a-token");
    }

    @Autowired ConnectorService connectors;
    @Autowired DataSource dataSource;

    @Test
    void resolvesOnlyBoundCredentialsAndRejectsUnsafeConnectorStates() {
        var jdbc = new JdbcTemplate(dataSource);
        pointCrm(jdbc);
        var result = connectors.readCustomer(TENANT, WORKSPACE, "customer-001", Instant.now().plusSeconds(3));
        assertThat(result.customerId()).isEqualTo("customer-001");
        assertThat(AUTHENTICATED_CALLS).hasValue(1);

        var peer = connectors.requireActive(TENANT, WORKSPACE, "A2A_REVIEW_PEER");
        assertThat(peer.credentialRef()).isEqualTo("a2a-peer-review");
        assertThat(peer.audience()).isEqualTo("eaf:a2a:peer");
        //  固定为同一远端 Task 增加取消用途，不允许借用通用 Connector 权限。
        assertThat(peer.allowedUses()).containsExactlyInAnyOrder("a2a.send", "a2a.get", "a2a.cancel");
        assertThat(peer.permissions()).containsExactly("agent:risk-review");
        assertThat(jdbc.queryForObject("select count(*) from connector.instance where credential_ref = ? or base_url like ?",
                Integer.class, TEST_TOKEN, "%" + TEST_TOKEN + "%")).isZero();

        jdbc.update("update connector.instance set credential_ref = 'missing-secret' where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);
        assertRejected("CREDENTIAL_UNAVAILABLE", "customer-001");
        jdbc.update("update connector.instance set credential_ref = null where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);
        assertRejected("CREDENTIAL_UNAVAILABLE", "customer-001");
        jdbc.update("update connector.instance set credential_ref = 'test-crm' where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);

        jdbc.update("update connector.instance set audience = 'eaf:wrong-audience' where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);
        assertRejected("CREDENTIAL_SCOPE_DENIED", "customer-001");
        jdbc.update("update connector.instance set audience = 'eaf:test-crm', allowed_uses = array['followup.create'] where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);
        assertRejected("CREDENTIAL_SCOPE_DENIED", "customer-001");
        assertThat(CRM_CALLS).hasValue(1);
        jdbc.update("update connector.instance set allowed_uses = array['customer.read', 'followup.create', 'followup.verify'] where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);

        var callsBeforeDisable = CRM_CALLS.get();
        jdbc.update("update connector.instance set status = 'DISABLED' where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);
        assertRejected("CONNECTOR_UNAVAILABLE", "customer-001");
        assertThat(CRM_CALLS).hasValue(callsBeforeDisable);
        jdbc.update("update connector.instance set status = 'ACTIVE' where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://localhost:" + crm.getAddress().getPort(), TENANT, WORKSPACE);
        assertRejected("CONNECTOR_UNAVAILABLE", "customer-001");
        jdbc.update("update connector.instance set base_url = 'http://example.com:80' where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);
        assertRejected("CONNECTOR_UNAVAILABLE", "customer-001");
        jdbc.update("update connector.instance set base_url = 'file:///etc/passwd' where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'", TENANT, WORKSPACE);
        assertRejected("CONNECTOR_UNAVAILABLE", "customer-001");
        assertThat(CRM_CALLS).hasValue(callsBeforeDisable);
        pointCrm(jdbc);
        assertRejected("INVALID_TOOL_RESULT", "customer-redirect");
        assertThat(REDIRECT_TARGET_CALLS).hasValue(0);
        assertThat(CRM_CALLS).hasValue(callsBeforeDisable + 1);

        var failure = org.assertj.core.api.Assertions.catchThrowable(() ->
                connectors.readCustomer(TENANT, WORKSPACE, "customer-500", Instant.now().plusSeconds(3)));
        assertThat(failure).isInstanceOf(EafException.class);
        assertThat(((EafException) failure).code()).isEqualTo("CRM_UPSTREAM_FAILURE");
        assertThat(failure.getMessage()).doesNotContain(TEST_TOKEN);
        assertThat(CRM_CALLS.get()).isGreaterThan(callsBeforeDisable);
    }

    private void assertRejected(String code, String customerId) {
        assertThatThrownBy(() -> connectors.readCustomer(TENANT, WORKSPACE, customerId, Instant.now().plusSeconds(3)))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo(code));
    }

    private static boolean authorized(com.sun.net.httpserver.HttpExchange exchange) {
        return ("Bearer " + TEST_TOKEN).equals(exchange.getRequestHeaders().getFirst("Authorization"));
    }

    private void pointCrm(JdbcTemplate jdbc) {
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://127.0.0.1:" + crm.getAddress().getPort(), TENANT, WORKSPACE);
    }
}
