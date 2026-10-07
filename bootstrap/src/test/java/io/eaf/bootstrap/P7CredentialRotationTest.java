package io.eaf.bootstrap;

import com.sun.net.httpserver.HttpServer;
import io.eaf.connector.api.ConnectorService;
import io.eaf.credential.api.CredentialAdministration;
import io.eaf.credential.api.CredentialRegistration;
import io.eaf.credential.api.CredentialRequest;
import io.eaf.credential.api.CredentialResolutionPort;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
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
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false", "eaf.knowledge.outbox-publisher-enabled=false",
        "eaf.memory.outbox-publisher-enabled=false"})
//  用持久凭据元数据和本地秘密适配验证轮换、撤销与出站前失败关闭。
class P7CredentialRotationTest {
    private static final String INITIAL_SECRET = "p7-synthetic-credential-old";
    private static final String ROTATED_SECRET = "p7-synthetic-credential-new";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final UUID ALICE = UUID.fromString("80000000-0000-4000-8000-000000000001");
    private static final UUID OTHER_TENANT = UUID.fromString("70000000-0000-4000-8000-000000000002");
    private static final CopyOnWriteArrayList<String> SEEN_AUTHORIZATION = new CopyOnWriteArrayList<>();
    private static final AtomicReference<String> SECURITY_MODE = new AtomicReference<>("disabled");
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
    private static HttpServer crm;

    @BeforeAll
    static void startLocalCrm() throws Exception {
        crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/customers/customer-001", exchange -> {
            SEEN_AUTHORIZATION.add(exchange.getRequestHeaders().getFirst("Authorization"));
            var body = "{\"customerId\":\"customer-001\",\"renewalStatus\":\"ACTIVE\",\"lastContactDate\":\"2026-09-20\",\"complaintSummary\":\"synthetic\",\"sourceId\":\"p7-credential-fixture\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.start();
    }

    @AfterAll
    static void stopLocalCrm() {
        if (crm != null) crm.stop(0);
    }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.credentials.test-crm.token", () -> INITIAL_SECRET);
        registry.add("eaf.credentials.test-crm.rotated.token", () -> ROTATED_SECRET);
        registry.add("eaf.security.mode", SECURITY_MODE::get);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired ConnectorService connectors;
    @Autowired CredentialResolutionPort credentials;
    @Autowired CredentialAdministration administration;

    @BeforeEach
    void resetCredentialFixture() {
        SEEN_AUTHORIZATION.clear();
        jdbc.update("update credential.binding set status = 'ACTIVE', current_version = 1, updated_at = now() "
                + "where id = 'a7000000-0000-4000-8000-000000000001'");
        jdbc.update("delete from credential.secret_version where binding_id = 'a7000000-0000-4000-8000-000000000001' and version > 1");
        jdbc.update("update connector.instance set status = 'ACTIVE', credential_ref = 'test-crm', audience = 'eaf:test-crm', "
                + "allowed_uses = array['customer.read','followup.create','followup.verify'], base_url = ? "
                + "where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://127.0.0.1:" + crm.getAddress().getPort(), TENANT, WORKSPACE);
        jdbc.update("update credential.secret_version set status = case when version = 1 then 'ACTIVE' else 'REVOKED' end, "
                + "secret_ref = case when version = 1 then 'env://eaf.credentials.test-crm.token' else 'env://eaf.credentials.test-crm.rotated.token' end, "
                + "valid_from = now(), expires_at = null where binding_id = 'a7000000-0000-4000-8000-000000000001'");
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, 'credential:manage', 'ACTIVE') on conflict (workspace_id, actor_id, action) "
                        + "do update set status = 'ACTIVE'", TENANT, WORKSPACE, ALICE);
    }

    @Test
    void resolvesNewVersionOnTheNextOutboundAttemptAndAuditsWithoutSecretValues() {
        var first = connectors.readCustomer(TENANT, WORKSPACE, "customer-001", Instant.now().plusSeconds(3));
        var actor = new ActorContext(ALICE, TENANT, ActorType.HUMAN, Set.of("credential:manage"));
        var nextVersion = administration.rotate(actor, WORKSPACE, "test-crm",
                "env://eaf.credentials.test-crm.rotated.token", 1, null);
        var retry = connectors.readCustomer(TENANT, WORKSPACE, "customer-001", Instant.now().plusSeconds(3));
        var resolved = credentials.resolve(new CredentialRequest(TENANT, WORKSPACE, "test-crm",
                "eaf:test-crm", "customer.read", "crm.customer.read"));

        assertThat(first.customerId()).isEqualTo("customer-001");
        assertThat(retry.customerId()).isEqualTo("customer-001");
        assertThat(nextVersion).isEqualTo(2);
        assertThat(SEEN_AUTHORIZATION).containsExactly("Bearer " + INITIAL_SECRET, "Bearer " + ROTATED_SECRET);
        assertThat(resolved.version()).isEqualTo(2);
        assertThat(resolved.toString()).doesNotContain(INITIAL_SECRET, ROTATED_SECRET);
        assertThat(jdbc.queryForList("select secret_ref from credential.secret_version where binding_id = ?", String.class,
                UUID.fromString("a7000000-0000-4000-8000-000000000001")))
                .contains("env://eaf.credentials.test-crm.token", "env://eaf.credentials.test-crm.rotated.token")
                .doesNotContain(INITIAL_SECRET, ROTATED_SECRET);
        assertThat(jdbc.queryForList("select payload_json::text from audit.audit_event where action = 'credential.rotate'", String.class))
                .anySatisfy(payload -> assertThat(payload).contains("test-crm", "2")
                        .doesNotContain(INITIAL_SECRET, ROTATED_SECRET));
    }

    @Test
    void wrongScopePurposeExpiredVersionAndUnavailableSecretFailBeforeCrmCall() {
        assertUnavailable(new CredentialRequest(OTHER_TENANT, WORKSPACE, "test-crm", "eaf:test-crm", "customer.read", "crm.customer.read"));
        assertThatThrownBy(() -> credentials.resolve(new CredentialRequest(TENANT, WORKSPACE, "test-crm",
                "eaf:test-crm", "admin.write", "crm.customer.read")))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("CREDENTIAL_SCOPE_DENIED"));

        jdbc.update("update credential.secret_version set secret_ref = 'env://eaf.credentials.test-crm.missing.token' "
                + "where binding_id = 'a7000000-0000-4000-8000-000000000001' and version = 1");
        assertUnavailable(new CredentialRequest(TENANT, WORKSPACE, "test-crm", "eaf:test-crm", "customer.read", "crm.customer.read"));
        assertThat(SEEN_AUTHORIZATION).isEmpty();

        jdbc.update("update credential.secret_version set secret_ref = 'env://eaf.credentials.test-crm.token', "
                + "valid_from = now() - interval '2 seconds', expires_at = now() - interval '1 second' "
                + "where binding_id = 'a7000000-0000-4000-8000-000000000001' and version = 1");
        assertThatThrownBy(() -> connectors.readCustomer(TENANT, WORKSPACE, "customer-001", Instant.now().plusSeconds(3)))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("CREDENTIAL_UNAVAILABLE"));
        assertThat(SEEN_AUTHORIZATION).isEmpty();

        SECURITY_MODE.set("enterprise");
        try {
            jdbc.update("update credential.secret_version set expires_at = null where binding_id = ? and version = 1",
                    UUID.fromString("a7000000-0000-4000-8000-000000000001"));
            assertUnavailable(new CredentialRequest(TENANT, WORKSPACE, "test-crm", "eaf:test-crm", "customer.read", "crm.customer.read"));
        } finally {
            SECURITY_MODE.set("disabled");
        }
        assertThat(SEEN_AUTHORIZATION).isEmpty();
    }

    @Test
    void revokedCredentialStopsLaterCallsAndScopeCannotBeChangedByRotation() {
        var actor = new ActorContext(ALICE, TENANT, ActorType.HUMAN, Set.of("credential:manage"));
        administration.rotate(actor, WORKSPACE, "test-crm", "env://eaf.credentials.test-crm.rotated.token", 1, null);
        assertThatThrownBy(() -> administration.rotate(actor, WORKSPACE, "test-crm",
                "env://eaf.credentials.test-crm.token", 1, null))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("CREDENTIAL_VERSION_CONFLICT"));
        administration.disable(actor, WORKSPACE, "test-crm", 2);

        assertThatThrownBy(() -> connectors.readCustomer(TENANT, WORKSPACE, "customer-001", Instant.now().plusSeconds(3)))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("CREDENTIAL_UNAVAILABLE"));
        assertThat(SEEN_AUTHORIZATION).isEmpty();
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where action = 'credential.disable'", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void registrationStoresOnlySecretReferencesAndRequiresCurrentWorkspaceManagement() {
        var actor = new ActorContext(ALICE, TENANT, ActorType.HUMAN, Set.of("credential:manage"));
        //  迁移已固定登记 p7-crm-read；本测试使用独立引用验证登记与撤权。
        var version = administration.register(actor, new CredentialRegistration(WORKSPACE, "p7-crm-registration-test",
                "env://eaf.credentials.test-crm.rotated.token", "eaf:test-crm", Set.of("customer.read"),
                Set.of("crm.customer.read"), null));
        var resolved = credentials.resolve(new CredentialRequest(TENANT, WORKSPACE, "p7-crm-registration-test",
                "eaf:test-crm", "customer.read", "crm.customer.read"));

        assertThat(version).isEqualTo(1);
        assertThat(resolved.valueForOutboundRequest()).isEqualTo(ROTATED_SECRET);
        assertThat(jdbc.queryForObject("select secret_ref from credential.secret_version v join credential.binding b on b.id = v.binding_id "
                + "where b.tenant_id = ? and b.workspace_id = ? and b.credential_ref = ?", String.class,
                TENANT, WORKSPACE, "p7-crm-registration-test")).isEqualTo("env://eaf.credentials.test-crm.rotated.token");
        // 旧 ActorContext 即使仍带管理动作快照，实时撤销 Workspace 授权后也必须拒绝登记。
        jdbc.update("delete from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'credential:manage'",
                TENANT, WORKSPACE, ALICE);
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'workspace:read', 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE', tenant_id = excluded.tenant_id",
                TENANT, WORKSPACE, ALICE);
        assertThatThrownBy(() -> administration.register(actor,
                new CredentialRegistration(WORKSPACE, "p7-crm-denied", "env://eaf.credentials.test-crm.rotated.token",
                        "eaf:test-crm", Set.of("customer.read"), Set.of("crm.customer.read"), null)))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("POLICY_DENIED"));
    }

    private void assertUnavailable(CredentialRequest request) {
        assertThatThrownBy(() -> credentials.resolve(request))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("CREDENTIAL_UNAVAILABLE"));
    }
}
